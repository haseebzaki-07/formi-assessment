"""
Phase 1 tests — durable execution + recording fan-out.

Validates the acceptance criteria this phase commits to:
  - AC3: Worker kill mid-task does not lose the job; another worker resumes.
  - AC8: Short-transcript skip is enforced both at pipeline creation and
    again inside the worker (defensive).
  - Partial AC5: every state transition writes an audit_events row keyed by
    interaction_id. Full audit-trail coverage lands in Phase 6.

Requires a live Postgres reachable at DATABASE_URL (docker-compose up). The
fixture truncates processing_jobs and audit_events before each test.
"""

import asyncio
from datetime import datetime
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text

from src.services import job_store
from src.utils.db import async_session_factory


@pytest_asyncio.fixture
async def db_session():
    async with async_session_factory() as session:
        try:
            await session.execute(
                text("TRUNCATE processing_jobs, audit_events RESTART IDENTITY")
            )
            await session.commit()
        except Exception as exc:
            pytest.skip(
                "Postgres unavailable or schema not loaded — run "
                "`docker-compose up -d` and ensure data/schema.sql has been "
                f"applied (re-init with `docker-compose down -v` if stale). "
                f"Underlying error: {exc}"
            )
        yield session


def _payload(transcript_turns: int = 8) -> dict:
    transcript = [
        {"role": "agent" if i % 2 == 0 else "customer", "content": f"turn {i}"}
        for i in range(transcript_turns)
    ]
    return {
        "interaction_id": "set-by-test",
        "session_id": str(uuid4()),
        "lead_id": str(uuid4()),
        "campaign_id": str(uuid4()),
        "customer_id": str(uuid4()),
        "agent_id": str(uuid4()),
        "call_sid": "test-sid-001",
        "transcript_text": "agent: ...",
        "conversation_data": {"transcript": transcript},
        "additional_data": {},
        "ended_at": datetime.utcnow().isoformat(),
        "exotel_account_id": "test-account",
    }


async def test_pipeline_creates_four_stages_for_long_transcript(db_session):
    interaction_id = uuid4()
    job_ids = await job_store.create_pipeline(
        db_session,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload(transcript_turns=8),
        has_long_transcript=True,
    )
    await db_session.commit()

    assert set(job_ids.keys()) == {
        "recording", "analysis", "signal_jobs", "lead_stage",
    }

    rows = await db_session.execute(
        text(
            "SELECT stage, state, depends_on_job_id "
            "FROM processing_jobs WHERE interaction_id = :i"
        ),
        {"i": str(interaction_id)},
    )
    by_stage = {r.stage: r for r in rows.all()}

    assert by_stage["recording"].state == "pending"
    assert by_stage["recording"].depends_on_job_id is None
    assert by_stage["analysis"].state == "pending"
    assert by_stage["analysis"].depends_on_job_id is None
    assert by_stage["signal_jobs"].state == "blocked"
    assert by_stage["signal_jobs"].depends_on_job_id is not None
    assert by_stage["lead_stage"].state == "blocked"
    assert by_stage["lead_stage"].depends_on_job_id is not None


async def test_short_transcript_skips_analysis_at_creation(db_session):
    """AC8 — pipeline-creation enforcement.

    Analysis stage lands in `skipped` state with a pre-populated result so
    that signal_jobs / lead_stage dependents reading it see call_stage =
    'short_call'. Dependents are 'pending' (not 'blocked' on a skipped row).
    """
    interaction_id = uuid4()
    await job_store.create_pipeline(
        db_session,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload(transcript_turns=2),
        has_long_transcript=False,
    )
    await db_session.commit()

    rows = await db_session.execute(
        text(
            "SELECT stage, state, result "
            "FROM processing_jobs WHERE interaction_id = :i"
        ),
        {"i": str(interaction_id)},
    )
    by_stage = {r.stage: r for r in rows.all()}

    assert by_stage["analysis"].state == "skipped"
    assert by_stage["analysis"].result is not None
    assert by_stage["analysis"].result.get("call_stage") == "short_call"
    assert by_stage["signal_jobs"].state == "pending"
    assert by_stage["lead_stage"].state == "pending"


async def test_short_transcript_skip_enforced_in_worker():
    """AC8 — defensive worker enforcement.

    Even if a long-transcript pipeline row is redelivered to the worker, the
    worker re-checks transcript length and skips rather than burning LLM
    quota on a 2-turn transcript.
    """
    from src.tasks.celery_tasks import _do_analysis, _SkipStage

    short_payload = _payload(transcript_turns=2)
    with pytest.raises(_SkipStage) as exc:
        await _do_analysis(
            interaction_id=str(uuid4()),
            job_id=str(uuid4()),
            payload=short_payload,
        )
    assert exc.value.reason == "short_transcript"


async def test_lease_idempotent_under_redelivery(db_session):
    """A second lease attempt against an already-leased row whose lease has
    not expired returns None — duplicate Celery delivery is a no-op."""
    interaction_id = uuid4()
    job_ids = await job_store.create_pipeline(
        db_session,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload(),
        has_long_transcript=True,
    )
    await db_session.commit()

    first = await job_store.lease_job(
        db_session,
        job_id=job_ids["analysis"],
        worker_id="worker-A",
        lease_seconds=60,
    )
    await db_session.commit()
    assert first is not None

    second = await job_store.lease_job(
        db_session,
        job_id=job_ids["analysis"],
        worker_id="worker-B",
        lease_seconds=60,
    )
    await db_session.commit()
    assert second is None


async def test_worker_crash_resumes_via_lease_expiry(db_session):
    """AC3 — a leased job whose lease has expired can be re-leased by
    another worker. The original lease_token becomes invalid for completion,
    so a "late completion" from the crashed worker is a durability no-op.
    """
    interaction_id = uuid4()
    job_ids = await job_store.create_pipeline(
        db_session,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload(),
        has_long_transcript=True,
    )
    await db_session.commit()

    leased_a = await job_store.lease_job(
        db_session,
        job_id=job_ids["analysis"],
        worker_id="worker-A",
        lease_seconds=1,
    )
    await db_session.commit()
    assert leased_a is not None
    original_token = leased_a["lease_token"]

    await asyncio.sleep(1.5)

    leased_b = await job_store.lease_job(
        db_session,
        job_id=job_ids["analysis"],
        worker_id="worker-B",
        lease_seconds=60,
    )
    await db_session.commit()
    assert leased_b is not None
    assert str(leased_b["lease_token"]) != str(original_token)
    assert leased_b["attempts"] == 2

    a_late_complete = await job_store.complete_job(
        db_session,
        job_id=job_ids["analysis"],
        lease_token=original_token,
        result={"late": True},
    )
    await db_session.commit()
    assert a_late_complete is False

    b_complete = await job_store.complete_job(
        db_session,
        job_id=job_ids["analysis"],
        lease_token=leased_b["lease_token"],
        result={"call_stage": "rebook_confirmed"},
    )
    await db_session.commit()
    assert b_complete is True


async def test_dependents_unblock_on_completion(db_session):
    """signal_jobs and lead_stage transition from blocked → pending when the
    analysis stage they depend on completes."""
    interaction_id = uuid4()
    job_ids = await job_store.create_pipeline(
        db_session,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload(),
        has_long_transcript=True,
    )
    await db_session.commit()

    leased = await job_store.lease_job(
        db_session,
        job_id=job_ids["analysis"],
        worker_id="worker",
        lease_seconds=60,
    )
    await db_session.commit()

    ok = await job_store.complete_job(
        db_session,
        job_id=job_ids["analysis"],
        lease_token=leased["lease_token"],
        result={"call_stage": "rebook_confirmed", "tokens_used": 1500},
    )
    await db_session.commit()
    assert ok is True

    rows = await db_session.execute(
        text(
            "SELECT stage, state FROM processing_jobs "
            "WHERE interaction_id = :i AND stage IN ('signal_jobs','lead_stage')"
        ),
        {"i": str(interaction_id)},
    )
    states = {r.stage: r.state for r in rows.all()}
    assert states["signal_jobs"] == "pending"
    assert states["lead_stage"] == "pending"


async def test_failed_job_retries_until_dead(db_session):
    """Repeatedly leasing and failing a job exhausts max_attempts and
    transitions it to `dead` (terminal state, payload preserved)."""
    interaction_id = uuid4()
    job_ids = await job_store.create_pipeline(
        db_session,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload(),
        has_long_transcript=True,
    )
    await db_session.commit()

    final_state = None
    for attempt in range(4):
        leased = await job_store.lease_job(
            db_session,
            job_id=job_ids["analysis"],
            worker_id=f"w-{attempt}",
            lease_seconds=60,
        )
        if leased is None:
            break
        await db_session.commit()
        final_state = await job_store.fail_job(
            db_session,
            job_id=job_ids["analysis"],
            lease_token=leased["lease_token"],
            error="simulated failure",
            retry_in_seconds=0,
        )
        await db_session.commit()

    assert final_state == "dead"

    payload_row = await db_session.execute(
        text(
            "SELECT payload, last_error FROM processing_jobs WHERE id = :id"
        ),
        {"id": str(job_ids["analysis"])},
    )
    record = payload_row.first()
    assert record.payload is not None  # payload preserved for replay
    assert "simulated failure" in record.last_error


async def test_audit_event_round_trip(db_session):
    """Partial AC5 — write_audit_event persists a row queryable by
    interaction_id and event_type."""
    from src.utils.logging import write_audit_event

    interaction_id = uuid4()
    customer_id = uuid4()
    campaign_id = uuid4()

    await write_audit_event(
        db_session,
        interaction_id=interaction_id,
        event_type="pipeline_created",
        customer_id=customer_id,
        campaign_id=campaign_id,
        is_short_transcript=False,
        stages=["recording", "analysis", "signal_jobs", "lead_stage"],
    )
    await db_session.commit()

    row = await db_session.execute(
        text(
            "SELECT event_type, customer_id, details "
            "FROM audit_events WHERE interaction_id = :i"
        ),
        {"i": str(interaction_id)},
    )
    record = row.first()
    assert record is not None
    assert record.event_type == "pipeline_created"
    assert str(record.customer_id) == str(customer_id)
    assert record.details["is_short_transcript"] is False
    assert "analysis" in record.details["stages"]


async def test_reap_stale_leases_recovers_crashed_jobs(db_session):
    """The janitor query transitions expired-lease rows back to pending so
    poller-style consumers can re-lease them without a per-job redelivery."""
    interaction_id = uuid4()
    job_ids = await job_store.create_pipeline(
        db_session,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload(),
        has_long_transcript=True,
    )
    await db_session.commit()

    await job_store.lease_job(
        db_session,
        job_id=job_ids["analysis"],
        worker_id="crashed-worker",
        lease_seconds=1,
    )
    await db_session.commit()
    await asyncio.sleep(1.5)

    reaped = await job_store.reap_stale_leases(db_session)
    await db_session.commit()
    assert reaped >= 1

    row = await db_session.execute(
        text("SELECT state FROM processing_jobs WHERE id = :id"),
        {"id": str(job_ids["analysis"])},
    )
    assert row.first().state == "pending"
