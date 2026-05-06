"""
End-to-end tests for the new post-call pipeline.

Replaces the old tests/test_post_call.py (which documented the broken
behaviour). These tests drive the actual pipeline composition that
ships in main:

    POST /…/end  (mocked at fixture level)
        ↓
    triage.classify       → lane / priority / disposition
        ↓
    job_store.create_pipeline + audit_event 'pipeline_created'
        ↓
    celery_tasks._run_stage_async (analysis stage)
        ├ short-transcript defensive skip (AC8)
        ├ rate-limit reservation (Phase 2)
        ├ mocked LLM call → token_usage ledger
        └ audit_event 'stage_completed' + dependents unblocked
        ↓
    signal_jobs / lead_stage stages reading the parent's result

Requires live Postgres + Redis. Each test truncates pipeline-state
tables and flushes Redis so runs are independent.

Acceptance-criteria mapping (one assertion per AC where this file is
the canonical surface):
  - AC1: provider 429 absorbed as RateLimitDeferred → defer_job
  - AC5: complete audit trail per interaction
  - AC8: short-transcript skip enforced at the worker
  - AC2/AC3/AC4/AC7 are exercised in their own phase-specific test
    files; cross-references live in tests/test_acceptance_criteria.py.
"""

from datetime import datetime
from typing import Any, Dict, List
from unittest.mock import patch
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text

from src.services import budget as budget_svc
from src.services import job_store, triage
from src.services.job_store import (
    STAGE_ANALYSIS,
    STAGE_LEAD_STAGE,
    STAGE_RECORDING,
    STAGE_SIGNAL_JOBS,
)
from src.services.post_call_processor import ProviderRateLimitedError
from src.tasks import celery_tasks
from src.utils import db as db_module
from src.utils.db import async_session_factory
from src.utils.redis_client import redis_client


# ── Fixtures ───────────────────────────────────────────────────────────────

@pytest_asyncio.fixture(autouse=True)
async def _reset_async_engine():
    """asyncpg pool is bound to whichever event loop the first test ran in;
    pytest-asyncio (mode=auto) gives each test its own loop, so we rebuild
    the pool between tests."""
    yield
    await db_module.engine.dispose()


@pytest_asyncio.fixture
async def live_redis():
    try:
        await redis_client.ping()
        await redis_client.flushdb()
    except Exception as exc:
        pytest.skip(
            f"Redis unavailable — start `docker-compose up -d`. "
            f"Underlying error: {exc}"
        )
    yield redis_client


@pytest_asyncio.fixture
async def live_db():
    async with async_session_factory() as session:
        try:
            await session.execute(
                text(
                    "TRUNCATE processing_jobs, audit_events, token_usage "
                    "RESTART IDENTITY"
                )
            )
            await session.commit()
            budget_svc.invalidate_spillover_cache()
        except Exception as exc:
            pytest.skip(
                "Postgres unavailable or schema not loaded — re-init with "
                "`docker-compose down -v && docker-compose up -d`. "
                f"Underlying error: {exc}"
            )
        yield session


@pytest_asyncio.fixture
async def stub_celery_dispatch():
    """Replace run_stage.apply_async + recording_poller dispatch with no-ops
    so finalize-completed doesn't try to talk to a live broker. We only
    drive the analysis stage directly in these tests."""
    captured: List[Dict[str, Any]] = []

    class _StubTask:
        def apply_async(self, **kwargs):
            captured.append(kwargs)

    real_run_stage = celery_tasks.run_stage
    celery_tasks.run_stage = _StubTask()
    try:
        yield captured
    finally:
        celery_tasks.run_stage = real_run_stage


def _transcript(turns: List[Dict[str, str]]) -> List[Dict[str, str]]:
    return turns


def _payload_for(transcript_turns: List[Dict[str, str]]) -> Dict[str, Any]:
    return {
        "interaction_id": "set-by-test",
        "session_id": str(uuid4()),
        "lead_id": str(uuid4()),
        "campaign_id": str(uuid4()),
        "customer_id": str(uuid4()),
        "agent_id": str(uuid4()),
        "call_sid": "test-sid-001",
        "transcript_text": "\n".join(
            f"{t.get('role')}: {t.get('content')}" for t in transcript_turns
        ),
        "conversation_data": {"transcript": transcript_turns},
        "additional_data": {},
        "ended_at": datetime.utcnow().isoformat(),
        "exotel_account_id": "test-account",
    }


_REBOOK_TRANSCRIPT = [
    {"role": "agent", "content": "Hello, am I speaking with Mr. Sharma?"},
    {"role": "customer", "content": "Haan ji"},
    {"role": "agent", "content": "Calling about your phone evaluation. Can we reschedule?"},
    {"role": "customer", "content": "Tomorrow 3:30 PM works"},
    {"role": "agent", "content": "Confirmed, our executive will visit tomorrow at 3:30 PM"},
    {"role": "customer", "content": "Okay, confirmed. Bye."},
]


_NOT_INTERESTED_TRANSCRIPT = [
    {"role": "agent", "content": "Hello, am I speaking with Ms. Gupta?"},
    {"role": "customer", "content": "Not interested, don't call again"},
    {"role": "agent", "content": "Sorry for the inconvenience. Have a good day."},
    {"role": "customer", "content": "Bye"},
]


_WRONG_NUMBER_TRANSCRIPT = [
    {"role": "agent", "content": "Hello—"},
    {"role": "customer", "content": "Wrong number"},
]


# ── Triage → pipeline composition ─────────────────────────────────────────

async def test_triage_classifies_rebook_as_hot_lane(live_db):
    """A confirmed rebook transcript drives the triage classifier into the
    hot lane with priority=1. This is the lane that drives end-to-end
    latency for time-sensitive bookings."""
    result = await triage.classify(_REBOOK_TRANSCRIPT)
    assert result.lane == triage.LANE_HOT
    assert result.priority == triage.PRIORITY_HOT
    assert result.is_skip is False


async def test_triage_classifies_not_interested_as_cold_lane(live_db):
    """A "not interested" transcript drives cold lane (priority 9). Cold-lane
    work is processed within 30 minutes; quota is preserved for hot work."""
    result = await triage.classify(_NOT_INTERESTED_TRANSCRIPT)
    assert result.lane == triage.LANE_COLD
    assert result.priority == triage.PRIORITY_COLD


async def test_triage_classifies_wrong_number_as_skip(live_db):
    """AC8 — "wrong number" transcripts never reach the LLM. Triage returns
    skip; pipeline-creation short-circuits the analysis stage."""
    result = await triage.classify(_WRONG_NUMBER_TRANSCRIPT)
    assert result.is_skip is True
    assert result.disposition in ("short_call", "wrong_number")


async def test_pipeline_creation_for_hot_lane_sets_lane_and_priority(live_db):
    """After triage, create_pipeline persists lane='hot' / priority=1 on
    every stage row so the worker can route to the correct Celery queue."""
    interaction_id = uuid4()
    result = await triage.classify(_REBOOK_TRANSCRIPT)
    job_ids = await job_store.create_pipeline(
        live_db,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload_for(_REBOOK_TRANSCRIPT),
        has_long_transcript=True,
        lane=result.lane,
        priority=result.priority,
    )
    await live_db.commit()

    rows = await live_db.execute(
        text(
            "SELECT stage, lane, priority FROM processing_jobs "
            "WHERE interaction_id = :i"
        ),
        {"i": str(interaction_id)},
    )
    by_stage = {r.stage: r for r in rows.all()}
    for stage in (STAGE_RECORDING, STAGE_ANALYSIS, STAGE_SIGNAL_JOBS, STAGE_LEAD_STAGE):
        assert by_stage[stage].lane == "hot"
        assert by_stage[stage].priority == 1


async def test_short_transcript_pipeline_skips_analysis_pre_populates_result(live_db):
    """AC8 — pipeline creation for a short transcript marks analysis as
    `skipped` with a pre-populated result so signal_jobs / lead_stage
    dependents read call_stage = 'short_call' without an LLM call."""
    interaction_id = uuid4()
    job_ids = await job_store.create_pipeline(
        live_db,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload_for(_WRONG_NUMBER_TRANSCRIPT),
        has_long_transcript=False,
    )
    await live_db.commit()

    row = await live_db.execute(
        text(
            "SELECT state, result FROM processing_jobs "
            "WHERE id = :id"
        ),
        {"id": str(job_ids[STAGE_ANALYSIS])},
    )
    record = row.first()
    assert record.state == "skipped"
    assert record.result["call_stage"] == "short_call"
    assert record.result.get("skipped") is True


# ── Analysis worker integration ────────────────────────────────────────────

async def test_analysis_stage_reserves_records_completes(
    live_db, live_redis, stub_celery_dispatch,
):
    """End-to-end: a long transcript runs the analysis stage through
    _run_stage_async. The worker reserves quota, calls the (mocked) LLM,
    writes a token_usage ledger row, completes the job, unblocks
    dependents, and emits the audit_events trail."""
    interaction_id = uuid4()
    customer_id = uuid4()
    campaign_id = uuid4()
    job_ids = await job_store.create_pipeline(
        live_db,
        interaction_id=interaction_id,
        customer_id=customer_id,
        campaign_id=campaign_id,
        payload=_payload_for(_REBOOK_TRANSCRIPT),
        has_long_transcript=True,
        lane="hot",
        priority=1,
    )
    await live_db.commit()

    # Mock the LLM. The default _call_llm mock already returns a fixed
    # response; here we make the call_stage match the rebook outcome so
    # dependents can read it via depends_on_job_id.
    fake_response = {
        "call_stage": "rebook_confirmed",
        "entities": {"date": "tomorrow", "time": "3:30 PM"},
        "summary": "Confirmed rebook for tomorrow 3:30 PM",
        "usage": {"total_tokens": 1450},
    }

    with patch(
        "src.services.post_call_processor.PostCallProcessor._call_llm",
        return_value=fake_response,
    ):
        await celery_tasks._run_stage_async(
            str(job_ids[STAGE_ANALYSIS]), STAGE_ANALYSIS,
        )

    # Job state: completed.
    row = await live_db.execute(
        text(
            "SELECT state, result FROM processing_jobs WHERE id = :id"
        ),
        {"id": str(job_ids[STAGE_ANALYSIS])},
    )
    record = row.first()
    assert record.state == "completed"
    assert record.result["call_stage"] == "rebook_confirmed"
    assert record.result["tokens_used"] == 1450

    # Token usage ledger: one row for this interaction.
    ledger = await live_db.execute(
        text(
            "SELECT tokens_actual, source FROM token_usage "
            "WHERE interaction_id = :i"
        ),
        {"i": str(interaction_id)},
    )
    ledger_row = ledger.first()
    assert ledger_row is not None
    assert ledger_row.tokens_actual == 1450
    assert ledger_row.source in ("reserved", "spillover", "mixed")

    # Dependents (signal_jobs, lead_stage) flipped from blocked to pending
    # and were dispatched via the stubbed run_stage.apply_async.
    dep_rows = await live_db.execute(
        text(
            "SELECT stage, state FROM processing_jobs "
            "WHERE interaction_id = :i AND stage IN ('signal_jobs','lead_stage')"
        ),
        {"i": str(interaction_id)},
    )
    dep_states = {r.stage: r.state for r in dep_rows.all()}
    assert dep_states[STAGE_SIGNAL_JOBS] == "pending"
    assert dep_states[STAGE_LEAD_STAGE] == "pending"
    assert len(stub_celery_dispatch) == 2

    # Audit trail: stage_leased + stage_completed for analysis.
    audits = await live_db.execute(
        text(
            "SELECT event_type FROM audit_events "
            "WHERE interaction_id = :i ORDER BY id"
        ),
        {"i": str(interaction_id)},
    )
    types = [r.event_type for r in audits.all()]
    assert "stage_leased" in types
    assert "stage_completed" in types


async def test_provider_429_absorbed_as_defer_not_failure(
    live_db, live_redis, stub_celery_dispatch,
):
    """AC1 — when the provider returns 429 despite our reservation, the
    worker absorbs it: the reservation is refunded, the customer's
    effective TPM is multiplicatively decreased for 60s, and the job
    transitions to pending via defer_job (NOT failed). The 429 never
    surfaces to the API caller."""
    interaction_id = uuid4()
    customer_id = uuid4()
    job_ids = await job_store.create_pipeline(
        live_db,
        interaction_id=interaction_id,
        customer_id=customer_id,
        campaign_id=uuid4(),
        payload=_payload_for(_REBOOK_TRANSCRIPT),
        has_long_transcript=True,
        lane="hot",
        priority=1,
    )
    await live_db.commit()

    async def raise_429(self, prompt):
        raise ProviderRateLimitedError(retry_after_seconds=30)

    with patch(
        "src.services.post_call_processor.PostCallProcessor._call_llm",
        new=raise_429,
    ):
        await celery_tasks._run_stage_async(
            str(job_ids[STAGE_ANALYSIS]), STAGE_ANALYSIS,
        )

    row = await live_db.execute(
        text(
            "SELECT state, attempts, defer_count, last_error "
            "FROM processing_jobs WHERE id = :id"
        ),
        {"id": str(job_ids[STAGE_ANALYSIS])},
    )
    record = row.first()
    assert record.state == "pending"
    # defer_job decrements attempts to undo the lease's auto-increment.
    assert record.attempts == 0
    assert record.defer_count == 1
    assert "provider_429" in record.last_error

    # No token_usage row should have been written — the LLM call failed.
    ledger = await live_db.execute(
        text("SELECT COUNT(*) AS n FROM token_usage WHERE interaction_id = :i"),
        {"i": str(interaction_id)},
    )
    assert ledger.first().n == 0

    # An audit event of type stage_rate_limit_deferred MUST exist.
    audits = await live_db.execute(
        text(
            "SELECT event_type, severity FROM audit_events "
            "WHERE interaction_id = :i AND event_type = 'stage_rate_limit_deferred'"
        ),
        {"i": str(interaction_id)},
    )
    deferred_events = audits.all()
    assert len(deferred_events) == 1
    assert deferred_events[0].severity == "warning"


async def test_worker_skips_short_transcript_on_redelivery(
    live_db, live_redis, stub_celery_dispatch,
):
    """AC8 defensive — even if a long-transcript pipeline row is leased and
    the redelivered payload turns out to be short, the worker re-checks
    transcript length and marks the job skipped without consuming LLM
    quota. Dependents still unblock as if it succeeded."""
    interaction_id = uuid4()
    # Create as long-transcript so the row starts in `pending` analysis,
    # then mutate the payload to a short transcript to simulate the race.
    job_ids = await job_store.create_pipeline(
        live_db,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload_for(_REBOOK_TRANSCRIPT),
        has_long_transcript=True,
        lane="hot",
        priority=1,
    )
    await live_db.commit()
    await live_db.execute(
        text(
            "UPDATE processing_jobs SET payload = cast(:p as jsonb) "
            "WHERE id = :id"
        ),
        {
            "p": __import__("json").dumps(
                _payload_for(_WRONG_NUMBER_TRANSCRIPT), default=str,
            ),
            "id": str(job_ids[STAGE_ANALYSIS]),
        },
    )
    await live_db.commit()

    await celery_tasks._run_stage_async(
        str(job_ids[STAGE_ANALYSIS]), STAGE_ANALYSIS,
    )

    row = await live_db.execute(
        text(
            "SELECT state, result FROM processing_jobs WHERE id = :id"
        ),
        {"id": str(job_ids[STAGE_ANALYSIS])},
    )
    record = row.first()
    assert record.state == "skipped"
    assert record.result["call_stage"] == "short_call"

    # No ledger row — no LLM call was made.
    ledger = await live_db.execute(
        text("SELECT COUNT(*) AS n FROM token_usage WHERE interaction_id = :i"),
        {"i": str(interaction_id)},
    )
    assert ledger.first().n == 0


async def test_audit_trail_complete_for_one_interaction(
    live_db, live_redis, stub_celery_dispatch,
):
    """AC5 — every interaction has a complete audit trail. After running the
    analysis stage to completion, querying audit_events by interaction_id
    returns the expected event types in chronological order."""
    interaction_id = uuid4()
    customer_id = uuid4()
    campaign_id = uuid4()

    # Write the pipeline_created event the same way the API endpoint does.
    from src.utils.logging import write_audit_event
    job_ids = await job_store.create_pipeline(
        live_db,
        interaction_id=interaction_id,
        customer_id=customer_id,
        campaign_id=campaign_id,
        payload=_payload_for(_REBOOK_TRANSCRIPT),
        has_long_transcript=True,
        lane="hot",
        priority=1,
    )
    await write_audit_event(
        live_db,
        interaction_id=str(interaction_id),
        event_type="pipeline_created",
        customer_id=str(customer_id),
        campaign_id=str(campaign_id),
        stages=list(job_ids.keys()),
        lane="hot",
    )
    await live_db.commit()

    with patch(
        "src.services.post_call_processor.PostCallProcessor._call_llm",
        return_value={
            "call_stage": "rebook_confirmed",
            "entities": {},
            "summary": "ok",
            "usage": {"total_tokens": 800},
        },
    ):
        await celery_tasks._run_stage_async(
            str(job_ids[STAGE_ANALYSIS]), STAGE_ANALYSIS,
        )

    audits = await live_db.execute(
        text(
            "SELECT event_type, stage, severity FROM audit_events "
            "WHERE interaction_id = :i ORDER BY id"
        ),
        {"i": str(interaction_id)},
    )
    rows = audits.all()
    types = [r.event_type for r in rows]

    assert types[0] == "pipeline_created"
    assert "stage_leased" in types
    assert "stage_completed" in types
    # Severity is info for the happy path — no warnings/errors expected.
    assert all(r.severity == "info" for r in rows)
