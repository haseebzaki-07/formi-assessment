"""
Phase 4 tests — recording poller.

Validates:
  - Poll-with-backoff schedule: a 404, 404, 404, 200 sequence is observed
    and produces the expected audit_events (one per attempt).
  - Terminal failure: a recording that never appears within the budget
    produces a `recording_failed_terminal` event at severity=error.
  - Permanent failure: missing call_sid short-circuits to terminal failure
    without consuming the full poll budget.
  - Reconciliation: a stuck leased recording row is reset to pending and
    an audit event is written for it.

Tests drive _poll_recording_async (and the reconciliation async helper)
directly so we don't need a running Celery worker. Per-attempt sleeps are
replaced by a fake sleep that records the durations rather than actually
waiting, so the suite stays fast.

Requires a live Postgres reachable at DATABASE_URL. The db_session fixture
truncates processing_jobs and audit_events before each test.
"""

from datetime import datetime
from typing import List
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text

from src.services import job_store
from src.services.recording import RecordingPollResult, RecordingPollStatus
from src.tasks import recording_poller
from src.tasks.recording_poller import (
    _poll_recording_async,
    _reconcile_async,
)
from src.utils import db as db_module
from src.utils.db import async_session_factory


@pytest_asyncio.fixture(autouse=True)
async def _reset_async_engine():
    """pytest-asyncio (mode=auto) creates a fresh event loop per test, but
    the module-level AsyncEngine + asyncpg pool is bound to whichever loop
    the first test ran in. Reusing it from a new loop raises 'another
    operation in progress'. Dispose-and-rebuild between tests so each test
    runs against a fresh pool on its own loop.
    """
    yield
    await db_module.engine.dispose()


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
                f"applied. Underlying error: {exc}"
            )
        yield session


def _payload(call_sid: str = "exotel-call-001") -> dict:
    transcript = [
        {"role": "agent" if i % 2 == 0 else "customer", "content": f"turn {i}"}
        for i in range(8)
    ]
    return {
        "interaction_id": "set-by-test",
        "session_id": str(uuid4()),
        "lead_id": str(uuid4()),
        "campaign_id": str(uuid4()),
        "customer_id": str(uuid4()),
        "agent_id": str(uuid4()),
        "call_sid": call_sid,
        "transcript_text": "agent: ...",
        "conversation_data": {"transcript": transcript},
        "additional_data": {},
        "ended_at": datetime.utcnow().isoformat(),
        "exotel_account_id": "test-account",
    }


async def _create_recording_pipeline(db_session, **payload_overrides) -> dict:
    interaction_id = uuid4()
    payload = _payload(**payload_overrides)
    payload["interaction_id"] = str(interaction_id)
    job_ids = await job_store.create_pipeline(
        db_session,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=payload,
        has_long_transcript=True,
    )
    await db_session.commit()
    return {"interaction_id": interaction_id, "job_ids": job_ids}


class _ScriptedPoll:
    """Returns a sequence of RecordingPollResults, then raises if asked
    again — that surfaces an over-budget polling test bug."""

    def __init__(self, results: List[RecordingPollResult]):
        self._results = list(results)
        self.calls = 0

    async def __call__(self, *, interaction_id, call_sid, exotel_account_id):
        if not self._results:
            raise AssertionError(
                f"poll_recording_once called {self.calls + 1} times but "
                f"only {len(self._results) + self.calls} were scripted"
            )
        self.calls += 1
        return self._results.pop(0)


class _FakeSleep:
    """Records every requested delay instead of sleeping."""

    def __init__(self):
        self.delays: List[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


async def test_poll_succeeds_after_three_misses(db_session, monkeypatch):
    """AC4 — a recording that returns 404 for the first three polls, then
    200, ends with the row completed. Backoff schedule entries are observed
    in order. Each attempt produces an audit_events row, plus one for the
    terminal completion."""
    pipeline = await _create_recording_pipeline(db_session)
    recording_job_id = pipeline["job_ids"]["recording"]
    interaction_id = pipeline["interaction_id"]

    scripted = _ScriptedPoll([
        RecordingPollResult(status=RecordingPollStatus.NOT_READY, reason="404"),
        RecordingPollResult(status=RecordingPollStatus.NOT_READY, reason="404"),
        RecordingPollResult(status=RecordingPollStatus.NOT_READY, reason="404"),
        RecordingPollResult(
            status=RecordingPollStatus.READY,
            recording_url="https://exotel/rec/001.mp3",
        ),
    ])
    monkeypatch.setattr(recording_poller, "poll_recording_once", scripted)

    fake_sleep = _FakeSleep()

    await _poll_recording_async(
        str(recording_job_id),
        backoff_schedule=[5.0, 10.0, 20.0, 40.0, 80.0],
        jitter_frac=0.0,  # deterministic for assertion
        sleep=fake_sleep,
    )

    # Four poll calls (3 misses + 1 ready); three sleeps between them.
    assert scripted.calls == 4
    assert fake_sleep.delays == [5.0, 10.0, 20.0]

    state_row = await db_session.execute(
        text("SELECT state, result FROM processing_jobs WHERE id = :id"),
        {"id": str(recording_job_id)},
    )
    state = state_row.first()
    assert state.state == "completed"
    assert state.result["s3_key"].endswith(f"{interaction_id}.mp3")

    audits = await db_session.execute(
        text(
            "SELECT event_type, severity, details FROM audit_events "
            "WHERE interaction_id = :i ORDER BY id"
        ),
        {"i": str(interaction_id)},
    )
    rows = audits.all()
    event_types = [r.event_type for r in rows]
    # pipeline_created (from create_pipeline isn't written — we did that
    # outside the endpoint) + stage_leased + 4× recording_poll_attempt +
    # recording_completed.
    assert event_types.count("recording_poll_attempt") == 4
    assert "recording_completed" in event_types
    # The first three attempts must record the next backoff delay; the
    # fourth (READY) must not.
    poll_rows = [r for r in rows if r.event_type == "recording_poll_attempt"]
    assert poll_rows[0].details.get("next_delay_seconds") == 5.0
    assert poll_rows[1].details.get("next_delay_seconds") == 10.0
    assert poll_rows[2].details.get("next_delay_seconds") == 20.0
    assert poll_rows[3].details.get("next_delay_seconds") is None
    assert poll_rows[3].details.get("outcome") == "ready"


async def test_poll_exhaustion_writes_terminal_failure_event(db_session, monkeypatch):
    """AC4 — a recording that never arrives produces a terminal failure
    event at severity=error so the metrics scrape can alert on it."""
    pipeline = await _create_recording_pipeline(db_session)
    recording_job_id = pipeline["job_ids"]["recording"]
    interaction_id = pipeline["interaction_id"]

    # max_attempts=3 by default for the JOB; we want exhaustion of the
    # POLL budget which has its own count = len(schedule) + 1 = 3.
    scripted = _ScriptedPoll([
        RecordingPollResult(status=RecordingPollStatus.NOT_READY, reason="404"),
        RecordingPollResult(status=RecordingPollStatus.NOT_READY, reason="404"),
        RecordingPollResult(status=RecordingPollStatus.NOT_READY, reason="404"),
    ])
    monkeypatch.setattr(recording_poller, "poll_recording_once", scripted)
    fake_sleep = _FakeSleep()

    # Walk the job to its terminal state by repeatedly running the poller.
    # Each call leases (attempts += 1), exhausts the in-job poll budget,
    # and either retries the JOB (state=pending) or marks dead. Our default
    # max_attempts is 3, so 3 outer runs to reach dead.
    for run_idx in range(3):
        scripted.calls = 0
        scripted._results = [
            RecordingPollResult(status=RecordingPollStatus.NOT_READY, reason="404"),
            RecordingPollResult(status=RecordingPollStatus.NOT_READY, reason="404"),
            RecordingPollResult(status=RecordingPollStatus.NOT_READY, reason="404"),
        ]
        await _poll_recording_async(
            str(recording_job_id),
            backoff_schedule=[1.0, 2.0],  # 2 backoffs → 3 attempts
            jitter_frac=0.0,
            sleep=fake_sleep,
        )
        # Force scheduled_at into the past so the next lease is eligible
        # without waiting for the retry delay.
        await db_session.execute(
            text(
                "UPDATE processing_jobs SET scheduled_at = NOW() - INTERVAL '5 minutes' "
                "WHERE id = :id"
            ),
            {"id": str(recording_job_id)},
        )
        await db_session.commit()

    state_row = await db_session.execute(
        text("SELECT state FROM processing_jobs WHERE id = :id"),
        {"id": str(recording_job_id)},
    )
    assert state_row.first().state == "dead"

    audits = await db_session.execute(
        text(
            "SELECT event_type, severity FROM audit_events "
            "WHERE interaction_id = :i AND event_type LIKE 'recording_failed%' "
            "ORDER BY id"
        ),
        {"i": str(interaction_id)},
    )
    failure_events = audits.all()
    # We expect 2 will_retry (first two job attempts) + 1 terminal (third).
    assert any(e.event_type == "recording_failed_terminal" for e in failure_events)
    terminal = next(e for e in failure_events if e.event_type == "recording_failed_terminal")
    assert terminal.severity == "error"


async def test_missing_call_sid_terminates_immediately(db_session, monkeypatch):
    """A recording job for an interaction that never connected (no call_sid)
    short-circuits to terminal failure on the first poll without burning
    the full backoff budget."""
    pipeline = await _create_recording_pipeline(db_session, call_sid="")
    recording_job_id = pipeline["job_ids"]["recording"]
    interaction_id = pipeline["interaction_id"]

    fake_sleep = _FakeSleep()

    # No need to monkeypatch; the real poll_recording_once short-circuits
    # on empty call_sid.
    # Walk through max_attempts to confirm we eventually go dead.
    for _ in range(3):
        await _poll_recording_async(
            str(recording_job_id),
            backoff_schedule=[1.0, 2.0],
            jitter_frac=0.0,
            sleep=fake_sleep,
        )
        await db_session.execute(
            text(
                "UPDATE processing_jobs SET scheduled_at = NOW() - INTERVAL '5 minutes' "
                "WHERE id = :id"
            ),
            {"id": str(recording_job_id)},
        )
        await db_session.commit()

    # No sleeps should have happened — every attempt terminates on the
    # first poll with PERMANENT_FAILURE.
    assert fake_sleep.delays == []

    state_row = await db_session.execute(
        text("SELECT state FROM processing_jobs WHERE id = :id"),
        {"id": str(recording_job_id)},
    )
    assert state_row.first().state == "dead"

    audits = await db_session.execute(
        text(
            "SELECT event_type, details FROM audit_events "
            "WHERE interaction_id = :i AND stage = 'recording' "
            "ORDER BY id"
        ),
        {"i": str(interaction_id)},
    )
    rows = audits.all()
    poll_rows = [r for r in rows if r.event_type == "recording_poll_attempt"]
    # One poll attempt per outer run, terminating immediately each time.
    assert len(poll_rows) == 3
    assert all(r.details.get("outcome") == "permanent_failure" for r in poll_rows)


async def test_reconcile_resets_stuck_leased_recordings(db_session):
    """The reconciliation beat catches recording jobs stuck in `leased`
    state past the stuck threshold and resets them to pending. An audit
    event is written for each reset row."""
    pipeline = await _create_recording_pipeline(db_session)
    recording_job_id = pipeline["job_ids"]["recording"]
    interaction_id = pipeline["interaction_id"]

    # Manually transition the row to leased with an old leased_at to simulate
    # a worker that uploaded to S3 and crashed before completing the row.
    await db_session.execute(
        text("""
            UPDATE processing_jobs
            SET state = 'leased',
                lease_token = :tok,
                leased_at = NOW() - INTERVAL '10 minutes',
                lease_expires_at = NOW() + INTERVAL '1 hour',
                worker_id = 'crashed-worker'
            WHERE id = :id
        """),
        {"id": str(recording_job_id), "tok": str(uuid4())},
    )
    await db_session.commit()

    # The reconciler dispatches via Celery. Stub apply_async so it doesn't
    # try to talk to a broker during the test.
    apply_async_calls = []

    class _StubTask:
        def apply_async(self, **kwargs):
            apply_async_calls.append(kwargs)

    import src.tasks.recording_poller as poller_mod
    real_task = poller_mod.poll_recording
    poller_mod.poll_recording = _StubTask()
    try:
        count = await _reconcile_async()
    finally:
        poller_mod.poll_recording = real_task

    assert count == 1
    assert len(apply_async_calls) == 1

    state_row = await db_session.execute(
        text("SELECT state, lease_token FROM processing_jobs WHERE id = :id"),
        {"id": str(recording_job_id)},
    )
    state = state_row.first()
    assert state.state == "pending"
    assert state.lease_token is None

    audit = await db_session.execute(
        text(
            "SELECT event_type, severity FROM audit_events "
            "WHERE interaction_id = :i AND event_type = 'recording_reconciled_to_pending'"
        ),
        {"i": str(interaction_id)},
    )
    rows = audit.all()
    assert len(rows) == 1
    assert rows[0].severity == "warning"
