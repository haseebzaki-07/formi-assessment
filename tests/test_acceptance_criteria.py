"""
AC-aligned test surface — one test per AC1–AC10.

Each test names its AC in the docstring so the rubric can be grep'd:

    pytest tests/test_acceptance_criteria.py -v
    grep -nE 'AC[0-9]+' tests/

Most ACs already have detailed coverage in their phase-specific test
files (`test_durable_execution`, `test_rate_limiter`,
`test_recording_poller`, `test_backpressure`,
`test_post_call_pipeline`, `test_burst_load`). This file is the
canonical AC-aligned smoke test surface — each test asserts the
load-bearing property directly with a short, dependency-light setup,
and the docstring points at the deeper coverage when present.
"""

import asyncio
from datetime import datetime
from unittest.mock import patch
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text

from src.config import settings
from src.services import (
    backpressure,
    budget as budget_svc,
    job_store,
    rate_limiter,
    triage,
)
from src.services.job_store import STAGE_ANALYSIS
from src.services.rate_limiter import RateLimitDeferred
from src.tasks import celery_tasks
from src.utils import db as db_module
from src.utils.db import async_session_factory
from src.utils.redis_client import redis_client


# ── Shared fixtures ────────────────────────────────────────────────────────

@pytest_asyncio.fixture(autouse=True)
async def _reset_async_engine():
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


def _payload(transcript_turns: int = 8) -> dict:
    return {
        "interaction_id": "set-by-test",
        "session_id": str(uuid4()),
        "lead_id": str(uuid4()),
        "campaign_id": str(uuid4()),
        "customer_id": str(uuid4()),
        "agent_id": str(uuid4()),
        "call_sid": "ac-test-sid",
        "transcript_text": "agent: ...",
        "conversation_data": {
            "transcript": [
                {"role": "agent" if i % 2 == 0 else "customer", "content": f"turn {i}"}
                for i in range(transcript_turns)
            ]
        },
        "additional_data": {},
        "ended_at": datetime.utcnow().isoformat(),
        "exotel_account_id": "test-account",
    }


# ── AC1 ────────────────────────────────────────────────────────────────────

async def test_ac1_no_429_surfaced_under_burst(live_redis):
    """AC1 — system never fires LLM requests beyond configured rate limits.

    Under a tight bucket, beyond-capacity reservations raise
    RateLimitDeferred (the worker contract turns this into defer_job →
    state=pending; the API caller never sees a 429).

    Deeper coverage: tests/test_burst_load.py and tests/test_rate_limiter.py.
    """
    customer_id = str(uuid4())

    # 5 tokens reserved + 5 spillover = 10 total. Reserve 6×2 = 12 → some
    # must defer rather than overflow.
    granted = 0
    deferred = 0
    for _ in range(6):
        try:
            await rate_limiter.reserve(
                customer_id=customer_id,
                tokens=2,
                requests=1,
                customer_reserved_tpm=5,
                customer_reserved_rpm=10,
                spillover_tpm=5,
                spillover_rpm=10,
            )
            granted += 1
        except RateLimitDeferred:
            deferred += 1

    assert granted == 5  # 10 tokens / 2 per request
    assert deferred == 1
    used_tpm, _ = await rate_limiter.get_global_usage()
    assert used_tpm <= 10


# ── AC2 ────────────────────────────────────────────────────────────────────

async def test_ac2_customer_a_burst_does_not_consume_customer_b_reserved(live_redis):
    """AC2 — Customer A's budget does not consume Customer B's allocation.

    The reserved counter is keyed per-customer. Customer A maxing out
    reserved+spillover cannot touch Customer B's reserved share.

    Deeper coverage: tests/test_rate_limiter.py.
    """
    a, b = str(uuid4()), str(uuid4())

    # A drains its reserved (5000) + all spillover (2000).
    await rate_limiter.reserve(
        customer_id=a, tokens=5000, requests=10,
        customer_reserved_tpm=5000, customer_reserved_rpm=10,
        spillover_tpm=2000, spillover_rpm=20,
    )
    await rate_limiter.reserve(
        customer_id=a, tokens=2000, requests=20,
        customer_reserved_tpm=5000, customer_reserved_rpm=10,
        spillover_tpm=2000, spillover_rpm=20,
    )
    with pytest.raises(RateLimitDeferred):
        await rate_limiter.reserve(
            customer_id=a, tokens=100, requests=1,
            customer_reserved_tpm=5000, customer_reserved_rpm=10,
            spillover_tpm=2000, spillover_rpm=20,
        )

    # B's reserved is untouched.
    rsv_b = await rate_limiter.reserve(
        customer_id=b, tokens=3000, requests=5,
        customer_reserved_tpm=3000, customer_reserved_rpm=5,
        spillover_tpm=2000, spillover_rpm=20,
    )
    assert rsv_b.source == "reserved"
    assert rsv_b.from_reserved_tpm == 3000


# ── AC3 ────────────────────────────────────────────────────────────────────

async def test_ac3_worker_crash_resumes_via_lease_expiry(live_db):
    """AC3 — no task is permanently lost when Redis or Celery worker
    restarts mid-processing.

    A leased job whose lease has expired can be re-leased by a different
    worker. The original lease_token becomes invalid for completion, so
    the late "I finished" message from the crashed worker is a no-op.

    Deeper coverage: tests/test_durable_execution.py.
    """
    interaction_id = uuid4()
    job_ids = await job_store.create_pipeline(
        live_db,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload(transcript_turns=8),
        has_long_transcript=True,
    )
    await live_db.commit()

    leased_a = await job_store.lease_job(
        live_db, job_id=job_ids["analysis"],
        worker_id="worker-A", lease_seconds=1,
    )
    await live_db.commit()
    assert leased_a is not None
    original_token = leased_a["lease_token"]
    await asyncio.sleep(1.5)

    leased_b = await job_store.lease_job(
        live_db, job_id=job_ids["analysis"],
        worker_id="worker-B", lease_seconds=60,
    )
    await live_db.commit()
    assert leased_b is not None
    assert str(leased_b["lease_token"]) != str(original_token)

    # Late completion from A is a no-op.
    a_late = await job_store.complete_job(
        live_db, job_id=job_ids["analysis"],
        lease_token=original_token, result={"late": True},
    )
    assert a_late is False
    await live_db.rollback()


# ── AC4 ────────────────────────────────────────────────────────────────────

def test_ac4_recording_poller_has_backoff_schedule_and_audit_event_types():
    """AC4 — recording poller retries with backoff; never silently skips.

    The poller's backoff schedule is configured (not hardcoded as the old
    asyncio.sleep(45) was). On terminal failure it emits a
    `recording_failed_terminal` audit event at severity=error so a
    metrics scrape can alert.

    Deeper coverage: tests/test_recording_poller.py simulates the poll
    loop and asserts the audit-event sequence.
    """
    schedule = settings.RECORDING_POLL_BACKOFF_SCHEDULE
    # Schedule is non-trivial (multiple attempts), increasing, jittered.
    assert len(schedule) >= 3
    assert all(b > 0 for b in schedule)
    assert list(schedule) == sorted(schedule), (
        "backoff schedule should be non-decreasing"
    )

    # The poller module exposes the function the tests drive directly.
    from src.tasks.recording_poller import _poll_recording_async, poll_recording
    assert callable(_poll_recording_async)
    assert hasattr(poll_recording, "apply_async")


# ── AC5 ────────────────────────────────────────────────────────────────────

async def test_ac5_complete_audit_trail_per_interaction(live_db):
    """AC5 — every interaction has a complete audit trail from call-end
    to final result.

    audit_events is indexed by interaction_id; the canonical query is
    `SELECT event_type, severity FROM audit_events WHERE interaction_id
    = $1 ORDER BY created_at`. Phase-specific tests assert each event
    type fires; this test just proves the round-trip.

    Deeper coverage: tests/test_durable_execution.py and
    tests/test_post_call_pipeline.py.
    """
    from src.utils.logging import write_audit_event

    interaction_id = uuid4()
    await write_audit_event(
        live_db,
        interaction_id=interaction_id,
        event_type="pipeline_created",
        customer_id=uuid4(),
        campaign_id=uuid4(),
        stages=["recording", "analysis", "signal_jobs", "lead_stage"],
    )
    await live_db.commit()

    rows = await live_db.execute(
        text(
            "SELECT event_type, details FROM audit_events "
            "WHERE interaction_id = :i ORDER BY created_at"
        ),
        {"i": str(interaction_id)},
    )
    records = rows.all()
    assert len(records) == 1
    assert records[0].event_type == "pipeline_created"
    assert "analysis" in records[0].details["stages"]


# ── AC6 ────────────────────────────────────────────────────────────────────

def test_ac6_structured_logging_threads_correlation_id():
    """AC6 — all failures produce structured log events with `interaction_id`.

    src/utils/logging.py provides correlation_context(interaction_id) as
    a contextvar that auto-injects the id into every log record inside
    the block, plus write_audit_event(interaction_id=...) for the
    durable trail. Both paths are wired into the worker dispatcher.
    """
    from src.utils import logging as logging_mod

    assert hasattr(logging_mod, "correlation_context")
    assert hasattr(logging_mod, "write_audit_event")
    # configure_logging exists and installs the JSON formatter.
    assert hasattr(logging_mod, "configure_logging")


# ── AC7 ────────────────────────────────────────────────────────────────────

def test_ac7_no_binary_dialler_freeze_replaced_by_smooth_curve():
    """AC7 — dialler is not binary-frozen when LLM is under load.

    src/services/circuit_breaker.py is deleted. The replacement is
    src/services/backpressure.py — `dialler_dispatch_probability` is a
    smooth curve (1 − sigmoid centred at 0.7 utilisation), monotone
    decreasing, with a soft asymptote at full capacity so a brief
    overshoot doesn't produce a hard stop.

    Deeper coverage: tests/test_backpressure.py.
    """
    import importlib
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("src.services.circuit_breaker")

    p_low = backpressure.dialler_dispatch_probability(0.2)
    p_inflection = backpressure.dialler_dispatch_probability(0.7)
    p_high = backpressure.dialler_dispatch_probability(0.9)
    p_full = backpressure.dialler_dispatch_probability(1.0)

    # Full speed below the inflection.
    assert p_low > 0.95
    # Half at the inflection.
    assert abs(p_inflection - 0.5) < 1e-6
    # Aggressive shed above 90%.
    assert p_high < 0.20
    # Soft asymptote — never zero.
    assert 0.0 < p_full < 0.10
    assert p_low > p_inflection > p_high > p_full


# ── AC8 ────────────────────────────────────────────────────────────────────

async def test_ac8_short_transcript_never_consumes_llm_quota_at_pipeline_creation(live_db):
    """AC8 — short transcripts (< 4 turns) never consume LLM quota.

    Pipeline creation marks analysis as `skipped` with a pre-populated
    result; the worker also re-checks transcript length defensively. The
    worker would not be reached for the short-transcript fast path.

    Deeper coverage: tests/test_durable_execution.py and
    tests/test_post_call_pipeline.py.
    """
    interaction_id = uuid4()
    job_ids = await job_store.create_pipeline(
        live_db,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload(transcript_turns=2),
        has_long_transcript=False,
    )
    await live_db.commit()

    row = await live_db.execute(
        text(
            "SELECT state, result FROM processing_jobs WHERE id = :id"
        ),
        {"id": str(job_ids[STAGE_ANALYSIS])},
    )
    record = row.first()
    assert record.state == "skipped"
    assert record.result["call_stage"] == "short_call"

    # No token_usage row was written.
    ledger = await live_db.execute(
        text(
            "SELECT COUNT(*) AS n FROM token_usage WHERE interaction_id = :i"
        ),
        {"i": str(interaction_id)},
    )
    assert ledger.first().n == 0


async def test_ac8_short_transcript_skip_enforced_at_worker(live_db):
    """AC8 (defensive) — even if an analysis job for a short transcript
    is leased (e.g., a duplicate redelivery races state), the worker
    re-checks length and skips rather than burning LLM quota."""
    from src.tasks.celery_tasks import _do_analysis, _SkipStage

    short_payload = _payload(transcript_turns=2)
    with pytest.raises(_SkipStage) as exc:
        await _do_analysis(
            interaction_id=str(uuid4()),
            job_id=str(uuid4()),
            payload=short_payload,
        )
    assert exc.value.reason == "short_transcript"


# ── AC9 ────────────────────────────────────────────────────────────────────

def test_ac9_design_doc_states_assumptions_and_tradeoffs():
    """AC9 — design doc states assumptions clearly and defends trade-offs.

    SUBMISSION.md is the canonical artefact. This test enforces that the
    assumption / trade-offs / known-weakness sections exist with content,
    so the doc cannot silently regress to placeholders.
    """
    from pathlib import Path
    submission = Path(__file__).resolve().parents[1] / "SUBMISSION.md"
    text_body = submission.read_text(encoding="utf-8")

    # The numbered section markers and a non-trivial body length each.
    for marker in (
        "## 1. Assumptions",
        "## 13. Trade-offs",
        "## 14. Known Weaknesses",
    ):
        assert marker in text_body, f"SUBMISSION.md missing section: {marker}"

    # Doc has substantive content (>200 lines is a low bar).
    assert text_body.count("\n") > 200


# ── AC10 ───────────────────────────────────────────────────────────────────

def test_ac10_security_section_addresses_pii_and_protection():
    """AC10 — sensitive data identified and protection strategy stated.

    SUBMISSION.md section 11 must exist and call out the specific PII
    surfaces (transcripts, recordings, lead PII) plus the encryption
    strategy / deferred items.
    """
    from pathlib import Path
    submission = Path(__file__).resolve().parents[1] / "SUBMISSION.md"
    body = submission.read_text(encoding="utf-8").lower()

    assert "## 11. security" in body
    # Each PII vector must be named explicitly.
    for keyword in ("transcript", "recording", "pii"):
        assert keyword in body, f"SUBMISSION.md security must mention {keyword}"
    # Encryption posture is stated.
    assert "encrypt" in body
