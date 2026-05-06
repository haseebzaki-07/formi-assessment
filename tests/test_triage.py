"""
Phase 3 tests — triage classifier + hot/cold lane routing.

Validates:
  - Stage A (keyword/regex) classifies the eight fixture transcripts to
    their `expected_lane`. The `hinglish_ambiguous` case routes to Stage B
    (LLM fallback) and Stage B's mock returns cold.
  - Per-customer overrides flip a disposition's lane (already_done →
    hot for an upsell-focused customer).
  - queue_for_lane maps lane × stage to the correct Celery queue name.
  - Integration test: a hot-lane analysis row queued behind cold backlog
    is leased first by virtue of priority=1 winning the ORDER BY in
    lease_next.

The DB-backed integration test requires Postgres reachable at
DATABASE_URL; it skips cleanly if not.
"""

from datetime import datetime
from unittest.mock import patch
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text

from src.services import job_store, triage
from src.services.triage import (
    LANE_COLD,
    LANE_HOT,
    LANE_SKIP,
    PRIORITY_COLD,
    PRIORITY_HOT,
)
from src.tasks.celery_tasks import queue_for_lane
from src.config import settings


# ── Stage A: keyword classification of fixture transcripts ────────────────────

@pytest.mark.parametrize(
    "transcript_key, expected_lane",
    [
        ("rebook_confirmed", LANE_HOT),
        ("not_interested", LANE_COLD),
        ("callback_requested", LANE_COLD),
        ("demo_booked", LANE_HOT),
        ("already_purchased", LANE_COLD),
        ("short_call_hangup", LANE_SKIP),
        ("escalation_needed", LANE_HOT),
    ],
)
async def test_fixture_routes_to_expected_lane(
    sample_transcripts, transcript_key, expected_lane,
):
    """Each fixture's transcript routes to its `expected_lane` label.

    `hinglish_ambiguous` is excluded — it's the canonical Stage B case
    and is covered by its own test below.
    """
    transcript = sample_transcripts[transcript_key]["transcript"]
    result = await triage.classify(transcript)
    assert result.lane == expected_lane, (
        f"{transcript_key}: expected {expected_lane}, got {result.lane}"
        f" (scores={result.scores}, reason={result.reason})"
    )


async def test_hinglish_ambiguous_routes_to_stage_b(sample_transcripts):
    """The hinglish_ambiguous fixture is the canonical Stage B test —
    keyword scoring alone shouldn't commit to a lane; the LLM fallback
    should. The default Stage B heuristic returns cold for "considering"
    transcripts."""
    transcript = sample_transcripts["hinglish_ambiguous"]["transcript"]
    result = await triage.classify(transcript)
    assert result.stage_used == "llm", (
        f"expected stage_b, got stage_used={result.stage_used} "
        f"(scores={result.scores}, reason={result.reason})"
    )
    assert result.lane == LANE_COLD


async def test_skip_lane_for_short_transcript():
    """A two-turn transcript is skip even without a "wrong number" phrase
    — length alone forces the skip path."""
    short = [
        {"role": "agent", "content": "Hello?"},
        {"role": "customer", "content": "Hi."},
    ]
    result = await triage.classify(short)
    assert result.lane == LANE_SKIP
    assert result.stage_used == "length"


async def test_skip_pattern_matches_wrong_number():
    """An explicit "wrong number" hangup is skip even with enough turns
    that the length check would pass."""
    transcript = [
        {"role": "agent", "content": "Hello, am I speaking with Mr. Sharma?"},
        {"role": "customer", "content": "Wrong number, mate."},
        {"role": "agent", "content": "Sorry to disturb you, sir."},
        {"role": "customer", "content": "Don't bother."},
    ]
    result = await triage.classify(transcript)
    assert result.lane == LANE_SKIP


# ── Priority assignment + reservation hint shape ──────────────────────────────

async def test_hot_lane_gets_priority_one(sample_transcripts):
    transcript = sample_transcripts["rebook_confirmed"]["transcript"]
    result = await triage.classify(transcript)
    assert result.priority == PRIORITY_HOT


async def test_cold_lane_gets_priority_nine(sample_transcripts):
    transcript = sample_transcripts["not_interested"]["transcript"]
    result = await triage.classify(transcript)
    assert result.priority == PRIORITY_COLD


# ── Customer overrides: opt a disposition into a different lane ───────────────

async def test_customer_override_flips_disposition_lane(sample_transcripts):
    """A customer that wants every "already done" call escalated for
    upsell can map already_done → hot via customer_overrides without a
    deploy. Stage B applies the override after disambiguation."""
    transcript = sample_transcripts["hinglish_ambiguous"]["transcript"]

    # Stub Stage B to return a known disposition we can override.
    async def fake_stage_b(turns, customer_overrides=None):
        return LANE_COLD, "considering", 0.7

    with patch(
        "src.services.triage._stage_b_llm_fallback",
        side_effect=fake_stage_b,
    ):
        baseline = await triage.classify(transcript)
        overridden = await triage.classify(
            transcript,
            customer_overrides={"considering": LANE_HOT},
        )

    assert baseline.lane == LANE_COLD
    assert overridden.lane == LANE_HOT
    assert overridden.priority == PRIORITY_HOT


# ── queue_for_lane: routing matrix ────────────────────────────────────────────

def test_queue_for_lane_routes_analysis_by_lane():
    assert queue_for_lane("hot", "analysis") == settings.POSTCALL_HOT_QUEUE
    assert queue_for_lane("cold", "analysis") == settings.POSTCALL_COLD_QUEUE


def test_queue_for_lane_falls_back_to_default_for_other_stages():
    """signal_jobs / lead_stage / unknown stages stay on the default queue
    regardless of lane — they're not LLM-bound and shouldn't compete for
    hot-lane analysis worker slots."""
    assert (
        queue_for_lane("hot", "signal_jobs") == settings.POSTCALL_CELERY_QUEUE
    )
    assert (
        queue_for_lane("cold", "lead_stage") == settings.POSTCALL_CELERY_QUEUE
    )
    assert (
        queue_for_lane(None, "analysis") == settings.POSTCALL_CELERY_QUEUE
    )


# ── Integration: hot row queues behind cold backlog, leased first ────────────

@pytest_asyncio.fixture
async def db_session():
    from src.utils.db import async_session_factory
    async with async_session_factory() as session:
        try:
            await session.execute(
                text("TRUNCATE processing_jobs, audit_events RESTART IDENTITY")
            )
            await session.commit()
        except Exception as exc:
            pytest.skip(
                f"Postgres unavailable or schema not loaded: {exc}"
            )
        yield session


def _payload(turns: int = 8) -> dict:
    return {
        "interaction_id": "set-by-test",
        "session_id": str(uuid4()),
        "lead_id": str(uuid4()),
        "campaign_id": str(uuid4()),
        "customer_id": str(uuid4()),
        "agent_id": str(uuid4()),
        "call_sid": "test-sid",
        "transcript_text": "agent: ...",
        "conversation_data": {
            "transcript": [
                {"role": "agent", "content": f"turn {i}"} for i in range(turns)
            ]
        },
        "additional_data": {},
        "ended_at": datetime.utcnow().isoformat(),
        "exotel_account_id": "test-account",
    }


async def test_hot_row_leased_ahead_of_cold_backlog(db_session):
    """Build 5 cold-lane analysis rows and 1 hot-lane row, all eligible.
    lease_next on the analysis stage must hand back the hot row first
    because priority=1 wins the ORDER BY priority ASC, scheduled_at ASC."""
    cold_ids = []
    for _ in range(5):
        ids = await job_store.create_pipeline(
            db_session,
            interaction_id=uuid4(),
            customer_id=uuid4(),
            campaign_id=uuid4(),
            payload=_payload(),
            has_long_transcript=True,
            lane=LANE_COLD,
            priority=PRIORITY_COLD,
        )
        cold_ids.append(ids["analysis"])

    hot_ids = await job_store.create_pipeline(
        db_session,
        interaction_id=uuid4(),
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload(),
        has_long_transcript=True,
        lane=LANE_HOT,
        priority=PRIORITY_HOT,
    )
    await db_session.commit()

    leased = await job_store.lease_next(
        db_session,
        stage="analysis",
        worker_id="hot-worker",
        lease_seconds=60,
    )
    await db_session.commit()
    assert leased is not None
    assert str(leased["id"]) == str(hot_ids["analysis"]), (
        "hot row must be leased before cold backlog"
    )
    assert leased.get("lane") == LANE_HOT
    assert leased.get("priority") == PRIORITY_HOT


async def test_create_pipeline_persists_lane_and_priority(db_session):
    """Phase 3 columns are written when create_pipeline is called with
    the lane/priority kwargs."""
    interaction_id = uuid4()
    job_ids = await job_store.create_pipeline(
        db_session,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload(),
        has_long_transcript=True,
        lane=LANE_HOT,
        priority=PRIORITY_HOT,
    )
    await db_session.commit()

    rows = await db_session.execute(
        text(
            "SELECT stage, lane, priority FROM processing_jobs "
            "WHERE interaction_id = :i"
        ),
        {"i": str(interaction_id)},
    )
    by_stage = {r.stage: r for r in rows.all()}
    assert by_stage["analysis"].lane == LANE_HOT
    assert by_stage["analysis"].priority == PRIORITY_HOT
    # All rows for this interaction share the lane label so the dependent
    # dispatch can route consistently.
    assert by_stage["recording"].lane == LANE_HOT
    assert by_stage["signal_jobs"].lane == LANE_HOT
    assert by_stage["lead_stage"].lane == LANE_HOT


async def test_cold_lane_defers_analysis_scheduled_at(db_session):
    """Cold-lane analysis is scheduled `cold_defer_seconds` in the future
    so it yields to hot work even when the cold queue has free workers.
    Other stages stay on NOW(), since they're not the bottleneck."""
    interaction_id = uuid4()
    job_ids = await job_store.create_pipeline(
        db_session,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload(),
        has_long_transcript=True,
        lane=LANE_COLD,
        priority=PRIORITY_COLD,
        cold_defer_seconds=120,
    )
    await db_session.commit()

    rows = await db_session.execute(
        text(
            "SELECT stage, scheduled_at, "
            "EXTRACT(EPOCH FROM (scheduled_at - NOW())) AS delay_s "
            "FROM processing_jobs WHERE interaction_id = :i"
        ),
        {"i": str(interaction_id)},
    )
    by_stage = {r.stage: r for r in rows.all()}
    # Analysis pushed forward (within tolerance for clock drift).
    assert by_stage["analysis"].delay_s > 60
    # Recording / signal_jobs / lead_stage stay on NOW().
    assert by_stage["recording"].delay_s < 5


async def test_skip_lane_pipeline_marks_analysis_skipped(db_session):
    """A skip-lane pipeline (short transcript / wrong number) creates the
    analysis row in `skipped` state with a pre-populated short_call result.
    Downstream stages are pending immediately, no LLM call ever fires."""
    interaction_id = uuid4()
    await job_store.create_pipeline(
        db_session,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload=_payload(turns=2),
        has_long_transcript=False,
        lane=LANE_SKIP,
    )
    await db_session.commit()

    rows = await db_session.execute(
        text(
            "SELECT stage, state, lane, result FROM processing_jobs "
            "WHERE interaction_id = :i"
        ),
        {"i": str(interaction_id)},
    )
    by_stage = {r.stage: r for r in rows.all()}
    assert by_stage["analysis"].state == "skipped"
    assert by_stage["analysis"].lane == LANE_SKIP
    assert by_stage["analysis"].result is not None
    assert by_stage["analysis"].result.get("call_stage") == "short_call"
    assert by_stage["signal_jobs"].state == "pending"
    assert by_stage["lead_stage"].state == "pending"


# ── Reservation-size hint exposed for Phase 2's rate limiter ──────────────────

def test_reservation_estimate_caps_for_cold_lane():
    """The cold-lane reservation hint caps at COLD_LANE_RESERVATION_TOKENS,
    even when the heuristic estimate is larger. This is what frees
    headroom for hot-lane reservations under contention."""
    from src.services.post_call_processor import PostCallProcessor

    processor = PostCallProcessor()
    cold = processor._reservation_estimate_for_lane(2000, "cold")
    hot = processor._reservation_estimate_for_lane(2000, "hot")
    nolane = processor._reservation_estimate_for_lane(2000, None)

    assert cold == settings.COLD_LANE_RESERVATION_TOKENS
    assert hot <= settings.HOT_LANE_RESERVATION_TOKENS
    assert nolane == 2000


def test_cold_lane_uses_cheaper_prompt():
    """Cold lane gets a summary-only system prompt; hot lane gets the full
    analysis prompt with entity extraction."""
    from src.services.post_call_processor import PostCallProcessor

    processor = PostCallProcessor()
    cold_prompt = processor._build_analysis_prompt(
        "agent: ...", {}, single_prompt=True, lane="cold",
    )
    hot_prompt = processor._build_analysis_prompt(
        "agent: ...", {}, single_prompt=True, lane="hot",
    )
    assert "entities" not in cold_prompt
    assert "entities" in hot_prompt
    assert len(cold_prompt) < len(hot_prompt)
