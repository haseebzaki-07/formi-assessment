"""
Phase 2 tests — rate limiter + per-customer budgets.

Validates:
  - AC1: under burst, reservations are gated (no 429 surfaced to API caller).
    Beyond capacity, RateLimitDeferred is raised; the worker contract turns
    that into a soft retry via defer_job.
  - AC2: Customer A exhausting reserved + spillover does not consume
    Customer B's reserved share.
  - True-up arithmetic (charge_extra over, refund_tokens under, refund full).
  - Provider 429 absorption — apply_provider_429_backoff multiplicatively
    decreases the customer's effective TPM for the next minute.
  - defer_job semantics — soft retry that does not consume the work-attempt
    budget, but is bounded by max_defers so a permanently-constrained job
    doesn't spin forever.

Requires Redis + Postgres. Tests skip with a clear message on connection
errors. Redis DB 0 is FLUSHDB'd before each test that uses live_redis;
Phase 2 tables and processing_jobs are TRUNCATE'd by live_db.
"""

import asyncio
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text

from src.services import budget as budget_svc
from src.services import job_store
from src.services import rate_limiter
from src.services.rate_limiter import (
    RateLimitDeferred,
    estimate_tokens_for_transcript,
)
from src.utils.db import async_session_factory
from src.utils.redis_client import redis_client


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
    """Live Postgres session. Truncates the Phase 1/2 tables that the rate
    limiter and ledger touch — leaves customer_budgets seeds intact since
    the budget service reads them."""
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


# ── Reservation behaviour (AC1) ────────────────────────────────────────────

async def test_reserve_grants_within_reserved_quota(live_redis):
    cid = str(uuid4())
    for _ in range(5):
        rsv = await rate_limiter.reserve(
            customer_id=cid,
            tokens=1500,
            requests=1,
            customer_reserved_tpm=10000,
            customer_reserved_rpm=50,
            spillover_tpm=5000,
            spillover_rpm=25,
        )
        assert rsv.source == "reserved"


async def test_reserve_dips_into_spillover_when_reserved_exhausted(live_redis):
    cid = str(uuid4())
    first = await rate_limiter.reserve(
        customer_id=cid, tokens=1000, requests=1,
        customer_reserved_tpm=1000, customer_reserved_rpm=50,
        spillover_tpm=2000, spillover_rpm=50,
    )
    assert first.source == "reserved"

    second = await rate_limiter.reserve(
        customer_id=cid, tokens=500, requests=1,
        customer_reserved_tpm=1000, customer_reserved_rpm=50,
        spillover_tpm=2000, spillover_rpm=50,
    )
    assert second.source == "spillover"
    assert second.from_reserved_tpm == 0
    assert second.from_spill_tpm == 500


async def test_reserve_rejects_when_both_buckets_exhausted(live_redis):
    """AC1 — beyond capacity, the limiter raises RateLimitDeferred. The
    worker's responsibility is to defer the job, not surface a 429."""
    cid = str(uuid4())
    await rate_limiter.reserve(
        customer_id=cid, tokens=1000, requests=1,
        customer_reserved_tpm=1000, customer_reserved_rpm=50,
        spillover_tpm=500, spillover_rpm=50,
    )
    await rate_limiter.reserve(
        customer_id=cid, tokens=500, requests=1,
        customer_reserved_tpm=1000, customer_reserved_rpm=50,
        spillover_tpm=500, spillover_rpm=50,
    )
    with pytest.raises(RateLimitDeferred) as exc_info:
        await rate_limiter.reserve(
            customer_id=cid, tokens=100, requests=1,
            customer_reserved_tpm=1000, customer_reserved_rpm=50,
            spillover_tpm=500, spillover_rpm=50,
        )
    assert exc_info.value.reason in ("tpm_exhausted", "rpm_exhausted")
    assert exc_info.value.retry_after_seconds >= 1


async def test_burst_of_reservations_never_exceeds_combined_capacity(live_redis):
    """AC1 — a synthetic burst of N reservations against a fixed bucket
    only grants up to (reserved + spillover); the rest are deferred."""
    cid = str(uuid4())
    granted = 0
    rejected = 0
    for _ in range(50):
        try:
            await rate_limiter.reserve(
                customer_id=cid, tokens=1000, requests=1,
                customer_reserved_tpm=10000,
                customer_reserved_rpm=200,
                spillover_tpm=5000,
                spillover_rpm=200,
            )
            granted += 1
        except RateLimitDeferred:
            rejected += 1

    # 10 from reserved + 5 from spillover = 15 grants; rest rejected.
    assert granted == 15
    assert rejected == 35


# ── Per-customer isolation (AC2) ──────────────────────────────────────────

async def test_customer_a_burst_does_not_consume_customer_b_reserved(live_redis):
    """AC2 — the structural guarantee: each customer has its own counter
    key for the reserved share. Customer A maxing out reserved + spillover
    does not touch Customer B's reserved counter."""
    a = "11111111-1111-1111-1111-111111111111"
    b = "22222222-2222-2222-2222-222222222222"

    # A consumes its full reserved share (5000) plus all spillover (2000).
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

    # A's next request must be rejected.
    with pytest.raises(RateLimitDeferred):
        await rate_limiter.reserve(
            customer_id=a, tokens=100, requests=1,
            customer_reserved_tpm=5000, customer_reserved_rpm=10,
            spillover_tpm=2000, spillover_rpm=20,
        )

    # B's reserved share is intact and grantable, even though spillover
    # is exhausted system-wide.
    rsv_b = await rate_limiter.reserve(
        customer_id=b, tokens=3000, requests=5,
        customer_reserved_tpm=3000, customer_reserved_rpm=5,
        spillover_tpm=2000, spillover_rpm=20,
    )
    assert rsv_b.source == "reserved"
    assert rsv_b.from_reserved_tpm == 3000
    assert rsv_b.from_spill_tpm == 0


# ── True-up arithmetic ────────────────────────────────────────────────────

async def test_charge_extra_increases_bucket(live_redis):
    cid = str(uuid4())
    rsv = await rate_limiter.reserve(
        customer_id=cid, tokens=1000, requests=1,
        customer_reserved_tpm=2000, customer_reserved_rpm=10,
        spillover_tpm=1000, spillover_rpm=10,
    )
    await rate_limiter.charge_extra(rsv, extra_tokens=200)

    # 1000 + 200 = 1200 used out of 2000 reserved + 1000 spillover = 3000.
    # Available now: 1800. A 1900-token request must be rejected.
    with pytest.raises(RateLimitDeferred):
        await rate_limiter.reserve(
            customer_id=cid, tokens=1900, requests=1,
            customer_reserved_tpm=2000, customer_reserved_rpm=10,
            spillover_tpm=1000, spillover_rpm=10,
        )


async def test_refund_tokens_under_estimate_returns_capacity(live_redis):
    cid = str(uuid4())
    rsv = await rate_limiter.reserve(
        customer_id=cid, tokens=1500, requests=1,
        customer_reserved_tpm=1000, customer_reserved_rpm=10,
        spillover_tpm=1000, spillover_rpm=10,
    )
    # 1000 from reserved, 500 from spillover.
    assert rsv.from_reserved_tpm == 1000
    assert rsv.from_spill_tpm == 500

    # LLM returned 1000 actual (500 less than estimate). Refund 500.
    await rate_limiter.refund_tokens(rsv, tokens_to_refund=500)

    # Spillover should now be at 0 again — a different customer can claim.
    rsv2 = await rate_limiter.reserve(
        customer_id="other-customer", tokens=500, requests=1,
        customer_reserved_tpm=0, customer_reserved_rpm=0,
        spillover_tpm=1000, spillover_rpm=10,
    )
    assert rsv2.source == "spillover"


async def test_full_refund_returns_all_tokens(live_redis):
    cid = str(uuid4())
    rsv = await rate_limiter.reserve(
        customer_id=cid, tokens=900, requests=1,
        customer_reserved_tpm=1000, customer_reserved_rpm=5,
        spillover_tpm=500, spillover_rpm=5,
    )
    await rate_limiter.refund(rsv)

    # Full reservation back — can now reserve up to 1000 from reserved.
    rsv2 = await rate_limiter.reserve(
        customer_id=cid, tokens=1000, requests=1,
        customer_reserved_tpm=1000, customer_reserved_rpm=5,
        spillover_tpm=500, spillover_rpm=5,
    )
    assert rsv2.from_reserved_tpm == 1000
    assert rsv2.from_spill_tpm == 0


# ── Provider 429 absorption ────────────────────────────────────────────────

async def test_provider_429_backoff_shrinks_effective_quota(live_redis):
    """A multiplicative-decrease backoff is applied when the provider
    returns 429. The customer's effective reserved TPM is reduced by the
    factor for the next 60 seconds."""
    cid = str(uuid4())
    await rate_limiter.apply_provider_429_backoff(cid, factor=0.5)

    # Effective quota = 1000 * 0.5 = 500. A 600-token request that would
    # normally fit must fail (with 0 spillover).
    with pytest.raises(RateLimitDeferred):
        await rate_limiter.reserve(
            customer_id=cid, tokens=600, requests=1,
            customer_reserved_tpm=1000, customer_reserved_rpm=10,
            spillover_tpm=0, spillover_rpm=0,
        )

    # A 400-token request still fits within the reduced effective quota.
    rsv = await rate_limiter.reserve(
        customer_id=cid, tokens=400, requests=1,
        customer_reserved_tpm=1000, customer_reserved_rpm=10,
        spillover_tpm=0, spillover_rpm=0,
    )
    assert rsv.from_reserved_tpm == 400


# ── Estimate function ────────────────────────────────────────────────────

def test_estimate_tokens_for_short_transcript():
    short = "agent: hi\ncustomer: bye"
    assert estimate_tokens_for_transcript(short) >= 500


def test_estimate_tokens_for_long_transcript():
    long_text = "x" * 4000  # 4000 chars / 4 = 1000 + 500 overhead = 1500
    assert estimate_tokens_for_transcript(long_text) == 1500


def test_estimate_empty_transcript_uses_overhead_floor():
    assert estimate_tokens_for_transcript("") == 500


# ── defer_job worker contract ─────────────────────────────────────────────

async def test_defer_job_returns_to_pending_without_consuming_attempts(live_db):
    """A rate-limit deferral is a soft retry. attempts is decremented to
    undo the lease's auto-increment; defer_count is the bound that
    eventually moves a permanently-constrained job to dead."""
    interaction_id = uuid4()
    job_ids = await job_store.create_pipeline(
        live_db,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload={"transcript_text": "x"},
        has_long_transcript=True,
    )
    await live_db.commit()

    leased = await job_store.lease_job(
        live_db, job_id=job_ids["analysis"],
        worker_id="worker", lease_seconds=60,
    )
    await live_db.commit()
    assert leased["attempts"] == 1

    new_state = await job_store.defer_job(
        live_db,
        job_id=job_ids["analysis"],
        lease_token=leased["lease_token"],
        retry_in_seconds=5,
        reason="tpm_exhausted",
    )
    await live_db.commit()
    assert new_state == "pending"

    row = await live_db.execute(
        text(
            "SELECT attempts, defer_count, state, last_error "
            "FROM processing_jobs WHERE id = :id"
        ),
        {"id": str(job_ids["analysis"])},
    )
    record = row.first()
    assert record.attempts == 0  # decremented to undo lease auto-increment
    assert record.defer_count == 1
    assert record.state == "pending"
    assert "tpm_exhausted" in record.last_error


async def test_defer_job_marks_dead_after_max_defers(live_db):
    """Permanently-constrained jobs (e.g., a customer with reserved=0 and
    no spillover) must not spin forever. defer_count is bounded by
    max_defers; the job goes dead once exceeded."""
    interaction_id = uuid4()
    job_ids = await job_store.create_pipeline(
        live_db,
        interaction_id=interaction_id,
        customer_id=uuid4(),
        campaign_id=uuid4(),
        payload={"transcript_text": "x"},
        has_long_transcript=True,
    )
    await live_db.commit()

    # Tighten max_defers so the test exhausts quickly.
    await live_db.execute(
        text(
            "UPDATE processing_jobs SET max_defers = 2 "
            "WHERE id = :id"
        ),
        {"id": str(job_ids["analysis"])},
    )
    await live_db.commit()

    final_state = None
    for _ in range(3):
        leased = await job_store.lease_job(
            live_db, job_id=job_ids["analysis"],
            worker_id="w", lease_seconds=60,
        )
        if leased is None:
            break
        await live_db.commit()
        final_state = await job_store.defer_job(
            live_db,
            job_id=job_ids["analysis"],
            lease_token=leased["lease_token"],
            retry_in_seconds=0,
            reason="tpm_exhausted",
        )
        await live_db.commit()

    assert final_state == "dead"


# ── token_usage ledger ────────────────────────────────────────────────────

async def test_token_usage_ledger_records_actual_amount(live_db):
    """The provider's reported tokens_actual is captured exactly, alongside
    the estimate and the source label so per-customer billing reconciles."""
    interaction_id = uuid4()
    customer_id = uuid4()
    campaign_id = uuid4()
    job_id = uuid4()

    await budget_svc.record_token_usage(
        live_db,
        interaction_id=interaction_id,
        customer_id=customer_id,
        campaign_id=campaign_id,
        job_id=job_id,
        tokens_estimated=1000,
        tokens_actual=1234,
        source="reserved",
    )
    await live_db.commit()

    row = await live_db.execute(
        text(
            "SELECT tokens_estimated, tokens_actual, source "
            "FROM token_usage WHERE interaction_id = :i"
        ),
        {"i": str(interaction_id)},
    )
    record = row.first()
    assert record.tokens_estimated == 1000
    assert record.tokens_actual == 1234
    assert record.source == "reserved"


async def test_get_customer_budget_returns_default_for_unbudgeted(live_db):
    """Unbudgeted customers get reserved=0 — they can still run, but every
    reservation comes from spillover."""
    new_customer = uuid4()
    budget = await budget_svc.get_customer_budget(live_db, new_customer)
    assert budget.reserved_tpm == 0
    assert budget.reserved_rpm == 0


async def test_get_spillover_capacity_reflects_seed_data(live_db):
    """The schema seeds two customer_budgets rows totalling 45000 TPM; with
    the default LLM_TOKENS_PER_MINUTE=90000, spillover should be 45000."""
    spill_tpm, spill_rpm = await budget_svc.get_spillover_capacity(live_db)
    assert spill_tpm == 90000 - 45000
    assert spill_rpm == 500 - 225  # default RPM (500) - 150 - 75 = 275
