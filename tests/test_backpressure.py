"""
Phase 5 tests — continuous backpressure.

Validates AC7 (no hardcoded freeze; backpressure proportional):
  - dialler_dispatch_probability is a smooth, monotonically-decreasing
    curve through the 0%, 50%, 70%, 90%, 100% utilisation points.
  - current_utilization() reads the rate-limiter's aggregate global
    counters that the Phase 2 reservation Lua script writes alongside
    per-customer / spillover counters.
  - The /metrics endpoint emits Prometheus exposition text containing
    llm_utilization, dialler_dispatch_probability, and the per-customer
    queue-depth gauge with bounded cardinality.

Live Redis is required for the utilisation-from-counters assertions; the
metrics-endpoint assertions only need the app to import cleanly and a
working Redis (since utilisation reads through it).
"""

from uuid import uuid4

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.app import app
from src.services import backpressure, rate_limiter
from src.utils.redis_client import redis_client


# ── Curve shape ────────────────────────────────────────────────────────────

def test_dispatch_probability_at_zero_utilization_is_near_one():
    p = backpressure.dialler_dispatch_probability(0.0)
    assert 0.99 < p <= 1.0


def test_dispatch_probability_at_inflection_is_half():
    # Curve centre at 0.7 utilisation → exactly 0.5.
    p = backpressure.dialler_dispatch_probability(0.7)
    assert abs(p - 0.5) < 1e-6


def test_dispatch_probability_low_util_full_speed():
    # Below 50% utilisation we want the dialler running essentially full speed.
    assert backpressure.dialler_dispatch_probability(0.5) > 0.85


def test_dispatch_probability_high_util_aggressively_sheds():
    # 90% utilisation: dispatch is heavily throttled.
    p = backpressure.dialler_dispatch_probability(0.9)
    assert p < 0.20


def test_dispatch_probability_at_full_capacity_still_nonzero():
    # Soft asymptote: 100% util keeps a small trickle so a brief
    # overshoot can still allow the system to recover, never hard-stops.
    p = backpressure.dialler_dispatch_probability(1.0)
    assert 0.0 < p < 0.10


def test_dispatch_probability_is_monotone_decreasing():
    samples = [0.0, 0.25, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    probs = [backpressure.dialler_dispatch_probability(u) for u in samples]
    assert all(a >= b for a, b in zip(probs, probs[1:]))


def test_dispatch_probability_clamps_negative_input():
    # Defensive: a negative ratio (shouldn't happen) doesn't blow up.
    p = backpressure.dialler_dispatch_probability(-0.5)
    assert 0.0 <= p <= 1.0


# ── Utilisation reads Redis ────────────────────────────────────────────────

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


async def test_current_utilization_zero_when_no_reservations(live_redis):
    util = await backpressure.current_utilization()
    assert util == 0.0


async def test_reservation_increments_global_counter_visible_to_backpressure(live_redis):
    """
    AC7 — backpressure reads the live rate-limit counters. After a
    reservation, the global TPM counter reflects it and current_utilization
    rises proportionally.
    """
    cid = str(uuid4())
    # Reserve 9000 tokens against a 90,000 global cap → 10% utilisation.
    await rate_limiter.reserve(
        customer_id=cid,
        tokens=9000,
        requests=1,
        customer_reserved_tpm=10_000,
        customer_reserved_rpm=50,
        spillover_tpm=80_000,
        spillover_rpm=450,
    )

    used_tpm, used_rpm = await rate_limiter.get_global_usage()
    assert used_tpm == 9000
    assert used_rpm == 1

    util = await backpressure.current_utilization()
    # Default cap is 90,000 TPM → 9000/90000 = 0.10. RPM ratio is far smaller.
    assert abs(util - 0.10) < 0.001


async def test_refund_lowers_utilization(live_redis):
    """A full refund returns the global counter to zero so the dialler
    sees capacity recover immediately rather than waiting on the
    minute roll-over."""
    cid = str(uuid4())
    rsv = await rate_limiter.reserve(
        customer_id=cid, tokens=4500, requests=1,
        customer_reserved_tpm=10_000, customer_reserved_rpm=50,
        spillover_tpm=80_000, spillover_rpm=450,
    )
    util_before = await backpressure.current_utilization()
    assert util_before > 0.0

    await rate_limiter.refund(rsv)
    util_after = await backpressure.current_utilization()
    assert util_after == 0.0


async def test_charge_extra_raises_global_counter(live_redis):
    """True-up overshoot is reflected in the global counter so
    backpressure sees actual consumption, not just the estimate."""
    cid = str(uuid4())
    rsv = await rate_limiter.reserve(
        customer_id=cid, tokens=1000, requests=1,
        customer_reserved_tpm=10_000, customer_reserved_rpm=50,
        spillover_tpm=80_000, spillover_rpm=450,
    )
    used_before, _ = await rate_limiter.get_global_usage()
    assert used_before == 1000

    await rate_limiter.charge_extra(rsv, extra_tokens=500)
    used_after, _ = await rate_limiter.get_global_usage()
    assert used_after == 1500


async def test_dispatch_probability_drops_as_reservations_accumulate(live_redis):
    """End-to-end: as reservations consume the bucket, the dialler's
    dispatch probability falls. No threshold; smooth descent."""
    cid = str(uuid4())
    p0 = backpressure.dialler_dispatch_probability(
        await backpressure.current_utilization()
    )

    # Reserve ~70% of the default 90k cap to hit the curve's inflection.
    await rate_limiter.reserve(
        customer_id=cid, tokens=63_000, requests=1,
        customer_reserved_tpm=20_000, customer_reserved_rpm=50,
        spillover_tpm=70_000, spillover_rpm=450,
    )

    util = await backpressure.current_utilization()
    assert 0.65 < util < 0.75
    p1 = backpressure.dialler_dispatch_probability(util)
    assert p1 < p0
    # Inflection range — well below the always-on regime.
    assert p1 < 0.6


# ── /metrics endpoint shape ────────────────────────────────────────────────

@pytest_asyncio.fixture
async def asgi_client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_metrics_endpoint_returns_prometheus_text(live_redis, asgi_client):
    resp = await asgi_client.get("/metrics")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    body = resp.text

    # Exposition format basics: HELP, TYPE, samples.
    assert "# HELP llm_utilization" in body
    assert "# TYPE llm_utilization gauge" in body
    assert "\nllm_utilization " in body

    assert "# HELP dialler_dispatch_probability" in body
    assert "\ndialler_dispatch_probability " in body

    assert "# HELP llm_tokens_used_minute" in body
    assert "# HELP llm_requests_used_minute" in body
    assert "# HELP llm_tokens_per_minute_cap" in body


async def test_metrics_endpoint_reflects_live_utilisation(live_redis, asgi_client):
    """After a reservation, the scraped llm_utilization line carries a
    non-zero value matching what backpressure.current_utilization reads."""
    cid = str(uuid4())
    await rate_limiter.reserve(
        customer_id=cid, tokens=18_000, requests=1,
        customer_reserved_tpm=20_000, customer_reserved_rpm=50,
        spillover_tpm=70_000, spillover_rpm=450,
    )

    resp = await asgi_client.get("/metrics")
    body = resp.text
    util_line = next(
        line for line in body.splitlines()
        if line.startswith("llm_utilization ")
    )
    util_val = float(util_line.split()[1])
    # 18,000 / 90,000 cap = 0.20 expected.
    assert 0.18 < util_val < 0.22


async def test_metrics_endpoint_postcall_jobs_gauge_present(live_redis, asgi_client):
    """The postcall_jobs gauge MUST be declared (HELP / TYPE lines) even
    if no rows exist. Missing declarations break Prometheus scrape parsing.
    """
    resp = await asgi_client.get("/metrics")
    body = resp.text
    assert "# HELP postcall_jobs" in body
    assert "# TYPE postcall_jobs gauge" in body
