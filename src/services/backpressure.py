"""
Continuous backpressure — replaces the binary 1800s circuit-breaker freeze.

Why this exists:
  The dialler dispatches calls. Post-call processing consumes LLM tokens.
  When LLM utilisation climbs, naive systems either keep dialling at full
  speed (and then have post-call queues explode + 429s + customer-visible
  failures) or trip a binary breaker that shuts dialling down completely
  for half an hour. Both are bad. The right answer is: dial *fewer* calls
  as utilisation rises, smoothly.

Design:
  - `current_utilization()` reads the rate limiter's global aggregate
    counters (Phase 2's reservation Lua script writes them atomically on
    every reservation, refund, and true-up). One Redis pipeline call.
    Returns max(tpm_ratio, rpm_ratio) in [0, 1].
  - `dialler_dispatch_probability(utilization)` returns a probability in
    [0, 1] for the dialler to use as a multiplier on its dispatch rate.
    Curve: `1 − sigmoid((u − 0.7) × 10)`. Effectively:
        u ≤ 0.5  → ≥ 0.88   (full speed)
        u = 0.7  → 0.50     (mid-ramp; the inflection point)
        u = 0.9  → ~0.12    (heavy shedding)
        u ≥ 1.0  → ~0.05    (asymptote — never quite zero, so a
                              temporary overshoot still allows occasional
                              calls through, helping the system recover
                              instead of stalling)
    No hard threshold. The signal is continuous, so a slow rise produces
    a slow shed; a quick spike produces a sharp shed.

How the dialler uses this:
  Read `current_dispatch_probability()` once per dispatch decision and
  draw a [0,1] uniform. If draw < prob: dial. Else: skip. The expected
  dispatch rate is therefore `prob × baseline_rate`. The dialler keeps
  full local autonomy — backpressure is advice, not a command.

Why no breaker state, freeze duration, or trip counters:
  The system has no "open / half-open / closed" state. Utilisation is
  read fresh on every dispatch decision. As soon as the LLM bucket
  drains (workers finish, the minute rolls over), utilisation drops and
  the dispatch probability climbs back up automatically. There is no
  cooldown to misjudge.

Alert thresholds (consumed by the /metrics endpoint and external
alerting; this module emits the raw float, not the alerts):
    utilization >= 0.75  → warn  (capacity is tight)
    utilization >= 0.90  → page  (sustained 90%+ likely correlates with
                                  defer-spike on rate-limited jobs)
    queue depth growth-rate > 1000/min → page (backlog growing faster
                                                than workers drain it)
    reaped-stale-leases > 100/hour → warn (worker churn — investigate)
"""

from __future__ import annotations

import math

from src.config import settings
from src.services import rate_limiter


# Curve constants. Centred at 0.7 utilisation; slope 10 produces a
# half-power band of roughly 0.55 → 0.85 utilisation, with the soft
# asymptote at the top so a brief 100% blip still yields ~5% dispatch.
_INFLECTION = 0.7
_SLOPE = 10.0


def _sigmoid(x: float) -> float:
    # math.exp can overflow for very negative x; clamp to keep stable.
    if x < -50:
        return 0.0
    if x > 50:
        return 1.0
    return 1.0 / (1.0 + math.exp(-x))


def dialler_dispatch_probability(utilization: float) -> float:
    """
    Smooth shed curve. Pure function; safe to call on every dispatch
    decision without touching Redis (read utilisation once per tick,
    apply this curve as needed).

    >>> round(dialler_dispatch_probability(0.0), 3)
    0.999
    >>> round(dialler_dispatch_probability(0.7), 3)
    0.5
    >>> round(dialler_dispatch_probability(0.9), 3) < 0.2
    True
    """
    u = max(0.0, min(1.5, float(utilization)))
    return 1.0 - _sigmoid((u - _INFLECTION) * _SLOPE)


async def current_utilization() -> float:
    """
    Read max(tpm, rpm) ratio from the rate limiter's global counters.
    Returns a float in [0, 1]; values >1 are possible briefly during
    true-up overshoots and are returned as-is so callers can detect
    "we exceeded nominal" rather than silently capping.
    """
    return await rate_limiter.get_utilization(
        settings.LLM_TOKENS_PER_MINUTE,
        settings.LLM_REQUESTS_PER_MINUTE,
    )


async def current_dispatch_probability() -> float:
    """Convenience: utilisation → dispatch probability in one call."""
    return dialler_dispatch_probability(await current_utilization())
