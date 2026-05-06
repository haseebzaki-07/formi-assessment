"""
LLM rate limiter — Redis-backed atomic token bucket with two-tier accounting
(per-customer reserved + global spillover).

Why this design:
  AC2 ("Customer A's budget does not consume Customer B's allocation") is
  not just a soft preference — it's a structural property of where the
  counters live. Each customer's reserved share lives in its own counter
  key; spillover is a single shared key. A customer at reserved-limit can
  draw from spillover only, and the spillover key is bounded at
  (global_limit − sum_reserved). So even an extreme burst from Customer A
  can only consume A's reserved share + spillover, never B's reserved.

Why Lua:
  reserve() must check both TPM and RPM bounds across both reserved and
  spillover keys, then commit four INCRBYs, all atomically. Doing this in
  a Python pipeline with WATCH/MULTI is racy under concurrency. A Lua
  script runs as a single atomic Redis op — no other client interleaves.

Window:
  60-second minute bucket keyed by `epoch_minute`. TTL is 65s (5s slack so
  reads near the boundary still see the trailing window). The bucket
  auto-rolls over each minute — capacity recovers naturally without an
  explicit reset. A worker crash mid-LLM leaks at most one minute of
  capacity for the reserved tokens; the next minute starts fresh.

Provider 429 handling:
  If the provider STILL returns 429 despite our reservation (estimate was
  wrong, or we share a key with another instance), we refund the
  reservation and apply a multiplicative decrease on this customer's
  effective TPM for the next 60s. Caller catches RateLimitDeferred and
  defers the job. The 429 is never propagated to the API caller (AC1).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)


# Keys are namespaced rl:* and bucketed by epoch_minute so they auto-roll.
_KEY_CUST_TPM = "rl:tpm:rsv:{cid}:{minute}"
_KEY_CUST_RPM = "rl:rpm:rsv:{cid}:{minute}"
_KEY_SPILL_TPM = "rl:tpm:spill:{minute}"
_KEY_SPILL_RPM = "rl:rpm:spill:{minute}"
# Phase 5: aggregate global counters across reserved + spillover. Written
# atomically alongside the per-customer / spillover counters so backpressure
# can read a single number without iterating over every customer key.
_KEY_GLOBAL_TPM = "rl:tpm:global:{minute}"
_KEY_GLOBAL_RPM = "rl:rpm:global:{minute}"
_KEY_BACKOFF = "rl:backoff:{cid}"  # Multiplicative-decrease flag on 429
_TTL_SECONDS = 65


class RateLimitDeferred(Exception):
    """Reservation rejected — caller should defer the job, not fail it.

    retry_after_seconds is the suggested time before re-attempting. reason
    is a short string (`tpm_exhausted`, `rpm_exhausted`, `provider_429`)
    used in audit_events.details.
    """

    def __init__(self, retry_after_seconds: int, reason: str):
        super().__init__(f"{reason}; retry after {retry_after_seconds}s")
        self.retry_after_seconds = retry_after_seconds
        self.reason = reason


@dataclass
class Reservation:
    """Records what was consumed from where, so refund() can undo it precisely."""

    customer_id: str
    minute_epoch: int
    tokens: int
    requests: int
    from_reserved_tpm: int
    from_spill_tpm: int
    from_reserved_rpm: int
    from_spill_rpm: int
    source: str  # 'reserved' | 'spillover' | 'mixed'


# Returns: {granted (0|1), source_or_reason, available_or_total,
#           from_rsv_tpm, from_spill_tpm, from_rsv_rpm, from_spill_rpm}
#
# KEYS layout (Phase 5 added the two global keys):
#   1 = customer-reserved TPM
#   2 = customer-reserved RPM
#   3 = spillover TPM
#   4 = spillover RPM
#   5 = global TPM   (Phase 5 — aggregate across reserved+spillover)
#   6 = global RPM   (Phase 5)
_RESERVE_LUA = """
local tokens = tonumber(ARGV[1])
local requests = tonumber(ARGV[2])
local cust_tpm_limit = tonumber(ARGV[3])
local cust_rpm_limit = tonumber(ARGV[4])
local spill_tpm_limit = tonumber(ARGV[5])
local spill_rpm_limit = tonumber(ARGV[6])
local ttl = tonumber(ARGV[7])

local cust_tpm_used = tonumber(redis.call('GET', KEYS[1]) or '0')
local cust_rpm_used = tonumber(redis.call('GET', KEYS[2]) or '0')
local spill_tpm_used = tonumber(redis.call('GET', KEYS[3]) or '0')
local spill_rpm_used = tonumber(redis.call('GET', KEYS[4]) or '0')

local from_rsv_tpm = math.min(tokens, math.max(0, cust_tpm_limit - cust_tpm_used))
local from_spill_tpm = tokens - from_rsv_tpm
local from_rsv_rpm = math.min(requests, math.max(0, cust_rpm_limit - cust_rpm_used))
local from_spill_rpm = requests - from_rsv_rpm

if from_spill_tpm > spill_tpm_limit - spill_tpm_used then
    local avail = (cust_tpm_limit - cust_tpm_used) + (spill_tpm_limit - spill_tpm_used)
    return {0, 'tpm_exhausted', math.max(0, avail), 0, 0, 0, 0}
end
if from_spill_rpm > spill_rpm_limit - spill_rpm_used then
    local avail = (cust_rpm_limit - cust_rpm_used) + (spill_rpm_limit - spill_rpm_used)
    return {0, 'rpm_exhausted', math.max(0, avail), 0, 0, 0, 0}
end

if from_rsv_tpm > 0 then
    redis.call('INCRBY', KEYS[1], from_rsv_tpm)
    redis.call('EXPIRE', KEYS[1], ttl)
end
if from_rsv_rpm > 0 then
    redis.call('INCRBY', KEYS[2], from_rsv_rpm)
    redis.call('EXPIRE', KEYS[2], ttl)
end
if from_spill_tpm > 0 then
    redis.call('INCRBY', KEYS[3], from_spill_tpm)
    redis.call('EXPIRE', KEYS[3], ttl)
end
if from_spill_rpm > 0 then
    redis.call('INCRBY', KEYS[4], from_spill_rpm)
    redis.call('EXPIRE', KEYS[4], ttl)
end

-- Phase 5 global aggregate: a single counter for the whole pool. Reading
-- this is O(1) for backpressure regardless of customer count.
if tokens > 0 then
    redis.call('INCRBY', KEYS[5], tokens)
    redis.call('EXPIRE', KEYS[5], ttl)
end
if requests > 0 then
    redis.call('INCRBY', KEYS[6], requests)
    redis.call('EXPIRE', KEYS[6], ttl)
end

-- Source is decided by where the TOKENS came from (the headline resource
-- for billing). RPM is enforced but doesn't change the source tag.
local source
if from_spill_tpm == 0 then
    source = 'reserved'
elseif from_rsv_tpm == 0 then
    source = 'spillover'
else
    source = 'mixed'
end

return {1, source, tokens, from_rsv_tpm, from_spill_tpm, from_rsv_rpm, from_spill_rpm}
"""


def _minute_epoch() -> int:
    return int(time.time() // 60)


async def reserve(
    *,
    customer_id: str,
    tokens: int,
    requests: int = 1,
    customer_reserved_tpm: int,
    customer_reserved_rpm: int,
    spillover_tpm: int,
    spillover_rpm: int,
) -> Reservation:
    """
    Atomic reserve. Raises RateLimitDeferred on rejection — the caller is
    expected to defer the job and re-dispatch later (see job_store.defer_job).

    Pre-conditions: caller has resolved the customer's reserved TPM/RPM via
    src.services.budget.get_customer_budget and the spillover capacity via
    get_spillover_capacity.
    """
    # If the customer is in multiplicative-decrease backoff (set after a
    # provider 429), shrink their effective reserved by the backoff factor.
    backoff = await redis_client.get(_KEY_BACKOFF.format(cid=customer_id))
    effective_cust_tpm = customer_reserved_tpm
    if backoff:
        try:
            factor = float(backoff)
            effective_cust_tpm = int(customer_reserved_tpm * factor)
        except (TypeError, ValueError):
            pass

    minute = _minute_epoch()
    keys = [
        _KEY_CUST_TPM.format(cid=customer_id, minute=minute),
        _KEY_CUST_RPM.format(cid=customer_id, minute=minute),
        _KEY_SPILL_TPM.format(minute=minute),
        _KEY_SPILL_RPM.format(minute=minute),
        _KEY_GLOBAL_TPM.format(minute=minute),
        _KEY_GLOBAL_RPM.format(minute=minute),
    ]
    args = [
        tokens,
        requests,
        effective_cust_tpm,
        customer_reserved_rpm,
        spillover_tpm,
        spillover_rpm,
        _TTL_SECONDS,
    ]

    result = await redis_client.eval(_RESERVE_LUA, len(keys), *keys, *args)
    granted = int(result[0])
    second = result[1]
    if isinstance(second, bytes):
        second = second.decode()

    if granted == 0:
        # Suggest retry near the next minute boundary, plus jitter; floor 1s.
        secs_to_next_minute = 60 - int(time.time()) % 60
        retry_after = max(1, secs_to_next_minute)
        raise RateLimitDeferred(retry_after_seconds=retry_after, reason=second)

    return Reservation(
        customer_id=customer_id,
        minute_epoch=minute,
        tokens=tokens,
        requests=requests,
        source=second,
        from_reserved_tpm=int(result[3]),
        from_spill_tpm=int(result[4]),
        from_reserved_rpm=int(result[5]),
        from_spill_rpm=int(result[6]),
    )


async def refund(reservation: Reservation) -> None:
    """
    Return the entire reservation to the bucket. Used when the LLM call
    failed before consuming tokens (e.g., provider returned 429 despite
    our prior reservation, or the worker is bailing out cleanly).
    """
    minute = reservation.minute_epoch
    keys_decrements = [
        (_KEY_CUST_TPM.format(cid=reservation.customer_id, minute=minute),
         reservation.from_reserved_tpm),
        (_KEY_CUST_RPM.format(cid=reservation.customer_id, minute=minute),
         reservation.from_reserved_rpm),
        (_KEY_SPILL_TPM.format(minute=minute),
         reservation.from_spill_tpm),
        (_KEY_SPILL_RPM.format(minute=minute),
         reservation.from_spill_rpm),
        # Phase 5 — keep the aggregate global counter in sync so backpressure
        # reads a true picture of capacity even after refunds.
        (_KEY_GLOBAL_TPM.format(minute=minute), reservation.tokens),
        (_KEY_GLOBAL_RPM.format(minute=minute), reservation.requests),
    ]
    pipe = redis_client.pipeline()
    for key, amount in keys_decrements:
        if amount > 0:
            pipe.decrby(key, amount)
    await pipe.execute()


async def refund_tokens(reservation: Reservation, tokens_to_refund: int) -> None:
    """
    Refund a partial token amount — used at true-up time when the LLM's
    actual usage came in below our estimate. Pulls from spillover first
    (so the customer's reserved counter stays accurate to "tokens
    actually committed for this customer's work"), falling back to
    reserved for any remainder.
    """
    if tokens_to_refund <= 0:
        return
    from_spill = min(tokens_to_refund, reservation.from_spill_tpm)
    from_reserved = min(
        tokens_to_refund - from_spill, reservation.from_reserved_tpm,
    )
    minute = reservation.minute_epoch

    pipe = redis_client.pipeline()
    if from_spill > 0:
        pipe.decrby(_KEY_SPILL_TPM.format(minute=minute), from_spill)
    if from_reserved > 0:
        pipe.decrby(
            _KEY_CUST_TPM.format(cid=reservation.customer_id, minute=minute),
            from_reserved,
        )
    # Phase 5 — global counter mirrors the over-estimate refund.
    total_refund = from_spill + from_reserved
    if total_refund > 0:
        pipe.decrby(_KEY_GLOBAL_TPM.format(minute=minute), total_refund)
    await pipe.execute()


async def charge_extra(reservation: Reservation, extra_tokens: int) -> None:
    """
    True-up when the LLM's actual usage exceeded the estimate. Charges go
    to the same source the reservation came from. May briefly exceed the
    nominal capacity by the actual-vs-estimate variance (typically <10%);
    the work is already done, so this is accounting, not gating.
    """
    if extra_tokens <= 0:
        return
    minute = reservation.minute_epoch
    if reservation.source in ("reserved", "mixed"):
        await redis_client.incrby(
            _KEY_CUST_TPM.format(cid=reservation.customer_id, minute=minute),
            extra_tokens,
        )
        await redis_client.expire(
            _KEY_CUST_TPM.format(cid=reservation.customer_id, minute=minute),
            _TTL_SECONDS,
        )
    else:
        await redis_client.incrby(_KEY_SPILL_TPM.format(minute=minute), extra_tokens)
        await redis_client.expire(_KEY_SPILL_TPM.format(minute=minute), _TTL_SECONDS)
    # Phase 5 — true-up the global counter so utilisation reflects actual
    # consumption, not just the original estimate.
    await redis_client.incrby(_KEY_GLOBAL_TPM.format(minute=minute), extra_tokens)
    await redis_client.expire(_KEY_GLOBAL_TPM.format(minute=minute), _TTL_SECONDS)


async def apply_provider_429_backoff(customer_id: str, factor: float = 0.8) -> None:
    """
    Multiplicative decrease on this customer's effective reserved TPM for
    the next 60 seconds. Used when the provider returned 429 despite our
    reservation — our estimate of capacity was wrong and we need to slow
    down before retrying.
    """
    await redis_client.set(
        _KEY_BACKOFF.format(cid=customer_id),
        str(factor),
        ex=60,
    )
    logger.warning(
        "rate_limit_provider_429_backoff",
        extra={"customer_id": customer_id, "factor": factor},
    )


async def get_global_usage() -> tuple[int, int]:
    """
    Phase 5 — returns (tokens_used, requests_used) across the entire pool
    (reserved + spillover) for the current minute window. Single O(1)
    Redis read pair; no customer-key iteration. Source for backpressure's
    continuous-capacity signal.
    """
    minute = _minute_epoch()
    pipe = redis_client.pipeline()
    pipe.get(_KEY_GLOBAL_TPM.format(minute=minute))
    pipe.get(_KEY_GLOBAL_RPM.format(minute=minute))
    tpm_raw, rpm_raw = await pipe.execute()
    return int(tpm_raw or 0), int(rpm_raw or 0)


async def get_utilization(global_tpm: int, global_rpm: int) -> float:
    """
    Returns max(tpm_used / tpm_cap, rpm_used / rpm_cap) across the current
    minute window. Phase 5 reads the dedicated global aggregate counters
    that the Lua reserve script maintains alongside the per-customer and
    spillover keys, so this is O(1) regardless of customer count.
    """
    used_tpm, used_rpm = await get_global_usage()
    tpm_ratio = (used_tpm / global_tpm) if global_tpm > 0 else 0.0
    rpm_ratio = (used_rpm / global_rpm) if global_rpm > 0 else 0.0
    return max(tpm_ratio, rpm_ratio)


def estimate_tokens_for_transcript(transcript_text: str, prompt_overhead: int = 500) -> int:
    """
    Heuristic estimate of LLM tokens for a transcript-analysis call.
    Used at reserve-time before the actual LLM call returns the truth.
    chars/4 is a standard rule-of-thumb for English/Hinglish in modern BPE
    tokenisers; +500 covers system prompt, JSON wrapping, response.
    """
    if not transcript_text:
        return prompt_overhead
    return max(prompt_overhead, len(transcript_text) // 4 + prompt_overhead)
