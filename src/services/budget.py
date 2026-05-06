"""
Per-customer budget queries and the token_usage ledger.

This module bridges Postgres (where customer entitlements and the billing
ledger live) and the rate limiter (which operates on Redis counters keyed
by customer + minute). The rate limiter is intentionally agnostic to where
its limits come from — this module supplies them.

Caching note: get_spillover_capacity is called once per LLM reservation,
which at burst is thousands of times per minute. The underlying SQL is
SELECT SUM(reserved_tpm) FROM customer_budgets — cheap but not free. A
30-second in-process TTL cache is enough; customer_budgets changes
infrequently and a 30-second staleness is well within tolerance.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Optional, Tuple, Union
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings


@dataclass
class CustomerBudget:
    customer_id: str
    reserved_tpm: int
    reserved_rpm: int
    priority_weight: int
    burst_multiplier: float
    customer_overrides: dict


# Default for customers without an explicit row. They run on spillover only.
_DEFAULT_BUDGET = CustomerBudget(
    customer_id="",
    reserved_tpm=0,
    reserved_rpm=0,
    priority_weight=1,
    burst_multiplier=1.5,
    customer_overrides={},
)


async def get_customer_budget(
    session: AsyncSession, customer_id: Union[str, UUID],
) -> CustomerBudget:
    """
    Read the customer's budget row. Returns a default budget (zero reserved,
    spillover-only) if no row exists — unbudgeted customers can still
    process work, they just don't get a quota guarantee.
    """
    result = await session.execute(
        text("""
            SELECT reserved_tpm, reserved_rpm, priority_weight,
                   burst_multiplier, customer_overrides
            FROM customer_budgets
            WHERE customer_id = :id
        """),
        {"id": str(customer_id)},
    )
    row = result.first()
    if row is None:
        return CustomerBudget(
            customer_id=str(customer_id),
            reserved_tpm=_DEFAULT_BUDGET.reserved_tpm,
            reserved_rpm=_DEFAULT_BUDGET.reserved_rpm,
            priority_weight=_DEFAULT_BUDGET.priority_weight,
            burst_multiplier=_DEFAULT_BUDGET.burst_multiplier,
            customer_overrides={},
        )
    return CustomerBudget(
        customer_id=str(customer_id),
        reserved_tpm=int(row.reserved_tpm),
        reserved_rpm=int(row.reserved_rpm),
        priority_weight=int(row.priority_weight),
        burst_multiplier=float(row.burst_multiplier),
        customer_overrides=row.customer_overrides or {},
    )


# 30s in-process cache for the spillover total. Read on every reservation
# at burst, so worth caching; written rarely, so 30s staleness is fine.
_spillover_cache: dict = {"value": None, "expires_at": 0.0}
_SPILLOVER_TTL_SECONDS = 30


async def get_spillover_capacity(
    session: AsyncSession,
    *,
    global_tpm: Optional[int] = None,
    global_rpm: Optional[int] = None,
) -> Tuple[int, int]:
    """
    Returns (spillover_tpm, spillover_rpm) — the unallocated headroom
    available to customers after their own reserved share is exhausted.
    spillover = global − sum(per-customer reserved).

    global_tpm / global_rpm default to settings.LLM_TOKENS_PER_MINUTE /
    settings.LLM_REQUESTS_PER_MINUTE so callers don't have to thread them.
    """
    now = time.monotonic()
    if (
        _spillover_cache["value"] is not None
        and now < _spillover_cache["expires_at"]
    ):
        sum_tpm, sum_rpm = _spillover_cache["value"]
    else:
        result = await session.execute(
            text("""
                SELECT
                    COALESCE(SUM(reserved_tpm), 0) AS sum_tpm,
                    COALESCE(SUM(reserved_rpm), 0) AS sum_rpm
                FROM customer_budgets
            """),
        )
        row = result.first()
        sum_tpm = int(row.sum_tpm) if row else 0
        sum_rpm = int(row.sum_rpm) if row else 0
        _spillover_cache["value"] = (sum_tpm, sum_rpm)
        _spillover_cache["expires_at"] = now + _SPILLOVER_TTL_SECONDS

    g_tpm = global_tpm if global_tpm is not None else settings.LLM_TOKENS_PER_MINUTE
    g_rpm = global_rpm if global_rpm is not None else settings.LLM_REQUESTS_PER_MINUTE
    return max(0, g_tpm - sum_tpm), max(0, g_rpm - sum_rpm)


def invalidate_spillover_cache() -> None:
    """Call this when a customer_budgets row is inserted/updated/deleted so
    the next get_spillover_capacity hits Postgres. Tests use this between
    setups."""
    _spillover_cache["value"] = None
    _spillover_cache["expires_at"] = 0.0


async def record_token_usage(
    session: AsyncSession,
    *,
    interaction_id: Union[str, UUID],
    customer_id: Union[str, UUID],
    campaign_id: Union[str, UUID],
    job_id: Union[str, UUID],
    tokens_estimated: int,
    tokens_actual: int,
    source: str = "reserved",
    cost_micros: Optional[int] = None,
) -> None:
    """
    Append a row to token_usage. This is the billing-grade source of truth;
    it must capture the provider's reported tokens_actual exactly.
    Caller's transaction.
    """
    await session.execute(
        text("""
            INSERT INTO token_usage
                (interaction_id, customer_id, campaign_id, job_id,
                 tokens_estimated, tokens_actual, cost_micros, source)
            VALUES (:interaction_id, :customer_id, :campaign_id, :job_id,
                    :tokens_estimated, :tokens_actual, :cost_micros, :source)
        """),
        {
            "interaction_id": str(interaction_id),
            "customer_id": str(customer_id),
            "campaign_id": str(campaign_id),
            "job_id": str(job_id),
            "tokens_estimated": int(tokens_estimated),
            "tokens_actual": int(tokens_actual),
            "cost_micros": cost_micros,
            "source": source,
        },
    )
