"""
PostCallProcessor — runs LLM analysis on a completed call transcript.

Phase 2 changes:
  - Every LLM call is gated by a pre-flight rate-limit reservation. The
    estimate is heuristic (chars/4 + overhead from src.services.rate_limiter);
    actual usage from the provider is reconciled against the estimate via
    true-up after the call.
  - Per-customer budgets are loaded from customer_budgets; spillover
    headroom is shared across customers (see src/services/budget.py and
    src/services/rate_limiter.py for the enforcement model).
  - Every call writes a token_usage ledger row keyed by (interaction_id,
    customer_id, campaign_id, job_id). This is the source of truth for
    billing and is queryable for "tokens consumed by Customer X this hour".
  - A reservation rejection raises RateLimitDeferred, caught by the worker
    in src/tasks/celery_tasks.py and turned into a defer_job (soft retry,
    doesn't consume the work-attempt budget).
  - A provider-side 429 (despite our reservation) refunds the reservation,
    applies a per-customer multiplicative backoff, and raises
    RateLimitDeferred. The 429 never propagates to the API caller (AC1).
  - The circuit-breaker counter is no longer touched here. The rate
    limiter's Redis counters are the source of truth for capacity; Phase 5
    deletes circuit_breaker.py entirely.
"""

import json
import logging
from datetime import datetime
from dataclasses import dataclass
from typing import Any, Dict, Optional

from src.config import settings
from src.services import budget as budget_svc
from src.services import rate_limiter
from src.services.rate_limiter import RateLimitDeferred
from src.utils.db import async_session_factory

logger = logging.getLogger(__name__)


class ProviderRateLimitedError(Exception):
    """The LLM provider returned 429 despite our reservation."""

    def __init__(self, retry_after_seconds: int = 60):
        super().__init__(f"provider returned 429 (retry-after {retry_after_seconds}s)")
        self.retry_after_seconds = retry_after_seconds


@dataclass
class PostCallContext:
    interaction_id: str
    session_id: str
    lead_id: str
    campaign_id: str
    customer_id: str
    agent_id: str
    call_sid: str
    transcript_text: str
    conversation_data: dict
    additional_data: dict
    ended_at: datetime
    exotel_account_id: Optional[str] = None


@dataclass
class AnalysisResult:
    call_stage: str
    entities: Dict[str, Any]
    summary: str
    raw_response: Dict[str, Any]
    tokens_used: int
    tokens_estimated: int
    reservation_source: str
    latency_ms: float
    provider: str
    model: str


class PostCallProcessor:

    async def process_post_call(
        self,
        ctx: PostCallContext,
        *,
        job_id: str,
        single_prompt: bool = True,
        lane: Optional[str] = None,
    ) -> AnalysisResult:
        """
        Reserve quota → call LLM → true-up → write usage ledger.

        Raises RateLimitDeferred if the reservation is rejected or the
        provider returns 429 — caller (worker) defers the job rather than
        failing it. Other exceptions (malformed response, network errors)
        propagate as work failures and consume an attempt.

        Phase 3: `lane` selects a prompt variant and a reservation hint.
        Cold-lane work uses a summary-only prompt that emits ~500 tokens
        instead of full entity extraction (~1500). The reservation
        estimate is capped to that hint so the rate limiter doesn't
        over-reserve for cheap work — freeing hot-lane headroom under
        contention.
        """
        # 1. Resolve budget + spillover capacity from Postgres.
        async with async_session_factory() as session:
            customer_budget = await budget_svc.get_customer_budget(
                session, ctx.customer_id,
            )
            spillover_tpm, spillover_rpm = await budget_svc.get_spillover_capacity(
                session,
            )

        # 2. Estimate tokens for this transcript. Heuristic estimate from
        # transcript length; cold lane caps the estimate at the cheaper
        # cold-lane reservation size so the rate limiter doesn't reserve
        # more capacity than the cold prompt will consume.
        raw_estimate = rate_limiter.estimate_tokens_for_transcript(
            ctx.transcript_text,
        )
        tokens_estimated = self._reservation_estimate_for_lane(raw_estimate, lane)

        # 3. Reserve. Raises RateLimitDeferred on rejection — propagates out.
        reservation = await rate_limiter.reserve(
            customer_id=ctx.customer_id,
            tokens=tokens_estimated,
            requests=1,
            customer_reserved_tpm=customer_budget.reserved_tpm,
            customer_reserved_rpm=customer_budget.reserved_rpm,
            spillover_tpm=spillover_tpm,
            spillover_rpm=spillover_rpm,
        )

        # 4. Call the LLM. Refund the reservation on any failure.
        try:
            prompt = self._build_analysis_prompt(
                ctx.transcript_text,
                ctx.additional_data,
                single_prompt,
                lane=lane,
            )
            start = datetime.utcnow()
            response = await self._call_llm(prompt)
            elapsed_ms = (datetime.utcnow() - start).total_seconds() * 1000

        except ProviderRateLimitedError as exc:
            await rate_limiter.refund(reservation)
            await rate_limiter.apply_provider_429_backoff(ctx.customer_id)
            logger.warning(
                "llm_provider_429_absorbed",
                extra={
                    "interaction_id": ctx.interaction_id,
                    "customer_id": ctx.customer_id,
                    "retry_after": exc.retry_after_seconds,
                },
            )
            raise RateLimitDeferred(
                retry_after_seconds=exc.retry_after_seconds,
                reason="provider_429",
            ) from exc

        except Exception:
            await rate_limiter.refund(reservation)
            raise

        # 5. Parse + true-up actual vs estimated.
        result = self._parse_response(
            response, elapsed_ms,
            tokens_estimated=tokens_estimated,
            reservation_source=reservation.source,
        )
        delta = result.tokens_used - tokens_estimated
        if delta > 0:
            await rate_limiter.charge_extra(reservation, delta)
        elif delta < 0:
            await rate_limiter.refund_tokens(reservation, -delta)

        # 6. Append the billing ledger row + update the interaction metadata
        # in a single Postgres transaction so they always agree.
        async with async_session_factory() as session:
            await budget_svc.record_token_usage(
                session,
                interaction_id=ctx.interaction_id,
                customer_id=ctx.customer_id,
                campaign_id=ctx.campaign_id,
                job_id=job_id,
                tokens_estimated=tokens_estimated,
                tokens_actual=result.tokens_used,
                source=reservation.source,
            )
            await self._update_interaction_metadata_in_session(
                session, ctx.interaction_id, result,
            )
            await session.commit()

        logger.info(
            "postcall_analysis_complete",
            extra={
                "interaction_id": ctx.interaction_id,
                "customer_id": ctx.customer_id,
                "campaign_id": ctx.campaign_id,
                "call_stage": result.call_stage,
                "tokens_estimated": tokens_estimated,
                "tokens_actual": result.tokens_used,
                "reservation_source": reservation.source,
                "latency_ms": result.latency_ms,
            },
        )

        return result

    def _build_analysis_prompt(
        self,
        transcript: str,
        additional_data: dict,
        single_prompt: bool,
        *,
        lane: Optional[str] = None,
    ) -> str:
        # Hot lane: full extraction prompt — call_stage + entities + summary.
        # Cold lane: summary-only prompt. Entities aren't acted on for cold
        # outcomes (already_done / not_interested / callback) so paying for
        # the extra token spend is waste at scale.
        if lane == "cold":
            system_prompt = """You are a call analysis assistant. Classify the
call and write a one-sentence summary.

Respond in JSON format:
{
    "call_stage": "...",
    "summary": "..."
}"""
        else:
            system_prompt = """You are a call analysis assistant. Analyze the following
call transcript and extract:
1. call_stage: The outcome/disposition of the call
2. entities: Key information mentioned (dates, times, amounts, names, preferences)
3. summary: A brief summary of what happened in the call

Respond in JSON format:
{
    "call_stage": "...",
    "entities": {...},
    "summary": "..."
}"""

        return (
            f"{system_prompt}\n\n"
            f"Transcript:\n{transcript}\n\n"
            f"Additional context:\n{json.dumps(additional_data)}"
        )

    def _reservation_estimate_for_lane(
        self, raw_estimate: int, lane: Optional[str],
    ) -> int:
        """Bound the reservation estimate by the lane's reservation hint.

        Hot lane uses the heuristic estimate as-is (it's already the size of
        a full analysis). Cold lane caps at COLD_LANE_RESERVATION_TOKENS so
        the rate limiter doesn't burn hot-lane-equivalent headroom on a
        summary-only prompt; the heuristic estimate is rarely smaller than
        the cap anyway because of the prompt-overhead floor.
        """
        if lane == "cold":
            return min(raw_estimate, settings.COLD_LANE_RESERVATION_TOKENS)
        if lane == "hot":
            return min(raw_estimate, settings.HOT_LANE_RESERVATION_TOKENS)
        return raw_estimate

    async def _call_llm(self, prompt: str) -> dict:
        """
        Mock LLM client for the assessment. Real implementation would POST
        to the provider's chat-completions endpoint and raise
        ProviderRateLimitedError on a 429 (with the Retry-After header).
        """
        return {
            "call_stage": "unknown",
            "entities": {},
            "summary": "Mock analysis result",
            "usage": {"total_tokens": 1500},
        }

    def _parse_response(
        self,
        response: dict,
        latency_ms: float,
        *,
        tokens_estimated: int,
        reservation_source: str,
    ) -> AnalysisResult:
        return AnalysisResult(
            call_stage=response.get("call_stage", "unknown"),
            entities=response.get("entities", {}),
            summary=response.get("summary", ""),
            raw_response=response,
            tokens_used=response.get("usage", {}).get("total_tokens", 0),
            tokens_estimated=tokens_estimated,
            reservation_source=reservation_source,
            latency_ms=latency_ms,
            provider=settings.LLM_PROVIDER,
            model=settings.LLM_MODEL,
        )

    async def _update_interaction_metadata_in_session(
        self, session, interaction_id: str, result: AnalysisResult,
    ) -> None:
        """
        Merge the analysis output into interactions.interaction_metadata.
        Caller's transaction so this lands atomically with the token_usage
        ledger row.
        """
        from sqlalchemy import text
        await session.execute(
            text("""
                UPDATE interactions
                SET interaction_metadata = interaction_metadata
                    || cast(:patch as jsonb),
                    updated_at = NOW()
                WHERE id = :id
            """),
            {
                "id": interaction_id,
                "patch": json.dumps({
                    "analysis_status": "complete",
                    "call_stage": result.call_stage,
                    "entities": result.entities,
                    "summary": result.summary,
                    "tokens_used": result.tokens_used,
                    "latency_ms": result.latency_ms,
                }, default=str),
            },
        )


post_call_processor = PostCallProcessor()
