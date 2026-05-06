"""
Celery tasks for post-call processing — durable, per-stage execution.

The pipeline is data-driven: processing_jobs rows define what work exists
and what depends on what. Celery is the executor — it delivers a "go work
on job X" message and the worker leases the row, executes, and either
completes (unblocking dependents) or fails (with retry/dead semantics).

Why one dispatcher task instead of one task per stage?
  Routing stays in the data, not in Celery. Phase 3 will add hot/cold lanes
  by setting `queue` based on the job's `lane` column, without restructuring
  the task definitions here.

What changed from the old monolithic task:
  - asyncio.sleep(45) is gone. Recording is its own job, runs in parallel
    with analysis, retries via the job state machine instead of blocking.
  - Short-transcript skip is enforced inside the worker (AC8). The endpoint
    creates the analysis stage as `skipped`, but the worker also defends in
    case a redelivery races the state.
  - Failure modes: every stage exit emits an audit_events row in the same
    transaction as the state change. Crashes mid-stage leave the row in
    `leased` state; the lease expires and reap_stale_leases re-queues it.
  - No more PostCallRetryQueue (Redis). The processing_jobs state machine
    IS the retry queue, and lives in Postgres alongside the job payload.
"""

import asyncio
import logging
import os
import socket
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import text

from src.config import settings
from src.tasks.celery_app import celery_app
from src.utils.db import async_session_factory
from src.utils.logging import (
    configure_logging,
    correlation_context,
    write_audit_event,
)
from src.services import job_store
from src.services.job_store import (
    STAGE_ANALYSIS,
    STAGE_LEAD_STAGE,
    STAGE_RECORDING,
    STAGE_SIGNAL_JOBS,
)
from src.services.post_call_processor import PostCallContext, PostCallProcessor
from src.services.rate_limiter import RateLimitDeferred
from src.services.signal_jobs import trigger_signal_jobs, update_lead_stage

logger = logging.getLogger(__name__)
configure_logging()


class _SkipStage(Exception):
    """Raised inside a stage handler to mark the job skipped (not failed)."""

    def __init__(self, reason: str, result_payload: Optional[Dict[str, Any]] = None):
        super().__init__(reason)
        self.reason = reason
        self.result_payload = result_payload or {}


def queue_for_lane(lane: Optional[str], stage: str) -> str:
    """Map a (lane, stage) pair to the Celery queue that should service it.

    Analysis is the only LLM-bound stage and the only one that benefits
    from lane separation. signal_jobs / lead_stage are short, IO-bound,
    and don't compete for hot-lane analysis worker slots, so they stay on
    the default queue regardless of lane. Recording is dispatched via the
    dedicated poller queue (Phase 4) and is not routed through this helper.
    """
    if stage == STAGE_ANALYSIS:
        if lane == "hot":
            return settings.POSTCALL_HOT_QUEUE
        if lane == "cold":
            return settings.POSTCALL_COLD_QUEUE
    return settings.POSTCALL_CELERY_QUEUE


def _worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


def _run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@celery_app.task(
    name="run_stage",
    bind=True,
    acks_late=True,
    queue=settings.POSTCALL_CELERY_QUEUE,
)
def run_stage(self, job_id: str, stage: str) -> None:
    """
    Lease a specific job row and execute its stage. Idempotent across
    redeliveries — the lease check makes a stale Celery message a no-op.
    """
    _run_async(_run_stage_async(job_id, stage))


async def _run_stage_async(job_id: str, stage: str) -> None:
    if stage == STAGE_RECORDING:
        # Recording is owned by the dedicated long-lived poller (Phase 4).
        # If a stale message routes a recording job here (e.g., a queued
        # message from before the routing change, or a dependent dispatch
        # that didn't special-case recording), forward without leasing so
        # we don't burn an attempt.
        from src.tasks.recording_poller import poll_recording
        poll_recording.apply_async(
            args=[str(job_id)],
            queue=settings.RECORDING_POLL_QUEUE,
        )
        logger.info(
            "stage_recording_forwarded_to_poller",
            extra={"job_id": job_id, "stage": stage},
        )
        return

    async with async_session_factory() as session:
        leased = await job_store.lease_job(
            session, job_id=job_id, worker_id=_worker_id(),
        )
        if leased is None:
            await session.commit()
            logger.info(
                "stage_lease_skipped",
                extra={"job_id": job_id, "stage": stage},
            )
            return

        interaction_id = str(leased["interaction_id"])
        lease_token = str(leased["lease_token"])
        customer_id = str(leased["customer_id"])
        campaign_id = str(leased["campaign_id"])

        await write_audit_event(
            session,
            interaction_id=interaction_id,
            event_type="stage_leased",
            stage=stage,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
            attempt=leased["attempts"],
            worker_id=_worker_id(),
        )
        await session.commit()

    with correlation_context(interaction_id):
        try:
            payload = leased["payload"] or {}
            lane = leased.get("lane")
            if stage == STAGE_ANALYSIS:
                result_payload = await _do_analysis(
                    interaction_id=interaction_id,
                    job_id=job_id,
                    payload=payload,
                    lane=lane,
                )
            elif stage == STAGE_SIGNAL_JOBS:
                result_payload = await _do_signal_jobs(
                    interaction_id, payload,
                )
            elif stage == STAGE_LEAD_STAGE:
                result_payload = await _do_lead_stage(interaction_id, payload)
            else:
                raise ValueError(f"unknown stage: {stage}")

        except _SkipStage as skip:
            await _finalize_skipped(
                job_id=job_id,
                lease_token=lease_token,
                interaction_id=interaction_id,
                customer_id=customer_id,
                campaign_id=campaign_id,
                stage=stage,
                reason=skip.reason,
                result_payload=skip.result_payload,
            )
            return

        except RateLimitDeferred as deferred:
            # Soft retry — quota wasn't available, this is not a work failure.
            # defer_job decrements attempts to undo the lease's auto-increment;
            # defer_count is the bound that prevents infinite spin.
            await _finalize_deferred(
                job_id=job_id,
                lease_token=lease_token,
                interaction_id=interaction_id,
                customer_id=customer_id,
                campaign_id=campaign_id,
                stage=stage,
                retry_after_seconds=deferred.retry_after_seconds,
                reason=deferred.reason,
                lane=lane,
            )
            return

        except Exception as exc:
            logger.exception(
                "stage_exception",
                extra={"job_id": job_id, "stage": stage},
            )
            await _finalize_failed(
                job_id=job_id,
                lease_token=lease_token,
                interaction_id=interaction_id,
                customer_id=customer_id,
                campaign_id=campaign_id,
                stage=stage,
                error=str(exc),
                attempts=leased["attempts"],
                max_attempts=leased["max_attempts"],
                lane=lane,
            )
            return

        await _finalize_completed(
            job_id=job_id,
            lease_token=lease_token,
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            stage=stage,
            result_payload=result_payload,
        )


# ── Stage handlers ─────────────────────────────────────────────────────────
# Each returns the dict that becomes processing_jobs.result. Raise to fail.
# Raise _SkipStage to skip without consuming an attempt.
#
# Note: the recording stage is NOT handled here. Phase 4 moved it to the
# dedicated poller (src/tasks/recording_poller.py) so the in-job exponential
# backoff loop avoids per-attempt Celery dispatch overhead.

async def _do_analysis(
    *,
    interaction_id: str,
    job_id: str,
    payload: Dict[str, Any],
    lane: Optional[str] = None,
) -> Dict[str, Any]:
    """
    LLM analysis. Re-checks short-transcript at the worker (AC8) — even if
    the endpoint missed the fast path (e.g., a duplicate redelivery races
    the state), the worker won't burn LLM quota on a 3-turn transcript.

    Phase 2: PostCallProcessor.process_post_call wraps the LLM call in a
    rate-limit reservation (raises RateLimitDeferred if rejected — caught
    by _run_stage_async and turned into a defer_job). The job_id is passed
    through so the token_usage ledger row is keyed correctly.

    Phase 3: lane controls the prompt variant and reservation hint. Cold
    lane uses a summary-only prompt and a smaller reservation; hot lane
    uses the full analysis prompt at default reservation size.
    """
    transcript = payload.get("conversation_data", {}).get("transcript", [])
    if len(transcript) < 4:
        raise _SkipStage(
            reason="short_transcript",
            result_payload={"call_stage": "short_call", "skipped": True},
        )

    ctx = PostCallContext(
        interaction_id=interaction_id,
        session_id=payload.get("session_id", ""),
        lead_id=payload.get("lead_id", ""),
        campaign_id=payload.get("campaign_id", ""),
        customer_id=payload.get("customer_id", ""),
        agent_id=payload.get("agent_id", ""),
        call_sid=payload.get("call_sid", ""),
        transcript_text=payload.get("transcript_text", ""),
        conversation_data=payload.get("conversation_data", {}),
        additional_data=payload.get("additional_data", {}),
        ended_at=datetime.fromisoformat(
            payload["ended_at"]
        ) if payload.get("ended_at") else datetime.utcnow(),
        exotel_account_id=payload.get("exotel_account_id"),
    )
    processor = PostCallProcessor()
    result = await processor.process_post_call(
        ctx, job_id=job_id, single_prompt=True, lane=lane,
    )
    return {
        "call_stage": result.call_stage,
        "entities": result.entities,
        "summary": result.summary,
        "tokens_used": result.tokens_used,
        "latency_ms": result.latency_ms,
        "raw_response": result.raw_response,
    }


async def _do_signal_jobs(
    interaction_id: str, payload: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Read the analysis result from the parent job and dispatch downstream
    actions. The analysis result is the parent (depends_on_job_id) job's
    `result` column — read it inline rather than passing it through Celery,
    so a stale message can't carry stale analysis.
    """
    analysis_result = await _read_analysis_result(
        interaction_id, fallback_call_stage="processed"
    )
    await trigger_signal_jobs(
        interaction_id=interaction_id,
        session_id=payload.get("session_id", ""),
        campaign_id=payload.get("campaign_id", ""),
        analysis_result=analysis_result,
    )
    return {"dispatched": True, "call_stage": analysis_result.get("call_stage")}


async def _do_lead_stage(
    interaction_id: str, payload: Dict[str, Any],
) -> Dict[str, Any]:
    analysis_result = await _read_analysis_result(
        interaction_id, fallback_call_stage="processed"
    )
    call_stage = analysis_result.get("call_stage", "processed")
    await update_lead_stage(
        lead_id=payload.get("lead_id", ""),
        interaction_id=interaction_id,
        call_stage=call_stage,
    )
    return {"call_stage": call_stage}


async def _read_analysis_result(
    interaction_id: str, fallback_call_stage: str,
) -> Dict[str, Any]:
    """Look up the analysis stage row for this interaction and read its result.
    If the analysis was skipped (short transcript), use the skipped result."""
    async with async_session_factory() as session:
        row = await session.execute(
            text("""
                SELECT result, state FROM processing_jobs
                WHERE interaction_id = :interaction_id AND stage = :stage
            """),
            {"interaction_id": interaction_id, "stage": STAGE_ANALYSIS},
        )
        record = row.first()
    if record is None or record.result is None:
        return {"call_stage": fallback_call_stage}
    return record.result


# ── State-transition finalizers ────────────────────────────────────────────

async def _finalize_completed(
    *,
    job_id: str,
    lease_token: str,
    interaction_id: str,
    customer_id: str,
    campaign_id: str,
    stage: str,
    result_payload: Dict[str, Any],
) -> None:
    async with async_session_factory() as session:
        ok = await job_store.complete_job(
            session,
            job_id=job_id,
            lease_token=lease_token,
            result=result_payload,
        )
        if not ok:
            await session.rollback()
            logger.warning(
                "complete_lost_race",
                extra={"job_id": job_id, "stage": stage},
            )
            return
        await write_audit_event(
            session,
            interaction_id=interaction_id,
            event_type="stage_completed",
            stage=stage,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
            details=_safe_summary(result_payload),
        )
        dependents = await _newly_pending_dependents(session, parent_job_id=job_id)
        await session.commit()

    for dep in dependents:
        run_stage.apply_async(
            args=[str(dep["id"]), dep["stage"]],
            queue=queue_for_lane(dep.get("lane"), dep["stage"]),
        )


async def _finalize_skipped(
    *,
    job_id: str,
    lease_token: str,
    interaction_id: str,
    customer_id: str,
    campaign_id: str,
    stage: str,
    reason: str,
    result_payload: Dict[str, Any],
) -> None:
    async with async_session_factory() as session:
        ok = await job_store.mark_skipped(
            session,
            job_id=job_id,
            lease_token=lease_token,
            reason=reason,
            result_payload=result_payload,
        )
        if not ok:
            await session.rollback()
            return
        await write_audit_event(
            session,
            interaction_id=interaction_id,
            event_type="stage_skipped",
            stage=stage,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
            details={"reason": reason, **_safe_summary(result_payload)},
        )
        dependents = await _newly_pending_dependents(session, parent_job_id=job_id)
        await session.commit()

    for dep in dependents:
        run_stage.apply_async(
            args=[str(dep["id"]), dep["stage"]],
            queue=queue_for_lane(dep.get("lane"), dep["stage"]),
        )


async def _finalize_failed(
    *,
    job_id: str,
    lease_token: str,
    interaction_id: str,
    customer_id: str,
    campaign_id: str,
    stage: str,
    error: str,
    attempts: int,
    max_attempts: int,
    lane: Optional[str] = None,
) -> None:
    async with async_session_factory() as session:
        new_state = await job_store.fail_job(
            session,
            job_id=job_id,
            lease_token=lease_token,
            error=error,
            retry_in_seconds=settings.POSTCALL_RETRY_DELAY,
        )
        if new_state is None:
            await session.rollback()
            return
        severity = "warning" if new_state == "pending" else "error"
        await write_audit_event(
            session,
            interaction_id=interaction_id,
            event_type=(
                "stage_failed_will_retry"
                if new_state == "pending"
                else "stage_dead"
            ),
            severity=severity,
            stage=stage,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
            error=error,
            attempts=attempts,
            max_attempts=max_attempts,
        )
        await session.commit()

    if new_state == "pending":
        run_stage.apply_async(
            args=[str(job_id), stage],
            queue=queue_for_lane(lane, stage),
            countdown=settings.POSTCALL_RETRY_DELAY,
        )


async def _finalize_deferred(
    *,
    job_id: str,
    lease_token: str,
    interaction_id: str,
    customer_id: str,
    campaign_id: str,
    stage: str,
    retry_after_seconds: int,
    reason: str,
    lane: Optional[str] = None,
) -> None:
    """
    Soft retry for rate-limit rejections. defer_job decrements attempts so
    this doesn't burn the work-attempt budget; defer_count is the bound
    that prevents infinite spin (a permanently-constrained customer's job
    eventually goes dead after max_defers).
    """
    async with async_session_factory() as session:
        new_state = await job_store.defer_job(
            session,
            job_id=job_id,
            lease_token=lease_token,
            retry_in_seconds=retry_after_seconds,
            reason=reason,
        )
        if new_state is None:
            await session.rollback()
            return
        await write_audit_event(
            session,
            interaction_id=interaction_id,
            event_type=(
                "stage_rate_limit_deferred"
                if new_state == "pending"
                else "stage_dead_defer_limit"
            ),
            severity="warning" if new_state == "pending" else "error",
            stage=stage,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
            reason=reason,
            retry_after_seconds=retry_after_seconds,
        )
        await session.commit()

    if new_state == "pending":
        run_stage.apply_async(
            args=[str(job_id), stage],
            queue=queue_for_lane(lane, stage),
            countdown=retry_after_seconds,
        )


async def _newly_pending_dependents(session, *, parent_job_id: str):
    result = await session.execute(
        text("""
            SELECT id, stage, lane FROM processing_jobs
            WHERE depends_on_job_id = :parent AND state = 'pending'
        """),
        {"parent": str(parent_job_id)},
    )
    return [dict(r._mapping) for r in result.all()]


def _safe_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    """Trim large fields out of audit_events.details (raw_response can be big)."""
    if not result:
        return {}
    keep = {"call_stage", "tokens_used", "latency_ms", "s3_key", "skipped"}
    return {k: v for k, v in result.items() if k in keep}


# ── Janitor: periodically reap stale leases (worker crashes) ──────────────

@celery_app.task(name="reap_stale_leases", queue=settings.POSTCALL_CELERY_QUEUE)
def reap_stale_leases_task() -> int:
    """Run via Celery beat every 30s. Returns count of rows reaped — surface
    to metrics so persistent worker churn is observable."""
    return _run_async(_reap_async())


async def _reap_async() -> int:
    async with async_session_factory() as session:
        count = await job_store.reap_stale_leases(session)
        await session.commit()
        if count > 0:
            logger.warning("reaped_stale_leases", extra={"count": count})
        return count
