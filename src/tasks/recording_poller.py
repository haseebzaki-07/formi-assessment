"""
Phase 4 — long-lived recording polling stage.

Why this exists separately from celery_tasks.run_stage:
  Phase 1 wired the recording stage into the generic run_stage dispatcher,
  which means each "still not ready" outcome consumed an attempts budget
  and required a fresh Celery redelivery to retry. That's expensive at
  100K calls per campaign — every call pays Celery dispatch overhead 3-5
  times for a recording that just hasn't been published yet.

  The poller runs the entire backoff loop inside a single leased job.
  asyncio.sleep is fine here because the worker is otherwise idle waiting
  on Exotel; the lease is long enough to cover the full schedule
  (sum(BACKOFF) + slack), and a worker crash mid-poll cleanly leaves the
  row in `leased` state for the reconciler to reset.

What lands per attempt:
  - A `recording_poll_attempt` audit_events row with the attempt number,
    the next scheduled delay, and the poll outcome.
  - On READY → S3 upload, then `recording_completed` audit + complete_job.
  - On terminal failure (call_sid missing, attempts exhausted, hard
    transport error past the retry budget) → `recording_failed_terminal`
    audit at severity=error so a metrics scrape can alert on it, then
    fail_job (which transitions to `dead` once max_attempts is exhausted).

Alert thresholds for the metrics layer (documented, not implemented here):
  recording_failure_rate_warn — >5% terminal failures per customer per hour.
  recording_failure_rate_page — >20%.

Reconciliation (catches the "uploaded to S3 but DB write failed" gap and
worker churn):
  Beat schedule runs reap_stale_recording_jobs every 60s. Anything in
  `leased` for the recording stage past a 5-minute stuck threshold is
  reset to `pending` so a fresh worker can re-lease it.
"""

import asyncio
import logging
import os
import random
import socket
from typing import Any, Dict, Optional, Sequence

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
from src.services.job_store import STAGE_RECORDING
from src.services.recording import (
    RecordingFetchError,
    RecordingPollResult,
    RecordingPollStatus,
    poll_recording_once,
    upload_recording_to_s3,
)

logger = logging.getLogger(__name__)
configure_logging()


def _worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


def _run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _jittered(delay: float, jitter_frac: float) -> float:
    """Symmetric jitter ±jitter_frac. Clamped to >=0 for safety."""
    if jitter_frac <= 0:
        return delay
    spread = delay * jitter_frac
    return max(0.0, delay + random.uniform(-spread, spread))


@celery_app.task(
    name="poll_recording",
    bind=True,
    acks_late=True,
    queue=settings.RECORDING_POLL_QUEUE,
)
def poll_recording(self, job_id: str) -> None:
    """Lease the recording job and run the full poll-backoff-upload loop
    inside one Celery delivery. Idempotent across redeliveries: the lease
    check at entry makes a stale message a no-op."""
    _run_async(_poll_recording_async(job_id))


async def _poll_recording_async(
    job_id: str,
    *,
    backoff_schedule: Optional[Sequence[float]] = None,
    jitter_frac: Optional[float] = None,
    sleep: Any = asyncio.sleep,
) -> None:
    schedule = tuple(
        backoff_schedule
        if backoff_schedule is not None
        else settings.RECORDING_POLL_BACKOFF_SCHEDULE
    )
    jitter = (
        jitter_frac
        if jitter_frac is not None
        else settings.RECORDING_POLL_JITTER
    )
    # Lease long enough to cover the full schedule plus an Exotel-side slow
    # call. The reconciler picks up anything that runs over.
    lease_seconds = max(int(sum(schedule)) + 60, 180)

    async with async_session_factory() as session:
        leased = await job_store.lease_job(
            session,
            job_id=job_id,
            worker_id=_worker_id(),
            lease_seconds=lease_seconds,
        )
        if leased is None:
            await session.commit()
            logger.info(
                "recording_poll_lease_skipped",
                extra={"job_id": job_id},
            )
            return

        interaction_id = str(leased["interaction_id"])
        lease_token = str(leased["lease_token"])
        customer_id = str(leased["customer_id"])
        campaign_id = str(leased["campaign_id"])
        payload = leased["payload"] or {}

        await write_audit_event(
            session,
            interaction_id=interaction_id,
            event_type="stage_leased",
            stage=STAGE_RECORDING,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
            attempt=leased["attempts"],
            worker_id=_worker_id(),
        )
        await session.commit()

    call_sid = payload.get("call_sid") or ""
    exotel_account_id = payload.get("exotel_account_id") or ""

    with correlation_context(interaction_id):
        outcome = await _run_poll_loop(
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
            call_sid=call_sid,
            exotel_account_id=exotel_account_id,
            schedule=schedule,
            jitter_frac=jitter,
            sleep=sleep,
        )

        if outcome.status == RecordingPollStatus.READY:
            try:
                s3_key = await upload_recording_to_s3(
                    recording_url=outcome.recording_url or "",
                    interaction_id=interaction_id,
                )
            except Exception as exc:
                logger.exception(
                    "recording_upload_error",
                    extra={"job_id": job_id},
                )
                await _finalize_terminal_failure(
                    job_id=job_id,
                    lease_token=lease_token,
                    interaction_id=interaction_id,
                    customer_id=customer_id,
                    campaign_id=campaign_id,
                    reason="upload_error",
                    error=str(exc),
                    attempts=leased["attempts"],
                    max_attempts=leased["max_attempts"],
                )
                return

            await _finalize_completed(
                job_id=job_id,
                lease_token=lease_token,
                interaction_id=interaction_id,
                customer_id=customer_id,
                campaign_id=campaign_id,
                s3_key=s3_key,
            )
            return

        # Otherwise outcome is NOT_READY (exhausted) or PERMANENT_FAILURE.
        await _finalize_terminal_failure(
            job_id=job_id,
            lease_token=lease_token,
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            reason=(
                "permanent_failure"
                if outcome.status == RecordingPollStatus.PERMANENT_FAILURE
                else "polling_exhausted"
            ),
            error=outcome.reason or "recording not delivered within budget",
            attempts=leased["attempts"],
            max_attempts=leased["max_attempts"],
        )


async def _run_poll_loop(
    *,
    interaction_id: str,
    customer_id: str,
    campaign_id: str,
    job_id: str,
    call_sid: str,
    exotel_account_id: str,
    schedule: Sequence[float],
    jitter_frac: float,
    sleep: Any,
) -> RecordingPollResult:
    """Drive the poll-with-backoff loop. Writes one audit_events row per
    attempt. Returns the final RecordingPollResult — READY on success,
    NOT_READY on exhaustion, PERMANENT_FAILURE if the provider tells us
    the recording will never exist (e.g., missing call_sid)."""
    max_attempts = len(schedule) + 1  # one initial poll + backoff entries
    last_result: RecordingPollResult = RecordingPollResult(
        status=RecordingPollStatus.NOT_READY,
        reason="no attempts run",
    )

    for attempt_idx in range(max_attempts):
        try:
            result = await poll_recording_once(
                interaction_id=interaction_id,
                call_sid=call_sid,
                exotel_account_id=exotel_account_id,
            )
        except RecordingFetchError as exc:
            # Hard transport / auth error mid-loop. Don't keep hammering;
            # let the job state machine apply its own retry budget on top.
            await _emit_attempt_audit(
                interaction_id=interaction_id,
                customer_id=customer_id,
                campaign_id=campaign_id,
                job_id=job_id,
                attempt=attempt_idx + 1,
                max_attempts=max_attempts,
                outcome="transport_error",
                next_delay_seconds=None,
                detail=str(exc),
                severity="error",
            )
            return RecordingPollResult(
                status=RecordingPollStatus.PERMANENT_FAILURE,
                reason=f"transport_error: {exc}",
            )

        last_result = result
        is_last_attempt = attempt_idx == max_attempts - 1
        # Pick the next delay BEFORE writing the audit so the audit row
        # records what we'll actually wait. None on terminal / last attempt.
        if (
            result.status == RecordingPollStatus.READY
            or result.status == RecordingPollStatus.PERMANENT_FAILURE
            or is_last_attempt
        ):
            next_delay: Optional[float] = None
        else:
            next_delay = _jittered(schedule[attempt_idx], jitter_frac)

        await _emit_attempt_audit(
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
            attempt=attempt_idx + 1,
            max_attempts=max_attempts,
            outcome=result.status.value,
            next_delay_seconds=next_delay,
            detail=result.reason,
            severity="info" if result.status == RecordingPollStatus.READY else "warning",
        )

        if result.status == RecordingPollStatus.READY:
            return result
        if result.status == RecordingPollStatus.PERMANENT_FAILURE:
            return result
        if is_last_attempt:
            return result
        if next_delay is not None and next_delay > 0:
            await sleep(next_delay)

    return last_result


async def _emit_attempt_audit(
    *,
    interaction_id: str,
    customer_id: str,
    campaign_id: str,
    job_id: str,
    attempt: int,
    max_attempts: int,
    outcome: str,
    next_delay_seconds: Optional[float],
    detail: Optional[str],
    severity: str,
) -> None:
    async with async_session_factory() as session:
        await write_audit_event(
            session,
            interaction_id=interaction_id,
            event_type="recording_poll_attempt",
            severity=severity,
            stage=STAGE_RECORDING,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
            attempt=attempt,
            max_attempts=max_attempts,
            outcome=outcome,
            next_delay_seconds=(
                round(next_delay_seconds, 2)
                if next_delay_seconds is not None
                else None
            ),
            detail=detail,
        )
        await session.commit()


async def _finalize_completed(
    *,
    job_id: str,
    lease_token: str,
    interaction_id: str,
    customer_id: str,
    campaign_id: str,
    s3_key: str,
) -> None:
    async with async_session_factory() as session:
        ok = await job_store.complete_job(
            session,
            job_id=job_id,
            lease_token=lease_token,
            result={"s3_key": s3_key},
        )
        if not ok:
            await session.rollback()
            logger.warning(
                "recording_complete_lost_race",
                extra={"job_id": job_id},
            )
            return
        await write_audit_event(
            session,
            interaction_id=interaction_id,
            event_type="recording_completed",
            stage=STAGE_RECORDING,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
            s3_key=s3_key,
        )
        await session.commit()


async def _finalize_terminal_failure(
    *,
    job_id: str,
    lease_token: str,
    interaction_id: str,
    customer_id: str,
    campaign_id: str,
    reason: str,
    error: str,
    attempts: int,
    max_attempts: int,
) -> None:
    """A recording that exhausted its in-job poll budget OR hit a permanent
    failure. fail_job decides retry vs dead based on attempts; either way,
    we write a structured terminal-failure audit row at severity=error so
    a metrics scrape can alert on it.

    Note: attempts here is the JOB's attempts (driven by lease_job), not the
    poll-loop attempt count. The poll loop's per-attempt detail lives in
    the recording_poll_attempt audit rows.
    """
    async with async_session_factory() as session:
        new_state = await job_store.fail_job(
            session,
            job_id=job_id,
            lease_token=lease_token,
            error=f"{reason}: {error}",
            retry_in_seconds=settings.POSTCALL_RETRY_DELAY,
        )
        if new_state is None:
            await session.rollback()
            return
        is_terminal = new_state == "dead"
        await write_audit_event(
            session,
            interaction_id=interaction_id,
            event_type=(
                "recording_failed_terminal"
                if is_terminal
                else "recording_failed_will_retry"
            ),
            severity="error" if is_terminal else "warning",
            stage=STAGE_RECORDING,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
            reason=reason,
            error=error,
            attempts=attempts,
            max_attempts=max_attempts,
        )
        await session.commit()

    if new_state == "pending":
        # Re-dispatch via the poller queue with a delay so the next
        # whole-poll-loop runs against a possibly-recovered Exotel.
        poll_recording.apply_async(
            args=[str(job_id)],
            queue=settings.RECORDING_POLL_QUEUE,
            countdown=settings.POSTCALL_RETRY_DELAY,
        )


# ── Reconciliation: catches stuck recording jobs ─────────────────────────────

@celery_app.task(
    name="reconcile_recording_jobs",
    queue=settings.RECORDING_POLL_QUEUE,
)
def reconcile_recording_jobs_task() -> int:
    """Run via Celery beat every RECORDING_RECONCILE_INTERVAL_SECONDS.

    Two things to catch:
      1. Rows in `leased` whose lease has expired — generic stale-lease
         reaping handled by job_store.reap_stale_leases (also covered by
         the existing reap_stale_leases_task), but we re-run it here so
         the recording lane stays responsive even if the global reaper is
         slow.
      2. The "uploaded to S3 but DB write failed" gap: rows in `leased`
         for the recording stage that have been leased longer than
         RECORDING_STUCK_AFTER_SECONDS are reset to pending. The S3
         upload is keyed on interaction_id so re-running it just
         overwrites the same key — safe.

    Returns the count of rows reset, surfaced as a log/metric.
    """
    return _run_async(_reconcile_async())


async def _reconcile_async() -> int:
    async with async_session_factory() as session:
        result = await session.execute(
            text("""
                UPDATE processing_jobs
                SET state = 'pending',
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    updated_at = NOW()
                WHERE stage = :stage
                  AND state = 'leased'
                  AND (
                    lease_expires_at < NOW()
                    OR leased_at < NOW() - (:stuck_after || ' seconds')::interval
                  )
                RETURNING id, interaction_id
            """),
            {
                "stage": STAGE_RECORDING,
                "stuck_after": str(settings.RECORDING_STUCK_AFTER_SECONDS),
            },
        )
        rows = result.fetchall()
        count = len(rows)
        for row in rows:
            await write_audit_event(
                session,
                interaction_id=str(row.interaction_id),
                event_type="recording_reconciled_to_pending",
                severity="warning",
                stage=STAGE_RECORDING,
                job_id=str(row.id),
                detail="reset by reconciliation beat",
            )
        await session.commit()

    if count > 0:
        logger.warning(
            "recording_jobs_reconciled",
            extra={"count": count},
        )
        # Re-dispatch each reset row so the new poller picks it up
        # without waiting for a separate scan.
        for row in rows:
            poll_recording.apply_async(
                args=[str(row.id)],
                queue=settings.RECORDING_POLL_QUEUE,
            )
    return count
