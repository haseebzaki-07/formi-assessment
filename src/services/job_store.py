"""
JobStore — durable pipeline state for post-call processing.

The processing_jobs table is the source of truth. Celery delivers a "go work
on job X" message; the worker leases the row, executes, and either completes,
fails-with-retry, or marks-dead. A worker crash leaves the row in `leased`
state; once `lease_expires_at` passes, any worker can pick it up via
`lease_job` (idempotency: the lease_token check on every state transition
makes the original worker's late completion a no-op).

State machine:
    pending  → leased    (lease_job)
    blocked  → pending   (when dependency completes)
    leased   → completed (complete_job — unblocks dependents)
    leased   → failed    (fail_job; if attempts < max → pending; else → dead)
    leased   → skipped   (mark_skipped — for short-transcript fast path)
    leased   → pending   (reap_stale_leases, lease expired)

Why raw SQL instead of the ORM? The lease query needs FOR UPDATE SKIP LOCKED
and a conditional-update-with-returning idiom that's awkward through the ORM.
The table is small in surface area and these queries are stable.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional, Sequence
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


# Stage names — used as Celery task targets and queue routing keys (Phase 3).
STAGE_RECORDING = "recording"
STAGE_ANALYSIS = "analysis"
STAGE_SIGNAL_JOBS = "signal_jobs"
STAGE_LEAD_STAGE = "lead_stage"
ALL_STAGES = (STAGE_RECORDING, STAGE_ANALYSIS, STAGE_SIGNAL_JOBS, STAGE_LEAD_STAGE)

# State constants.
STATE_PENDING = "pending"
STATE_BLOCKED = "blocked"
STATE_LEASED = "leased"
STATE_COMPLETED = "completed"
STATE_FAILED = "failed"
STATE_DEAD = "dead"
STATE_SKIPPED = "skipped"


def _row_to_dict(row: Mapping[str, Any]) -> Dict[str, Any]:
    return {k: row[k] for k in row.keys()}


async def create_pipeline(
    session: AsyncSession,
    *,
    interaction_id: str | UUID,
    customer_id: str | UUID,
    campaign_id: str | UUID,
    payload: Dict[str, Any],
    has_long_transcript: bool,
) -> Dict[str, UUID]:
    """
    Insert the stage rows for one interaction's pipeline in a single
    transaction. Idempotent: if rows already exist (duplicate webhook),
    returns the existing IDs without modification.

    Layout:
      recording   (independent, runs in parallel with analysis)
      analysis    (independent OR skipped if short transcript)
      signal_jobs (depends_on analysis)
      lead_stage  (depends_on analysis)

    For short transcripts, analysis is created in `skipped` state and
    signal_jobs/lead_stage are created in `pending` state directly (no
    dependency to wait on).
    """
    interaction_uuid = str(interaction_id)
    customer_uuid = str(customer_id)
    campaign_uuid = str(campaign_id)

    recording_id = uuid4()
    analysis_id = uuid4()
    signal_id = uuid4()
    lead_stage_id = uuid4()

    analysis_initial_state = STATE_PENDING if has_long_transcript else STATE_SKIPPED
    dependents_initial_state = (
        STATE_BLOCKED if has_long_transcript else STATE_PENDING
    )
    dependents_depends_on = analysis_id if has_long_transcript else None

    insert_sql = text("""
        INSERT INTO processing_jobs
            (id, interaction_id, customer_id, campaign_id,
             stage, state, payload, result, depends_on_job_id, scheduled_at)
        VALUES
            (:id, :interaction_id, :customer_id, :campaign_id,
             :stage, :state, cast(:payload as jsonb),
             cast(:result as jsonb), :depends_on, NOW())
        ON CONFLICT (interaction_id, stage) DO NOTHING
        RETURNING id, stage
    """)

    # When analysis is short-circuited (short transcript), pre-populate its
    # result column so signal_jobs / lead_stage dependents see call_stage =
    # 'short_call' when they read it via depends_on_job_id.
    skipped_analysis_result = (
        None
        if has_long_transcript
        else {"call_stage": "short_call", "skipped": True}
    )

    rows_to_insert = [
        {
            "id": str(recording_id),
            "stage": STAGE_RECORDING,
            "state": STATE_PENDING,
            "depends_on": None,
            "result": None,
        },
        {
            "id": str(analysis_id),
            "stage": STAGE_ANALYSIS,
            "state": analysis_initial_state,
            "depends_on": None,
            "result": skipped_analysis_result,
        },
        {
            "id": str(signal_id),
            "stage": STAGE_SIGNAL_JOBS,
            "state": dependents_initial_state,
            "depends_on": str(dependents_depends_on) if dependents_depends_on else None,
            "result": None,
        },
        {
            "id": str(lead_stage_id),
            "stage": STAGE_LEAD_STAGE,
            "state": dependents_initial_state,
            "depends_on": str(dependents_depends_on) if dependents_depends_on else None,
            "result": None,
        },
    ]

    import json as _json
    payload_json = _json.dumps(payload, default=str)
    inserted: Dict[str, UUID] = {}
    for row in rows_to_insert:
        result = await session.execute(
            insert_sql,
            {
                "id": row["id"],
                "interaction_id": interaction_uuid,
                "customer_id": customer_uuid,
                "campaign_id": campaign_uuid,
                "stage": row["stage"],
                "state": row["state"],
                "payload": payload_json,
                "result": (
                    _json.dumps(row["result"], default=str)
                    if row["result"] is not None
                    else None
                ),
                "depends_on": row["depends_on"],
            },
        )
        returned = result.first()
        if returned is not None:
            inserted[returned.stage] = UUID(str(returned.id))

    # If any rows hit ON CONFLICT (duplicate delivery), fetch the existing IDs
    # so the caller still gets the full {stage: id} map.
    if len(inserted) < len(rows_to_insert):
        existing = await session.execute(
            text("""
                SELECT id, stage FROM processing_jobs
                WHERE interaction_id = :interaction_id
            """),
            {"interaction_id": interaction_uuid},
        )
        for row in existing:
            inserted.setdefault(row.stage, UUID(str(row.id)))

    return inserted


async def lease_job(
    session: AsyncSession,
    *,
    job_id: str | UUID,
    worker_id: str,
    lease_seconds: int = 120,
) -> Optional[Dict[str, Any]]:
    """
    Atomically lease a specific job row. Used when Celery delivers a task
    that names the job_id. Succeeds only if the row is currently `pending`
    OR is `leased` with an expired lease (stale-worker recovery).

    Returns the leased row as a dict (incl. lease_token) or None if the row
    is not leasable (already completed, dead, blocked, etc.).
    """
    new_token = uuid4()
    result = await session.execute(
        text("""
            UPDATE processing_jobs
            SET state = 'leased',
                lease_token = :new_token,
                leased_at = NOW(),
                lease_expires_at = NOW() + (:lease_seconds || ' seconds')::interval,
                worker_id = :worker_id,
                attempts = attempts + 1,
                updated_at = NOW()
            WHERE id = :job_id
              AND (
                state = 'pending'
                OR (state = 'leased' AND lease_expires_at < NOW())
              )
            RETURNING id, interaction_id, customer_id, campaign_id, stage,
                      state, attempts, max_attempts, lease_token, payload,
                      depends_on_job_id
        """),
        {
            "new_token": str(new_token),
            "lease_seconds": str(lease_seconds),
            "worker_id": worker_id,
            "job_id": str(job_id),
        },
    )
    row = result.first()
    if row is None:
        return None
    return _row_to_dict(row._mapping)


async def lease_next(
    session: AsyncSession,
    *,
    stage: str,
    worker_id: str,
    lease_seconds: int = 120,
) -> Optional[Dict[str, Any]]:
    """
    Pick the next eligible row for `stage` and lease it atomically.
    Used by poller-style consumers (e.g., the Phase 4 recording poller and
    the periodic stale-lease reaper). Honors priority and scheduled_at.
    """
    new_token = uuid4()
    result = await session.execute(
        text("""
            UPDATE processing_jobs
            SET state = 'leased',
                lease_token = :new_token,
                leased_at = NOW(),
                lease_expires_at = NOW() + (:lease_seconds || ' seconds')::interval,
                worker_id = :worker_id,
                attempts = attempts + 1,
                updated_at = NOW()
            WHERE id = (
                SELECT id FROM processing_jobs
                WHERE stage = :stage
                  AND (
                    (state = 'pending' AND scheduled_at <= NOW())
                    OR (state = 'leased' AND lease_expires_at < NOW())
                  )
                ORDER BY priority ASC, scheduled_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, interaction_id, customer_id, campaign_id, stage,
                      state, attempts, max_attempts, lease_token, payload,
                      depends_on_job_id
        """),
        {
            "new_token": str(new_token),
            "lease_seconds": str(lease_seconds),
            "worker_id": worker_id,
            "stage": stage,
        },
    )
    row = result.first()
    if row is None:
        return None
    return _row_to_dict(row._mapping)


async def complete_job(
    session: AsyncSession,
    *,
    job_id: str | UUID,
    lease_token: str | UUID,
    result: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Mark a leased job complete. Returns True if the conditional UPDATE
    matched (this caller still owns the lease); False if it didn't (lease
    expired and was reclaimed by another worker — the original's late
    completion is a no-op, which is the durability guarantee).

    On success, dependents (rows with depends_on_job_id = $1 in `blocked`
    state) flip to `pending` in the same transaction.
    """
    import json as _json
    result_json = _json.dumps(result or {}, default=str)

    update_result = await session.execute(
        text("""
            UPDATE processing_jobs
            SET state = 'completed',
                result = cast(:result as jsonb),
                lease_token = NULL,
                lease_expires_at = NULL,
                updated_at = NOW()
            WHERE id = :job_id
              AND lease_token = :lease_token
              AND state = 'leased'
            RETURNING id
        """),
        {
            "result": result_json,
            "job_id": str(job_id),
            "lease_token": str(lease_token),
        },
    )
    if update_result.first() is None:
        return False

    # Unblock dependents in the same transaction.
    await session.execute(
        text("""
            UPDATE processing_jobs
            SET state = 'pending', updated_at = NOW()
            WHERE depends_on_job_id = :job_id AND state = 'blocked'
        """),
        {"job_id": str(job_id)},
    )
    return True


async def fail_job(
    session: AsyncSession,
    *,
    job_id: str | UUID,
    lease_token: str | UUID,
    error: str,
    retry_in_seconds: int = 60,
) -> Optional[str]:
    """
    Mark a leased job failed. If attempts < max_attempts, schedule a retry
    by transitioning back to `pending` with scheduled_at = NOW + delay.
    Otherwise transition to `dead` (terminal — preserves payload for replay).

    Returns the new state ('pending' for retry, 'dead' for terminal) or None
    if the lease check failed (caller no longer owns the row).
    """
    result = await session.execute(
        text("""
            UPDATE processing_jobs
            SET state = CASE
                    WHEN attempts >= max_attempts THEN 'dead'
                    ELSE 'pending'
                END,
                last_error = :error,
                lease_token = NULL,
                lease_expires_at = NULL,
                scheduled_at = CASE
                    WHEN attempts >= max_attempts THEN scheduled_at
                    ELSE NOW() + (:retry_in_seconds || ' seconds')::interval
                END,
                updated_at = NOW()
            WHERE id = :job_id
              AND lease_token = :lease_token
              AND state = 'leased'
            RETURNING state
        """),
        {
            "error": error[:8000],
            "retry_in_seconds": str(retry_in_seconds),
            "job_id": str(job_id),
            "lease_token": str(lease_token),
        },
    )
    row = result.first()
    if row is None:
        return None
    return row.state


async def mark_skipped(
    session: AsyncSession,
    *,
    job_id: str | UUID,
    lease_token: str | UUID,
    reason: str,
    result_payload: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Mark a leased job skipped — e.g., the analysis worker leased a job for
    a transcript that turned out to be short. Doesn't count as a failure;
    dependents still unblock as if it succeeded (skipped == fast-path success).

    The stored result merges {skipped_reason: ...} with result_payload so
    downstream stages reading via depends_on_job_id see the same shape they
    would see for a normal completion (e.g., {"call_stage": "short_call"}).
    """
    import json as _json
    merged = {"skipped_reason": reason}
    if result_payload:
        merged.update(result_payload)
    update_result = await session.execute(
        text("""
            UPDATE processing_jobs
            SET state = 'skipped',
                last_error = NULL,
                result = cast(:result as jsonb),
                lease_token = NULL,
                lease_expires_at = NULL,
                updated_at = NOW()
            WHERE id = :job_id
              AND lease_token = :lease_token
              AND state = 'leased'
            RETURNING id
        """),
        {
            "result": _json.dumps(merged, default=str),
            "job_id": str(job_id),
            "lease_token": str(lease_token),
        },
    )
    if update_result.first() is None:
        return False
    await session.execute(
        text("""
            UPDATE processing_jobs
            SET state = 'pending', updated_at = NOW()
            WHERE depends_on_job_id = :job_id AND state = 'blocked'
        """),
        {"job_id": str(job_id)},
    )
    return True


async def get_job(
    session: AsyncSession,
    *,
    job_id: str | UUID,
) -> Optional[Dict[str, Any]]:
    result = await session.execute(
        text("""
            SELECT id, interaction_id, customer_id, campaign_id, stage,
                   state, attempts, max_attempts, lease_token, payload,
                   result, last_error, depends_on_job_id, scheduled_at,
                   created_at, updated_at
            FROM processing_jobs WHERE id = :job_id
        """),
        {"job_id": str(job_id)},
    )
    row = result.first()
    return _row_to_dict(row._mapping) if row else None


async def reap_stale_leases(session: AsyncSession) -> int:
    """
    Janitor: rows where state='leased' and lease_expires_at has passed are
    transitioned back to 'pending' so another worker can pick them up. Run
    periodically (Celery beat task, every 30s in production). Returns the
    number of rows reaped — useful for alerting on persistent worker churn.
    """
    result = await session.execute(
        text("""
            UPDATE processing_jobs
            SET state = 'pending',
                lease_token = NULL,
                lease_expires_at = NULL,
                updated_at = NOW()
            WHERE state = 'leased' AND lease_expires_at < NOW()
            RETURNING id
        """),
    )
    return len(result.fetchall())


def _json_dump(value: Any) -> str:
    import json as _json
    return _json.dumps(value, default=str)
