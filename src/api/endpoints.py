"""
FastAPI endpoint for ending an interaction.

POST /session/{session_id}/interaction/{interaction_id}/end

Phase 0/1 changes from the original implementation:
  - The interaction row is now read from Postgres (no more hardcoded mock
    dict) and its status is updated atomically alongside pipeline creation
    in a single transaction. If the status UPDATE succeeds but the pipeline
    INSERT fails, neither lands — the interaction never enters a state
    where it is marked ENDED but has no work to do.
  - Pipeline state is durable: every interaction creates rows in
    processing_jobs in the same transaction as the audit_events
    "pipeline_created" entry. A crash anywhere downstream is recoverable
    because the rows already exist.
  - The duplicate fire-and-forget signal_jobs / update_lead_stage calls
    that ran from inside this endpoint with empty payloads are gone. Both
    are now durable stages owned by the worker pipeline.
  - For short transcripts, analysis is created in `skipped` state with a
    pre-populated result; the worker enforces the short-transcript skip
    again as a defensive check (AC8 — skip is enforced everywhere, not
    just at the API).
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.services import job_store
from src.services.job_store import (
    STAGE_ANALYSIS,
    STAGE_LEAD_STAGE,
    STAGE_RECORDING,
    STAGE_SIGNAL_JOBS,
)
from src.tasks.celery_tasks import run_stage
from src.utils.db import async_session_factory
from src.utils.logging import correlation_context, write_audit_event

logger = logging.getLogger(__name__)
router = APIRouter()


class InteractionEndRequest(BaseModel):
    call_sid: Optional[str] = None
    duration_seconds: Optional[int] = None
    call_status: Optional[str] = None
    additional_data: Optional[Dict[str, Any]] = None


class InteractionEndResponse(BaseModel):
    status: str
    interaction_id: str
    message: str


@router.post(
    "/session/{session_id}/interaction/{interaction_id}/end",
    response_model=InteractionEndResponse,
)
async def end_interaction(
    session_id: UUID,
    interaction_id: UUID,
    request: InteractionEndRequest,
):
    with correlation_context(interaction_id):
        try:
            interaction = await _load_interaction(interaction_id)
            if interaction is None:
                raise HTTPException(
                    status_code=404, detail="Interaction not found",
                )

            transcript = interaction.get("conversation_data", {}).get(
                "transcript", []
            )
            is_short = len(transcript) < 4
            transcript_text = "\n".join(
                f"{turn.get('role', 'unknown')}: {turn.get('content', '')}"
                for turn in transcript
            )

            payload = {
                "interaction_id": str(interaction_id),
                "session_id": str(session_id),
                "lead_id": interaction["lead_id"],
                "campaign_id": interaction["campaign_id"],
                "customer_id": interaction["customer_id"],
                "agent_id": interaction["agent_id"],
                "call_sid": request.call_sid,
                "transcript_text": transcript_text,
                "conversation_data": interaction.get("conversation_data", {}),
                "additional_data": request.additional_data or {},
                "ended_at": datetime.utcnow().isoformat(),
                "exotel_account_id": interaction.get("exotel_account_id"),
            }

            # Single transaction: status update + pipeline rows + audit event.
            # Either everything lands or nothing does. No half-states.
            async with async_session_factory() as db:
                await _update_interaction_status_in_session(
                    db,
                    interaction_id=str(interaction_id),
                    status="ENDED",
                    ended_at=datetime.utcnow(),
                    duration=request.duration_seconds,
                    call_sid=request.call_sid,
                )
                job_ids = await job_store.create_pipeline(
                    db,
                    interaction_id=str(interaction_id),
                    customer_id=interaction["customer_id"],
                    campaign_id=interaction["campaign_id"],
                    payload=payload,
                    has_long_transcript=not is_short,
                )
                await write_audit_event(
                    db,
                    interaction_id=str(interaction_id),
                    event_type="pipeline_created",
                    customer_id=interaction["customer_id"],
                    campaign_id=interaction["campaign_id"],
                    is_short_transcript=is_short,
                    stages=list(job_ids.keys()),
                    transcript_turns=len(transcript),
                )
                await db.commit()

            entry_stages = (
                [STAGE_RECORDING, STAGE_SIGNAL_JOBS, STAGE_LEAD_STAGE]
                if is_short
                else [STAGE_RECORDING, STAGE_ANALYSIS]
            )
            for stage in entry_stages:
                if stage not in job_ids:
                    continue
                run_stage.apply_async(
                    args=[str(job_ids[stage]), stage],
                    queue=settings.POSTCALL_CELERY_QUEUE,
                )

            logger.info(
                "pipeline_dispatched",
                extra={
                    "interaction_id": str(interaction_id),
                    "is_short_transcript": is_short,
                    "entry_stages": entry_stages,
                },
            )

            return InteractionEndResponse(
                status="ok",
                interaction_id=str(interaction_id),
                message="Interaction ended, durable pipeline created",
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.exception(
                "end_interaction_failed",
                extra={"interaction_id": str(interaction_id), "error": str(e)},
            )
            raise HTTPException(status_code=500, detail="Internal server error")


async def _load_interaction(interaction_id: UUID) -> Optional[Dict[str, Any]]:
    """
    Read the interaction row from Postgres. Returns None on miss; the
    caller raises 404. Note: exotel_account_id lives inside the
    conversation_data JSONB (per the existing model layout, see
    src/models/interaction.py) — there is no top-level column for it.
    """
    async with async_session_factory() as db:
        result = await db.execute(
            text("""
                SELECT id, lead_id, campaign_id, customer_id, agent_id,
                       conversation_data
                FROM interactions
                WHERE id = :id
            """),
            {"id": str(interaction_id)},
        )
        row = result.first()

    if row is None:
        return None

    conversation_data = row.conversation_data or {}
    return {
        "id": str(row.id),
        "lead_id": str(row.lead_id),
        "campaign_id": str(row.campaign_id),
        "customer_id": str(row.customer_id),
        "agent_id": str(row.agent_id),
        "exotel_account_id": conversation_data.get("exotel_account_sid"),
        "conversation_data": conversation_data,
    }


async def _update_interaction_status_in_session(
    db: AsyncSession,
    *,
    interaction_id: str,
    status: str,
    ended_at: datetime,
    duration: Optional[int],
    call_sid: Optional[str],
) -> None:
    """
    UPDATE the interaction row in the caller's transaction. COALESCE on
    duration / call_sid so a webhook that omits a field doesn't null out
    a value already set by an earlier event.
    """
    await db.execute(
        text("""
            UPDATE interactions
            SET status = :status,
                ended_at = :ended_at,
                duration_seconds = COALESCE(:duration, duration_seconds),
                call_sid = COALESCE(:call_sid, call_sid),
                updated_at = NOW()
            WHERE id = :id
        """),
        {
            "id": interaction_id,
            "status": status,
            "ended_at": ended_at,
            "duration": duration,
            "call_sid": call_sid,
        },
    )
    logger.info(
        "interaction_status_updated",
        extra={
            "interaction_id": interaction_id,
            "status": status,
            "ended_at": ended_at.isoformat(),
        },
    )
