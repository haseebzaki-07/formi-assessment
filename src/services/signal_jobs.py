"""
Signal jobs — downstream actions triggered after post-call analysis.

Examples of what runs here in production:
  - Send a WhatsApp message to the lead ("Your appointment is confirmed for 3 PM tomorrow")
  - Book a callback slot in the scheduling system
  - Push the call outcome to the customer's CRM via webhook
  - Flag the interaction for human review if the lead was angry

Phase 1 made these durable stages: the worker pipeline owns invocation
through processing_jobs. The endpoint no longer fires duplicate
empty-payload triggers, and a worker crash mid-dispatch leaves the row
in `leased` state so the stale-lease reaper recovers it. The CRM-push
retry contract (deferred nice-to-have, see SUBMISSION.md §14) would
extend these handlers with per-action attempt tracking.
"""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


async def trigger_signal_jobs(
    interaction_id: str,
    session_id: str,
    campaign_id: str,
    analysis_result: Dict[str, Any],
) -> None:
    """
    Dispatch downstream actions based on the call analysis.

    analysis_result is read from the parent analysis stage's result column
    (see celery_tasks._read_analysis_result), so the payload the worker
    sees is always the LLM's actual output (or the short-call fallback)
    rather than a stale snapshot.
    """
    logger.info(
        "signal_jobs_triggered",
        extra={
            "interaction_id": interaction_id,
            "campaign_id": campaign_id,
            "has_analysis": bool(analysis_result),
            # has_analysis=False means we fired with an empty payload.
            # That happens for every long-transcript call, from the endpoint.
        },
    )
    # Mock: production implementation dispatches to downstream services


async def update_lead_stage(
    lead_id: str,
    interaction_id: str,
    call_stage: str,
) -> None:
    """
    Update the lead's stage in the leads table.

    call_stage maps to a stage in the sales funnel:
      "rebook_confirmed" → "booked"
      "not_interested"   → "closed_lost"
      "callback_requested" → "follow_up"
      "short_call"       → unchanged (no business signal from the call)
    """
    logger.info(
        "lead_stage_updated",
        extra={
            "lead_id": lead_id,
            "interaction_id": interaction_id,
            "new_stage": call_stage,
        },
    )
    # Mock: production implementation runs UPDATE leads SET stage = $2 WHERE id = $1
