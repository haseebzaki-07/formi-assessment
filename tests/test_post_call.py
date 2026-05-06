"""
Tests for the current post-call processing system.

These tests document the EXISTING behaviour — including its problems.
Your solution should make these tests obsolete and replace them with
tests that validate the new architecture.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime

from src.services.post_call_processor import PostCallProcessor, PostCallContext


@pytest.mark.skip(
    reason=(
        "Documents pre-Phase-2 behaviour. Phase 2 wrapped process_post_call "
        "in a rate-limit reservation, requires a job_id, and renamed "
        "_update_interaction_metadata; Phase 3 introduced the triage step "
        "this test predates. Phase 6 replaces this file with "
        "tests/test_post_call_pipeline.py covering the new pipeline."
    )
)
@pytest.mark.asyncio
async def test_every_call_gets_full_llm_analysis(make_post_call_context):
    """
    CURRENT BEHAVIOUR: Even a clear "not interested" call gets full LLM analysis.
    This is the core inefficiency — there is no triage step.
    """
    ctx = make_post_call_context("not_interested")
    processor = PostCallProcessor()

    with patch.object(processor, "_call_llm", new_callable=AsyncMock) as mock_llm:
        mock_llm.return_value = {
            "call_stage": "not_interested",
            "entities": {},
            "summary": "Customer not interested",
            "usage": {"total_tokens": 1200},
        }

        with patch.object(processor, "_update_interaction_metadata", new_callable=AsyncMock):
            result = await processor.process_post_call(ctx)

        # LLM was called — even though the transcript clearly says "not interested"
        mock_llm.assert_called_once()
        assert result.tokens_used == 1200


@pytest.mark.asyncio
async def test_rebook_gets_same_priority_as_not_interested(make_post_call_context):
    """
    CURRENT BEHAVIOUR: A high-value rebook confirmation has zero priority
    over a "not interested" call. Both sit in the same Celery queue.
    """
    rebook_ctx = make_post_call_context("rebook_confirmed")
    not_interested_ctx = make_post_call_context("not_interested")

    # Both would be enqueued to the same "postcall_processing" queue
    # with no priority differentiation
    assert True  # Documenting the absence of prioritisation


@pytest.mark.asyncio
async def test_short_transcript_detected():
    """
    CURRENT BEHAVIOUR: Short transcripts ARE detected, but only at the
    FastAPI endpoint level. If the detection fails or the logic changes,
    short calls still enter the Celery queue.
    """
    ctx = PostCallContext(
        interaction_id="test-001",
        session_id="test-session",
        lead_id="test-lead",
        campaign_id="test-campaign",
        customer_id="test-customer",
        agent_id="test-agent",
        call_sid="test-call",
        transcript_text="agent: Hello\ncustomer: Wrong number",
        conversation_data={"transcript": [
            {"role": "agent", "content": "Hello"},
            {"role": "customer", "content": "Wrong number"},
        ]},
        additional_data={},
        ended_at=datetime.utcnow(),
    )

    # Short transcript detection exists but is fragile
    transcript = ctx.conversation_data.get("transcript", [])
    is_short = len(transcript) < 4
    assert is_short is True


@pytest.mark.asyncio
async def test_recording_blocks_processing(make_post_call_context):
    """
    CURRENT BEHAVIOUR: Recording upload blocks for 45 seconds before
    LLM analysis can start, even if the recording is available immediately
    or won't be available at all.
    """
    # The asyncio.sleep(45) in recording.py means every call waits 45s
    # before any LLM analysis begins, regardless of recording availability.
    # This test documents the coupling — recording should not block analysis.
    assert True  # Documenting the 45s blocking sleep


# NOTE: test_circuit_breaker_freezes_dialler was removed in Phase 5 along
# with src/services/circuit_breaker.py itself. The replacement module is
# src/services/backpressure.py — its smooth-shed behaviour is exercised
# in tests/test_backpressure.py.
