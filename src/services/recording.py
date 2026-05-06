"""
Recording fetch + S3 upload — poll-friendly shape (Phase 4).

What changed vs Phase 1:
  - The single fetch_and_upload_recording is split. The poll attempt
    (poll_recording_once) returns a structured RecordingPollResult so the
    long-lived recording_poller can decide whether to backoff-and-retry,
    upload, or terminate without raising/catching exceptions on the hot
    path. Exceptions still surface for hard transport errors so the worker's
    state-machine semantics catch them.
  - upload_recording_to_s3 is exposed separately so the poller can run the
    upload step *after* the recording is confirmed ready; this keeps the S3
    upload out of the polling retry budget. Re-uploading a recording on
    a redelivery is idempotent because the S3 key is keyed on
    interaction_id.

The asyncio.sleep(45) was killed in Phase 1. The Phase 4 poller uses real
exponential backoff with jitter: 5s, 10s, 20s, 40s, 80s (max 5 attempts).

Why the structured-result shape instead of "raise on every miss"? The
poller polls 5+ times per recording in the slow case. Building exception
objects on the happy-but-not-yet-ready path is wasted work, and it makes
the audit_events emission code awkward (the audit row should always be
written, not just on the exception edge). Returning a tagged result keeps
the per-attempt path linear.

Hard transport errors (network, auth) still raise RecordingFetchError so
the job state machine treats them as a stage failure and applies its own
retry/dead semantics on top of the poller's loop.
"""

import enum
import logging
from dataclasses import dataclass
from typing import Optional

import httpx

from src.config import settings

logger = logging.getLogger(__name__)


class RecordingNotReady(Exception):
    """Recording isn't available yet — caller should retry later.

    Retained for backwards compatibility with the Phase 1 _do_recording
    handler; the Phase 4 poller uses RecordingPollResult instead so it
    doesn't have to catch on every miss.
    """


class RecordingFetchError(Exception):
    """Hard failure fetching the recording (network, auth, etc.)."""


class RecordingPollStatus(str, enum.Enum):
    READY = "ready"
    NOT_READY = "not_ready"
    PERMANENT_FAILURE = "permanent_failure"


@dataclass
class RecordingPollResult:
    """Outcome of a single poll against the provider's recording endpoint.

    READY → recording_url is set; caller should upload to S3.
    NOT_READY → recording isn't published yet; caller should backoff + retry.
    PERMANENT_FAILURE → don't retry; surface as terminal failure
        (e.g. missing call_sid means we never connected).
    """

    status: RecordingPollStatus
    recording_url: Optional[str] = None
    reason: Optional[str] = None


async def poll_recording_once(
    *,
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
) -> RecordingPollResult:
    """One poll attempt. Does NOT sleep between calls — the poller owns the
    backoff schedule. Hard transport errors raise RecordingFetchError; soft
    "not ready" / "permanently won't be ready" outcomes return a tagged
    result so the poller can write an audit event without an exception
    round-trip on every iteration.
    """
    if not call_sid:
        return RecordingPollResult(
            status=RecordingPollStatus.PERMANENT_FAILURE,
            reason="missing call_sid; nothing to fetch",
        )

    try:
        recording_url = await _fetch_exotel_recording_url(
            call_sid, exotel_account_id,
        )
    except httpx.HTTPError as exc:
        logger.warning(
            "recording_fetch_transport_error",
            extra={
                "interaction_id": interaction_id,
                "call_sid": call_sid,
                "error": str(exc),
            },
        )
        raise RecordingFetchError(str(exc)) from exc

    if recording_url is None:
        return RecordingPollResult(
            status=RecordingPollStatus.NOT_READY,
            reason=f"exotel returned no url for call_sid={call_sid}",
        )

    return RecordingPollResult(
        status=RecordingPollStatus.READY,
        recording_url=recording_url,
    )


async def upload_recording_to_s3(
    *,
    recording_url: str,
    interaction_id: str,
) -> str:
    """Upload the recording artefact to S3. Returns the S3 key.

    Idempotent on interaction_id — a redelivery overwrites the same key.
    Mock for the assessment; real impl would use boto3 multipart upload.
    """
    s3_key = f"recordings/{interaction_id}.mp3"
    logger.info(
        "recording_uploaded",
        extra={"interaction_id": interaction_id, "s3_key": s3_key},
    )
    return s3_key


async def fetch_and_upload_recording(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
) -> str:
    """Single-shot fetch + upload. Retained as the Phase 1 entry point so
    the existing _do_recording handler keeps working until the dispatcher
    is routed to the poller. New callers should use poll_recording_once +
    upload_recording_to_s3 directly.

    Raises RecordingNotReady if Exotel returns 404 (not yet processed).
    Raises RecordingFetchError on transport / auth / unexpected errors.
    """
    result = await poll_recording_once(
        interaction_id=interaction_id,
        call_sid=call_sid,
        exotel_account_id=exotel_account_id,
    )
    if result.status == RecordingPollStatus.NOT_READY:
        logger.info(
            "recording_not_yet_available",
            extra={
                "interaction_id": interaction_id,
                "call_sid": call_sid,
            },
        )
        raise RecordingNotReady(
            result.reason or f"recording not ready for {call_sid}"
        )
    if result.status == RecordingPollStatus.PERMANENT_FAILURE:
        raise RecordingNotReady(
            result.reason or "recording cannot be fetched"
        )
    return await upload_recording_to_s3(
        recording_url=result.recording_url or "",
        interaction_id=interaction_id,
    )


async def _fetch_exotel_recording_url(
    call_sid: str, account_id: str,
) -> Optional[str]:
    """
    Hit the Exotel API. Returns the URL on 200, None on 404.
    Other status codes raise httpx.HTTPStatusError.
    """
    if not account_id:
        return None
    url = (
        f"https://api.exotel.com/v1/Accounts/{account_id}"
        f"/Calls/{call_sid}/Recording"
    )
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        return data.get("recording_url")
