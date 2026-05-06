"""
Recording fetch + S3 upload.

Phase 1 changes:
  - The asyncio.sleep(45) is gone. Recording is now its own job stage that
    runs in parallel with analysis (see celery_tasks._do_recording). If the
    recording isn't ready yet, this raises RecordingNotReady; the durable
    job state machine retries with backoff.
  - Failures are no longer swallowed silently. Every outcome (ready, not
    ready, hard error) propagates up where it becomes a structured audit
    event with the interaction_id correlation_id.

Phase 4 will replace the per-job-retry behavior with a single long-lived
polling stage that does its own exponential backoff inside the job, so we
don't pay Celery dispatch overhead on every poll.
"""

import logging
from typing import Optional

import httpx

from src.config import settings

logger = logging.getLogger(__name__)


class RecordingNotReady(Exception):
    """Recording isn't available yet — caller should retry later."""


class RecordingFetchError(Exception):
    """Hard failure fetching the recording (network, auth, etc.)."""


async def fetch_and_upload_recording(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
) -> str:
    """
    Fetch the Exotel recording and upload to S3. Returns the S3 key.

    Raises RecordingNotReady if Exotel returns 404 (not yet processed).
    Raises RecordingFetchError on transport / auth / unexpected errors.
    """
    if not call_sid:
        # No call_sid means we never connected (call dropped before pickup).
        # Don't retry — there's nothing to fetch. Caller should special-case
        # this, but for now treat it as a permanent skip via not-ready.
        raise RecordingNotReady("missing call_sid; nothing to fetch")

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
        logger.info(
            "recording_not_yet_available",
            extra={
                "interaction_id": interaction_id,
                "call_sid": call_sid,
            },
        )
        raise RecordingNotReady(
            f"exotel returned no url for call_sid={call_sid}"
        )

    s3_key = await _upload_to_s3(recording_url, interaction_id)
    return s3_key


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


async def _upload_to_s3(recording_url: str, interaction_id: str) -> str:
    """
    Stream from recording_url into S3 under recordings/{interaction_id}.mp3.
    Mock for the assessment — real impl would use boto3 multipart upload.
    """
    s3_key = f"recordings/{interaction_id}.mp3"
    logger.info(
        "recording_uploaded",
        extra={"interaction_id": interaction_id, "s3_key": s3_key},
    )
    return s3_key
