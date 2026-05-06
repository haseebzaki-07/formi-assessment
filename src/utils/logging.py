"""
Structured logging + audit trail for the post-call pipeline.

Two surfaces:
  - JsonFormatter / configure_logging — one-line-per-event JSON to stdout, with
    correlation_id automatically pulled from a contextvar so callers don't have
    to thread interaction_id through every log call.
  - write_audit_event — append a row to audit_events. Caller passes the
    AsyncSession so the audit row participates in the same transaction as the
    state change it describes (atomic: either the state change AND the audit
    entry both land, or neither does).

Why both? stdout JSON is for grep / Cloudwatch / Loki — fast for ops. The
audit_events table is the durable record an on-call engineer queries when
asked to reconstruct what happened to a specific interaction 3 days later
(AC5 / AC6). The two views are complementary.
"""

import contextvars
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Optional, Union
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


_correlation_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "correlation_id", default=None
)


def set_correlation_id(value: Optional[Union[str, UUID]]) -> None:
    _correlation_id.set(str(value) if value is not None else None)


def get_correlation_id() -> Optional[str]:
    return _correlation_id.get()


class _CorrelationContext:
    def __init__(self, value: Union[str, UUID]):
        self._value = str(value)
        self._token: Optional[contextvars.Token] = None

    def __enter__(self) -> str:
        self._token = _correlation_id.set(self._value)
        return self._value

    def __exit__(self, *_exc: Any) -> None:
        if self._token is not None:
            _correlation_id.reset(self._token)


def correlation_context(value: Union[str, UUID]) -> _CorrelationContext:
    """Scope a correlation_id to a `with` block. Restores prior value on exit."""
    return _CorrelationContext(value)


_RESERVED_LOG_RECORD_KEYS = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "asctime", "message", "taskName",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        cid = _correlation_id.get()
        if cid:
            payload["correlation_id"] = cid

        for key, value in record.__dict__.items():
            if key in _RESERVED_LOG_RECORD_KEYS or key.startswith("_"):
                continue
            try:
                json.dumps(value)
                payload[key] = value
            except (TypeError, ValueError):
                payload[key] = repr(value)

        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: int = logging.INFO) -> None:
    """Install JSON formatting on the root logger. Idempotent — safe to call
    from app startup, Celery worker init, and pytest fixtures."""
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)


_AUDIT_INSERT_SQL = text("""
    INSERT INTO audit_events
        (interaction_id, correlation_id, customer_id, campaign_id,
         job_id, stage, event_type, severity, details)
    VALUES (:interaction_id, :correlation_id, :customer_id, :campaign_id,
            :job_id, :stage, :event_type, :severity, cast(:details as jsonb))
""")


async def write_audit_event(
    session: AsyncSession,
    *,
    interaction_id: Union[str, UUID],
    event_type: str,
    severity: str = "info",
    customer_id: Optional[Union[str, UUID]] = None,
    campaign_id: Optional[Union[str, UUID]] = None,
    job_id: Optional[Union[str, UUID]] = None,
    stage: Optional[str] = None,
    correlation_id: Optional[Union[str, UUID]] = None,
    **details: Any,
) -> None:
    cid = correlation_id or _correlation_id.get() or str(interaction_id)
    await session.execute(
        _AUDIT_INSERT_SQL,
        {
            "interaction_id": str(interaction_id),
            "correlation_id": str(cid),
            "customer_id": str(customer_id) if customer_id else None,
            "campaign_id": str(campaign_id) if campaign_id else None,
            "job_id": str(job_id) if job_id else None,
            "stage": stage,
            "event_type": event_type,
            "severity": severity,
            "details": json.dumps(details, default=str),
        },
    )
    logger = logging.getLogger("audit")
    method = {
        "info": logger.info,
        "warn": logger.warning,
        "warning": logger.warning,
        "error": logger.error,
    }.get(severity.lower(), logger.info)
    method(
        event_type,
        extra={
            "interaction_id": str(interaction_id),
            "correlation_id": str(cid),
            "customer_id": str(customer_id) if customer_id else None,
            "campaign_id": str(campaign_id) if campaign_id else None,
            "job_id": str(job_id) if job_id else None,
            "stage": stage,
            "details": details,
        },
    )
