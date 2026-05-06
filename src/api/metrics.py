"""
Prometheus-format /metrics endpoint.

Exposes the signals the dialler and on-call dashboards need:
  - llm_utilization                  — current pool utilisation (0..1+)
  - dialler_dispatch_probability     — what the smooth-shed curve says now
  - llm_tokens_used_minute           — raw aggregate TPM (current minute)
  - llm_requests_used_minute         — raw aggregate RPM (current minute)
  - postcall_jobs                    — gauge of processing_jobs by
                                       (customer_id, state). The cardinality
                                       is bounded by customers × states,
                                       which stays small at any realistic
                                       scale.

We render Prometheus exposition text by hand (a few lines of f-strings)
rather than pulling in `prometheus_client` — the dependency footprint
isn't worth it for a handful of gauges, and the format is stable.
"""

from __future__ import annotations

from fastapi import APIRouter, Response
from sqlalchemy import text

from src.config import settings
from src.services import backpressure, rate_limiter
from src.utils.db import async_session_factory


router = APIRouter()


_PROM_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def _fmt_labels(labels: dict[str, str]) -> str:
    if not labels:
        return ""
    inner = ",".join(
        f'{k}="{_escape(v)}"' for k, v in sorted(labels.items())
    )
    return "{" + inner + "}"


def _escape(value: str) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )


@router.get("/metrics")
async def metrics() -> Response:
    """
    Prometheus scrape target. No auth on this endpoint by design — runs
    behind the cluster's network policy. Exposing it externally would
    leak per-customer queue depth, which is mildly sensitive.
    """
    utilization = await backpressure.current_utilization()
    dispatch_prob = backpressure.dialler_dispatch_probability(utilization)
    used_tpm, used_rpm = await rate_limiter.get_global_usage()

    lines: list[str] = []

    lines.append("# HELP llm_utilization Current LLM pool utilisation (max of TPM and RPM ratios).")
    lines.append("# TYPE llm_utilization gauge")
    lines.append(f"llm_utilization {utilization:.6f}")

    lines.append(
        "# HELP dialler_dispatch_probability Probability the dialler should use to gate "
        "outbound dispatch decisions (continuous shed curve)."
    )
    lines.append("# TYPE dialler_dispatch_probability gauge")
    lines.append(f"dialler_dispatch_probability {dispatch_prob:.6f}")

    lines.append("# HELP llm_tokens_used_minute Tokens reserved across all customers in the current minute window.")
    lines.append("# TYPE llm_tokens_used_minute gauge")
    lines.append(f"llm_tokens_used_minute {used_tpm}")

    lines.append("# HELP llm_requests_used_minute Requests reserved across all customers in the current minute window.")
    lines.append("# TYPE llm_requests_used_minute gauge")
    lines.append(f"llm_requests_used_minute {used_rpm}")

    lines.append("# HELP llm_tokens_per_minute_cap Configured global TPM cap.")
    lines.append("# TYPE llm_tokens_per_minute_cap gauge")
    lines.append(f"llm_tokens_per_minute_cap {settings.LLM_TOKENS_PER_MINUTE}")

    lines.append("# HELP llm_requests_per_minute_cap Configured global RPM cap.")
    lines.append("# TYPE llm_requests_per_minute_cap gauge")
    lines.append(f"llm_requests_per_minute_cap {settings.LLM_REQUESTS_PER_MINUTE}")

    # Per-customer queue depth. Bounded cardinality (customers × states).
    rows = await _read_queue_depth()
    lines.append("# HELP postcall_jobs Count of processing_jobs rows by customer and state.")
    lines.append("# TYPE postcall_jobs gauge")
    for row in rows:
        labels = _fmt_labels({
            "customer_id": str(row["customer_id"]),
            "state": str(row["state"]),
        })
        lines.append(f"postcall_jobs{labels} {row['count']}")

    body = "\n".join(lines) + "\n"
    return Response(content=body, media_type=_PROM_CONTENT_TYPE)


async def _read_queue_depth() -> list[dict]:
    """
    Aggregate processing_jobs by (customer_id, state). The
    idx_processing_jobs_customer_state index covers this exact query.
    Returns [] on any DB error so a Postgres blip can't take the
    metrics endpoint down — utilisation and dispatch probability remain
    available, which is the crucial path for backpressure.
    """
    try:
        async with async_session_factory() as session:
            result = await session.execute(
                text("""
                    SELECT customer_id, state, COUNT(*) AS count
                    FROM processing_jobs
                    GROUP BY customer_id, state
                """)
            )
            return [
                {
                    "customer_id": r.customer_id,
                    "state": r.state,
                    "count": int(r.count),
                }
                for r in result.all()
            ]
    except Exception:
        return []
