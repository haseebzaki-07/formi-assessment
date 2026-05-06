from fastapi import FastAPI

from src.api.endpoints import router
from src.api.metrics import router as metrics_router

app = FastAPI(
    title="VoiceBot Post-Call Processing",
    version="1.0.0",
)

app.include_router(router, prefix="/api/v1")
# Phase 5: Prometheus scrape target. Mounted at the root (not under
# /api/v1) so standard Prometheus configs find it without a path prefix.
app.include_router(metrics_router)
