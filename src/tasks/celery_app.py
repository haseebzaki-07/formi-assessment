from celery import Celery
from celery.schedules import schedule
from kombu import Queue

from src.config import settings

celery_app = Celery(
    "voicebot",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    # Task imports — register modules so the workers can resolve task names.
    # Phase 4 adds the recording_poller module.
    include=[
        "src.tasks.celery_tasks",
        "src.tasks.recording_poller",
    ],
)

# Phase 3: register hot / cold analysis queues alongside the lane-agnostic
# default and the recording-poll queue. Workers consume specific queues via
# `-Q postcall_hot,postcall_cold,...`; operators can scale lanes
# independently. Declaring the queues here avoids an auto-create lookup
# on first dispatch.
celery_app.conf.task_queues = (
    Queue(settings.POSTCALL_CELERY_QUEUE, routing_key=settings.POSTCALL_CELERY_QUEUE),
    Queue(settings.POSTCALL_HOT_QUEUE, routing_key=settings.POSTCALL_HOT_QUEUE),
    Queue(settings.POSTCALL_COLD_QUEUE, routing_key=settings.POSTCALL_COLD_QUEUE),
    Queue(settings.RECORDING_POLL_QUEUE, routing_key=settings.RECORDING_POLL_QUEUE),
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_default_queue=settings.POSTCALL_CELERY_QUEUE,
)

# Beat schedule. Run with `celery -A src.tasks.celery_app beat`.
# The reconciler keeps the recording lane responsive: it picks up rows
# stuck in `leased` past their lease (or past RECORDING_STUCK_AFTER_SECONDS)
# and resets them to pending so a fresh poller delivery re-runs the loop.
celery_app.conf.beat_schedule = {
    "reconcile-recording-jobs": {
        "task": "reconcile_recording_jobs",
        "schedule": schedule(
            run_every=settings.RECORDING_RECONCILE_INTERVAL_SECONDS,
        ),
        "options": {"queue": settings.RECORDING_POLL_QUEUE},
    },
    "reap-stale-leases": {
        "task": "reap_stale_leases",
        "schedule": schedule(run_every=30),
        "options": {"queue": settings.POSTCALL_CELERY_QUEUE},
    },
}
