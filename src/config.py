import os


class Settings:
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/voicebot"
    )
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/1")
    CELERY_RESULT_BACKEND: str = os.getenv(
        "CELERY_RESULT_BACKEND", "redis://localhost:6379/2"
    )

    # ── LLM ───────────────────────────────────────────────────────────────────
    # One provider, one model, one key. Everyone shares it.
    # These limits come straight from the provider's dashboard — they are HARD
    # limits that result in 429 errors when exceeded, not soft suggestions.
    #
    # At 100K calls/campaign: if even 10% hit the LLM concurrently that's
    # 10,000 requests fighting for 500 slots/min. You do the math.
    #
    # Worth noting: LLM_TOKENS_PER_MINUTE and LLM_REQUESTS_PER_MINUTE are
    # defined here but grep the codebase — nothing actually reads them before
    # firing a request. They exist as documentation, not enforcement.
    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "openai")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "gpt-4o")
    LLM_API_KEY: str = os.getenv("LLM_API_KEY", "sk-mock-key-for-assessment")
    LLM_TOKENS_PER_MINUTE: int = int(os.getenv("LLM_TOKENS_PER_MINUTE", "90000"))
    LLM_REQUESTS_PER_MINUTE: int = int(os.getenv("LLM_REQUESTS_PER_MINUTE", "500"))

    # Average tokens consumed per post-call analysis (measured from prod logs).
    # Useful if you're trying to estimate how many calls can be processed per
    # minute before hitting LLM_TOKENS_PER_MINUTE.
    LLM_AVG_TOKENS_PER_CALL: int = int(os.getenv("LLM_AVG_TOKENS_PER_CALL", "1500"))

    # ── Recording ─────────────────────────────────────────────────────────────
    # Why 45 seconds? Someone measured the average Exotel delivery time once,
    # added a buffer, and hardcoded it. That was on a quiet Friday afternoon.
    # Under load the delivery window is 10s–120s with no guarantee.
    RECORDING_WAIT_SECONDS: int = 45
    S3_BUCKET: str = os.getenv("S3_BUCKET", "voicebot-recordings")

    # ── Backpressure (Phase 5) ────────────────────────────────────────────────
    # The previous binary circuit breaker (90% utilisation → 1800s freeze of
    # the dialler) was deleted in Phase 5. It is replaced by a continuous
    # signal in src/services/backpressure.py: dialler_dispatch_probability
    # is a smooth shed curve over current LLM utilisation. There is no
    # freeze duration to misjudge — utilisation is read fresh on every
    # dispatch tick and dispatch probability tracks it directly.
    #
    # Alert thresholds (consumed by /metrics scrape, not by the app itself):
    #   utilization >= 0.75 → warn
    #   utilization >= 0.90 → page
    BACKPRESSURE_WARN_UTILIZATION: float = 0.75
    BACKPRESSURE_PAGE_UTILIZATION: float = 0.90

    # ── Post-call processing ──────────────────────────────────────────────────
    # Single queue. Everything goes here. A "not interested" 10-second call
    # and a confirmed rebook sit in the same line at the same priority.
    POSTCALL_CELERY_QUEUE: str = "postcall_processing"
    POSTCALL_MAX_RETRIES: int = 3
    POSTCALL_RETRY_DELAY: int = 60  # Fixed delay — not exponential backoff

    # ── Phase 3: Hot / cold lanes ─────────────────────────────────────────────
    # Triage routes work into two analysis queues. Operators size workers
    # per queue: more hot workers for tight latency, fewer cold workers
    # tolerating backlog. The default queue (POSTCALL_CELERY_QUEUE above)
    # is retained as the destination for lane-agnostic stages (signal_jobs
    # and lead_stage) so they don't compete with analysis for slots.
    POSTCALL_HOT_QUEUE: str = "postcall_hot"
    POSTCALL_COLD_QUEUE: str = "postcall_cold"
    # Cold-lane analysis is deferred to a low-utilisation window. Default
    # zero means "process as soon as a cold worker is free"; raise this to
    # actively batch cold work into off-peak periods.
    COLD_LANE_DEFER_SECONDS: int = int(os.getenv("COLD_LANE_DEFER_SECONDS", "0"))
    # Reservation sizes used as a hint by the analysis stage (Phase 2's
    # rate limiter consumes these when wiring up). Hot lane gets the full
    # prompt / reservation; cold lane uses a cheaper summary-only prompt.
    HOT_LANE_RESERVATION_TOKENS: int = int(
        os.getenv("HOT_LANE_RESERVATION_TOKENS", "1500")
    )
    COLD_LANE_RESERVATION_TOKENS: int = int(
        os.getenv("COLD_LANE_RESERVATION_TOKENS", "500")
    )

    # ── Phase 4: Recording poller ─────────────────────────────────────────────
    # The recording stage runs as a long-lived in-job poll loop instead of
    # per-attempt Celery redeliveries. Backoff is jittered exponential — see
    # src/tasks/recording_poller.py for the schedule.
    RECORDING_POLL_QUEUE: str = "recording_poll"
    # Backoff schedule in seconds. Each entry is the delay BEFORE the next
    # poll. Total wall-clock budget = sum of all entries (default ≈ 155s,
    # which exceeds the 120s tail of Exotel's normal delivery window).
    RECORDING_POLL_BACKOFF_SCHEDULE: tuple = (5, 10, 20, 40, 80)
    # Jitter as a fraction of the scheduled delay. 0.2 = ±20%.
    RECORDING_POLL_JITTER: float = 0.2
    # Reconciliation beat: scan for stuck recording rows every 60s.
    RECORDING_RECONCILE_INTERVAL_SECONDS: int = 60
    # A leased recording row older than this is considered stuck (worker
    # crash mid-upload) — the reconciler resets state to pending.
    RECORDING_STUCK_AFTER_SECONDS: int = 300


settings = Settings()
