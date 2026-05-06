# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A take-home assessment: a **deliberately flawed** post-call processing pipeline for a voice AI platform handling ~100K calls per campaign. The candidate must (a) write a design document at `SUBMISSION.md` (start from `SUBMISSION_TEMPLATE.md`) and (b) implement the highest-impact fixes. The full brief is in `README.md`; the rubric (AC1–AC10, evaluation weights) lives there too.

The inline comments in `src/` describe known problems on purpose — they are hints, not noise. Treat them as part of the spec. The "Known Failure Modes" section of `README.md` enumerates the most severe ones.

## Commands

```bash
docker-compose up -d                 # Postgres (5432) + Redis (6379). Schema auto-loads from data/schema.sql.
pip install -r requirements.txt
pytest tests/ -v                     # asyncio_mode=auto is set in pyproject.toml
pytest tests/test_post_call.py::test_every_call_gets_full_llm_analysis -v   # single test
uvicorn src.app:app --reload         # run the API
celery -A src.tasks.celery_app worker -Q postcall_processing --loglevel=info   # run the worker
```

There is no linter or formatter configured. The existing tests in `tests/test_post_call.py` document the *current broken* behaviour — the README explicitly says the candidate's solution should make them obsolete and replace them.

## Implementation status

Work is being done in phases (see plan file). **Phases 1–6 have landed; the implementation is complete against the brief.** The sections below describe the *original* broken architecture for context; the bullets here describe what's actually in the tree right now.

- `processing_jobs` and `audit_events` tables exist (in `data/schema.sql`); they're the durable source of truth for pipeline state, not the Celery broker.
- `src/services/job_store.py` is the repository layer (`create_pipeline`, `lease_job`, `lease_next`, `complete_job`, `fail_job`, `defer_job`, `mark_skipped`, `reap_stale_leases`). Idempotency is on `(interaction_id, stage)`; the conditional UPDATE with `lease_token` makes late completions from crashed workers a no-op.
- `src/tasks/celery_tasks.py` is one `run_stage` dispatcher that leases the row, executes the stage handler, and either completes (auto-dispatching newly-unblocked dependents) or fails (with retry/dead state-machine semantics). The old monolithic `process_interaction_end_background_task` is gone.
- `src/services/recording.py` is poll-friendly; recording is now a long-lived poller stage in `src/tasks/recording_poller.py` with jittered exponential backoff.
- `src/services/retry_queue.py` is deleted. The processing_jobs state machine replaces it.
- `src/utils/logging.py` provides `correlation_context(...)` (auto-injects `interaction_id` into every log call inside the block) and `write_audit_event(...)` (writes a row to `audit_events` in the caller's transaction).
- `src/api/endpoints.py` no longer fires duplicate empty-payload signal_jobs / update_lead_stage. Both are now durable stages on the worker pipeline.
- `src/services/rate_limiter.py` + `src/services/budget.py` (Phase 2): atomic Redis-Lua reservations with per-customer reserved + global spillover, multiplicative-decrease 429 backoff, append-only `token_usage` ledger.
- `src/services/triage.py` (Phase 3): keyword/regex stage A + cheap-LLM stage B; the `lane` and `priority` columns on `processing_jobs` route work to `postcall_hot`, `postcall_cold`, or the default queue.
- `src/services/backpressure.py` (Phase 5): continuous `current_utilization()` from the rate-limiter's global aggregate counters; smooth `dialler_dispatch_probability` curve (1 − sigmoid centred at 0.7 utilisation). `src/services/circuit_breaker.py` is **deleted** — the binary 1800s freeze is gone.
- `src/api/metrics.py` (Phase 5): Prometheus `/metrics` endpoint exposing `llm_utilization`, `dialler_dispatch_probability`, raw TPM/RPM aggregates, and per-`(customer_id, state)` queue depth.
- Phase 6: `tests/test_post_call_pipeline.py` (replaces the old `test_post_call.py` that documented broken behaviour), `tests/test_burst_load.py` (concurrent-pipeline invariants), `tests/test_acceptance_criteria.py` (one test per AC1–AC10 — grep-able rubric surface). `src/services/metrics.py` (the dead `PostCallMetricsTracker`) is **deleted**; signal_jobs docstrings refreshed. SUBMISSION.md sections 9–15 (auditability, data model migration, security, API interface, trade-offs, known weaknesses, what I'd do with more time) are written out.

The endpoint's `_load_interaction` / `_update_interaction_status` are wired to Postgres (Phase 0/1).

## Architecture (current, broken)

Webhook → FastAPI `BackgroundTask` → Celery (single queue `postcall_processing`) → sequential pipeline:

1. `recording.fetch_and_upload_recording` — `asyncio.sleep(45)` then one Exotel API call. Misses are silent.
2. `post_call_processor.PostCallProcessor.process_post_call` — full LLM analysis on every call, no rate-limit check, no per-customer accounting.
3. `signal_jobs.trigger_signal_jobs` and `update_lead_stage` — fire-and-forget, also fired *prematurely* from the endpoint with `analysis_result={}` for long transcripts (downstream gets duplicated/empty payloads).

Side-channel state in Redis:
- `llm:postcall:rpm` — RPM counter incremented in `circuit_breaker.record_postcall_start` (post-decision, so it's a measurement, not a gate). 90% trips a 1800s **per-agent** dialler freeze.
- `postcall:retry_queue` + `postcall:retry_state:{id}` — secondary retry list whose durability is identical to the primary Celery broker (both Redis), and whose `dequeue_ready` is non-atomic so duplicate processing is possible.

Persistence is Postgres with three tables (`leads`, `sessions`, `interactions`). The `interactions.interaction_metadata` JSONB is the dashboard's hot cache and the *only* place analysis results live — overwritten on retry. Schema in `data/schema.sql`; SQLAlchemy models in `src/models/` (note: endpoint and tasks currently use mock dict-based loads, not the ORM).

## Things that will save time

- `src/api/endpoints.py::_load_interaction` returns a hardcoded mock dict — wire-up to Postgres is intentionally absent. Don't go hunting for a missing query.
- `PostCallProcessor._call_llm` is a mock returning `{"usage": {"total_tokens": 1500}}`. The assignment says "no real API keys required to run tests."
- `LLM_TOKENS_PER_MINUTE` and `LLM_REQUESTS_PER_MINUTE` are defined in `config.py` but nothing reads them before firing — the comment in the file calls this out. Implementing rate-limit-aware scheduling that uses them is the core fix.
- `tests/fixtures/sample_transcripts.json` (loaded by `make_post_call_context` in `conftest.py`) is the canonical set of test cases for varied call outcomes. Use these as the basis for any classification/triage logic and for tests of differentiated processing.
- The brief explicitly permits changing the API contract and the data model — but requires the rationale be written into `SUBMISSION.md`.

## Required deliverables (from README)

Must implement: rate-limit-aware LLM scheduling, per-customer token budgets, recording poller w/ retry+backoff, durable task execution, structured audit logging keyed by `interaction_id`. Should/nice-to-have items are listed under "The Challenge" in `README.md`.

Constraints that the grader checks: no permanent loss of analysis results, no unhandled 429s under 100K-call burst, every token attributable to (customer, campaign, interaction), recording failures must be observable events, must run locally with `docker-compose up` and no real API keys.
