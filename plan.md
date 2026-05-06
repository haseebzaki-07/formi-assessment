# Implementation Plan — Post-Call Processing Pipeline

This document sets out the phased implementation of the redesigned pipeline. Each phase is intended to land as one atomic commit (or a tight series of commits) with passing tests at the boundary, so the git history is readable and any phase can be rolled back independently.

## Context

The repository ships a deliberately broken post-call processing pipeline: FastAPI receives a webhook on call-end, schedules a single Celery task that blocks 45s waiting for a recording, then runs full LLM analysis on every transcript with no rate-limit awareness, no per-customer accounting, no durable retry, and a binary 1800s circuit breaker that freezes outbound dialling whenever LLM utilisation hits 90%. At ~100K calls per campaign across multiple concurrent customers, this fails in cascade.

The redesign treats the pipeline as a sequence of durable, leasable, idempotent jobs persisted in Postgres, with rate-limit reservations bound to those jobs, hot/cold lanes driven by a cheap classifier, a real recording poller, and continuous backpressure replacing the binary freeze. The deliverables are this plan, a `SUBMISSION.md` design document, and the implementation itself satisfying acceptance criteria AC1–AC10 from the brief.

## Scope boundary

In scope: every "Must implement" item from the brief, every "Should implement" item, and the design document. Out of scope (deferred to the "Known Weaknesses" section of `SUBMISSION.md`): encryption at rest, CRM push with retry, per-customer no-deploy config UI. These are "Nice to have" items in the brief and would inflate scope without changing AC pass/fail.

## Cross-cutting principles

- **Idempotency key = `processing_jobs.id`.** A duplicate Celery delivery, a redelivery after worker crash, or an end-user retry of the webhook all converge on the same row. The conditional `UPDATE … WHERE lease_token = $1` makes a stale completion a no-op.
- **Correlation ID = `interaction_id`.** Threaded through every log call via the `correlation_context` contextvar in `src/utils/logging.py`. Every audit event carries it as a queryable column.
- **Postgres is the durable substrate.** Redis is for fast counters (rate limits) and the Celery broker. No durability guarantee depends on Redis surviving a restart.
- **Replaced files are deleted in their replacement's phase, not preserved as deprecated stubs.** A final dead-code sweep happens in Phase 6.
- **Each phase ends with a passing test suite.** No phase is "complete" until `pytest tests/` runs green against the committed schema.

---

## Phase 0 — Foundation

**Goal:** establish the durable substrate the rest of the implementation builds on, and commit the design doc's first three sections so subsequent phases have a place to attach detail.

- Draft `SUBMISSION.md` sections 1 (Assumptions), 2 (Problem Diagnosis), 3 (Architecture Overview with diagram), and 8 (Data Model). Sections 4–7 and 9–15 land in their corresponding implementation phases.
- Extend `data/schema.sql` with `processing_jobs` (per-interaction, per-stage, lease columns, state machine, unique on `(interaction_id, stage)`) and `audit_events` (append-only, indexed by `interaction_id` + `correlation_id`). Indexes: partial `WHERE state='pending'` for the lease-next query, partial `WHERE state='leased'` for the stale-lease reaper, `(customer_id, state)` for queue depth.
- Add `src/utils/logging.py`: `JsonFormatter`, `correlation_context(...)` contextvar, `write_audit_event(...)` that participates in the caller's transaction (so the audit row and the state change land atomically).
- Wire a real async SQLAlchemy session into `src/api/endpoints.py`. `_load_interaction` becomes a real `SELECT` against the `interactions` table; `_update_interaction_status` becomes a real `UPDATE`.
- Delete the duplicate empty-payload `signal_jobs` / `update_lead_stage` fire-and-forget calls from the long-transcript branch of the endpoint. Both will become durable stages owned by the worker pipeline in Phase 1.

**Files touched:** `data/schema.sql`, `src/utils/logging.py` (new), `src/api/endpoints.py`, `SUBMISSION.md` (new from `SUBMISSION_TEMPLATE.md`).

**Validates:** the foundation for AC5 (audit trail) and AC6 (structured logging with correlation ID).

---

## Phase 1 — Durable execution + recording fan-out

**Goal:** make the pipeline survive worker crashes, infrastructure restarts, and duplicate deliveries without losing or double-processing work. Decouple recording from analysis so the two run in parallel.

- Add `src/services/job_store.py` as the repository for `processing_jobs`: `create_pipeline`, `lease_job` (specific row, atomic conditional UPDATE with new lease_token), `lease_next` (queue-style, `FOR UPDATE SKIP LOCKED`), `complete_job` (transitions row + unblocks dependents in the same transaction), `fail_job` (retry vs. dead based on attempt count), `mark_skipped`, `reap_stale_leases`.
- Replace the monolithic `process_interaction_end_background_task` in `src/tasks/celery_tasks.py` with a `run_stage(job_id, stage)` dispatcher. The worker leases the row, executes the stage handler, and either completes (auto-dispatching newly-unblocked dependents found via `depends_on_job_id`) or fails (with retry/dead state-machine semantics). Handlers: `_do_recording`, `_do_analysis` (with defensive short-transcript skip — AC8 enforced inside the worker, not just at the API), `_do_signal_jobs` (reads parent's result via `depends_on_job_id`), `_do_lead_stage`.
- Modify `src/services/recording.py`: kill the `asyncio.sleep(45)`. The fetch is now single-shot per call and raises `RecordingNotReady` on 404 (which surfaces as a job retry through the state machine) or `RecordingFetchError` on transport / auth errors. No more silent skips.
- Modify `src/api/endpoints.py` to use the outbox pattern: `create_pipeline` writes the rows in the same transaction as the `pipeline_created` audit event, then dispatches the entry-point stages via Celery. A crash anywhere downstream is recoverable because the rows already exist in Postgres.
- Delete `src/services/retry_queue.py`. The `processing_jobs` state machine replaces it (retry attempts on the row, `dead` as a terminal preserved-payload state).
- Add `tests/test_durable_execution.py` covering AC3 (worker kill mid-task → another worker resumes; original lease_token is invalid for late completion), AC8 at both pipeline-creation and worker levels, partial AC5 (audit_events round-trip), retry-until-dead, dependent unblocking on completion, stale-lease reaping.

**Files touched:** `src/services/job_store.py` (new), `src/tasks/celery_tasks.py`, `src/services/recording.py`, `src/api/endpoints.py`, `tests/test_durable_execution.py` (new). `src/services/retry_queue.py` deleted.

**Validates:** AC3 (no permanent loss across worker restart), AC8 (short-transcript skip enforced everywhere), partial AC5 (audit trail entries written for each transition), partial AC4 (recording failures are observable; the retry/backoff loop itself lands in Phase 4).

---

## Phase 2 — Rate limiter + per-customer budgets

**Goal:** never surface a 429 from the LLM provider to the API caller. Allocate token capacity fairly across concurrent customers. Make every token attributable for billing.

- Add `src/services/rate_limiter.py`: a Redis Lua script implementing a token bucket keyed by `(customer_id, window)`. Two windows — 1-second smoothing and 1-minute headline (TPM/RPM). Atomic check-and-decrement; the script returns either a granted reservation (with token amount and expiry) or a rejection (with retry-after hint).
- Add `src/services/budget.py`: per-customer hard quota loaded from a new `customer_budgets` table (priority weight, reserved TPM/RPM, max burst). Unallocated headroom is shared via priority-weighted draws from a global "spillover" bucket; a customer at quota cannot consume another customer's reservation (AC2).
- Modify `src/services/post_call_processor.py` to wrap the LLM call with a pre-flight reservation: estimate token cost from transcript length (`chars/4 + fixed overhead`), reserve, call the LLM, true-up the actual `usage.total_tokens` against the reservation. **The reservation lease is bound to `processing_jobs.id`** so a worker crash mid-call lets the lease expire and the budget recovers without leak.
- Add `token_usage` ledger table: append-only rows of `(interaction_id, customer_id, campaign_id, tokens_estimated, tokens_actual, ts)`. Source of truth for billing.
- A 429 from the provider is a soft signal: back off this customer's bucket (multiplicative decrease) and re-queue the job with a delayed `scheduled_at`. Never propagate the 429 to the API caller (AC1).
- Use `LLM_TOKENS_PER_MINUTE` and `LLM_REQUESTS_PER_MINUTE` as actual gates (currently they are decorative in `src/config.py`).
- Add `tests/test_rate_limiter.py` validating AC1 (1000-call burst with no 429 surfaced), AC2 (Customer A exhausts quota → Customer B still processes), and lease recovery (AC3 reinforcement).
- Write `SUBMISSION.md` sections 4 (Rate Limit Management) and 5 (Per-Customer Token Budgeting).

**Files touched:** `src/services/rate_limiter.py` (new), `src/services/budget.py` (new), `src/services/post_call_processor.py`, `src/config.py`, `data/schema.sql` (`customer_budgets` and `token_usage` tables), `tests/test_rate_limiter.py` (new), `SUBMISSION.md`.

**Validates:** AC1, AC2, partial AC9 (rate-limit strategy is defended in the design doc).

---

## Phase 3 — Differentiated processing (hot/cold lanes)

**Goal:** spend full LLM quota on calls that drive business value; defer or short-circuit calls that don't. The fixture file already encodes the expected `hot`/`cold`/`skip` lanes per transcript outcome — the classifier's job is to predict that lane cheaply before the full LLM call.

- Add `src/services/triage.py`: a two-stage classifier. Stage A is keyword/regex matching on the transcript (`"wrong number"` → skip, `"not interested"` → cold, `"book"` / `"confirmed"` → hot). Stage B falls back to a small/cheap LLM for ambiguous transcripts where Stage A returns low confidence (the `hinglish_ambiguous` fixture is the canonical case).
- Set the `lane` and `priority` columns on `processing_jobs` rows at pipeline-creation time based on the triage output. Hot lane → priority 1; cold lane → priority 9, `scheduled_at` pushed to a low-utilisation window.
- Add a `customer_overrides` JSONB column to `customer_budgets` so a customer can opt specific outcomes from cold→hot or vice versa without a deployment.
- Triage output drives the rate limiter's reservation size: a confirmed rebook gets a full reservation (~1500 tokens); a cold-lane batched summary gets a smaller one (~500 tokens with a cheaper prompt variant).
- Register hot and cold queues in `src/tasks/celery_app.py`; route based on the row's `lane` column at dispatch time. The `run_stage` task definition stays the same — only the `queue=` argument to `apply_async` changes.
- Tests: `tests/test_triage.py` covering all eight fixture outcomes against their `expected_lane`. Add an integration test where a hot call queues behind a backlog of cold calls and is processed first.
- Write `SUBMISSION.md` section 6 (Differentiated Processing).

**Files touched:** `src/services/triage.py` (new), `src/services/job_store.py` (lane/priority columns set), `src/tasks/celery_app.py`, `data/schema.sql` (`customer_overrides` column), `tests/test_triage.py` (new), `SUBMISSION.md`.

**Validates:** AC8 (short calls never reach the LLM — confirmed via the `skip` lane), the "Should implement" differentiated-processing requirement.

---

## Phase 4 — Recording poller + alerts

**Goal:** replace the per-job-retry behaviour from Phase 1 with a single long-lived polling stage that does its own exponential backoff inside the job. Make every recording failure observable.

- Add `src/tasks/recording_poller.py`: a worker that leases a recording row and polls Exotel with exponential backoff (5s → 10s → 20s → 40s → 80s, 5 attempts max, jittered). On every state transition, write an `audit_events` row keyed by `interaction_id`. On terminal failure, write a row with `severity='error'` and `event_type='recording_failed_terminal'` so a metrics scrape can alert on it.
- Add a reconciliation job (Celery beat, every 60s): scans for `recording_jobs` rows in `uploading` state older than 5 minutes and resumes them. Catches the "uploaded to S3 but DB write failed" gap.
- Alert thresholds expressed as documented log-event types: `recording_failure_rate_warn` (>5% per customer per hour), `recording_failure_rate_page` (>20%). The alerting infrastructure itself is out of scope; the design doc states what events would feed it.
- Tests: `tests/test_recording_poller.py` simulates a delayed recording (Exotel returns 404 for the first three polls, then 200), asserts the backoff sequence, asserts every poll attempt produces an audit event, asserts a permanent failure produces the terminal event.
- Write `SUBMISSION.md` section 7 (Recording Pipeline).

**Files touched:** `src/services/recording.py` (factor the fetch into a poll-friendly shape), `src/tasks/recording_poller.py` (new), `src/tasks/celery_app.py` (beat schedule), `tests/test_recording_poller.py` (new), `SUBMISSION.md`.

**Validates:** AC4 (retry/backoff, no silent skips), AC6 (every error path emits a structured log with `interaction_id`).

---

## Phase 5 — Backpressure replacement

**Goal:** replace the binary 1800s dialler freeze with a continuous capacity signal that the dialler consumes to dispatch fewer calls when LLM capacity is constrained.

- Add `src/services/backpressure.py`: exposes `current_utilization()` returning a float in `[0, 1]` computed as `max(global_tpm_used / global_tpm_cap, global_rpm_used / global_rpm_cap)` from the rate-limiter's Redis counters.
- Add `dialler_dispatch_probability(utilization)`: a smooth curve (e.g., `1 − sigmoid((utilization − 0.7) × 10)`) so the dialler dispatches at full speed below 50% utilisation, ramps down between 70–90%, and sheds aggressively above 90%. No binary freeze.
- Per-customer queue-depth metrics published via a `/metrics` endpoint (Prometheus-format) — read from `processing_jobs` grouped by `(customer_id, state)`.
- Alert thresholds: 75% utilisation (warn), 90% (page), queue-depth growth-rate > 1000/min (page), reaped-stale-leases > 100/hour (warn — indicates persistent worker churn).
- Delete `src/services/circuit_breaker.py` (the binary version) — replaced by `src/services/backpressure.py`. Update `src/services/post_call_processor.py` to no longer call `record_postcall_start` / `record_postcall_end`; the rate limiter's counters from Phase 2 are the source of truth.
- Tests: `tests/test_backpressure.py` exercises the curve at the inflection points, asserts utilisation reads from Redis correctly, asserts the metrics endpoint output shape.
- Update `SUBMISSION.md` section 4 (Rate Limit Management) with the dialler-coordination details that depend on this phase.

**Files touched:** `src/services/backpressure.py` (new), `src/services/post_call_processor.py`, `src/api/endpoints.py` (or new `src/api/metrics.py`) for `/metrics`, `tests/test_backpressure.py` (new), `SUBMISSION.md`. `src/services/circuit_breaker.py` deleted.

**Validates:** AC7 (no hardcoded freeze; backpressure proportional).

---

## Phase 6 — Tests + design doc finalization

**Goal:** end-to-end load test, AC-aligned test coverage, dead-code sweep, and the design doc's remaining sections.

- Add `tests/test_burst_load.py`: simulate 1000 concurrent webhook calls; assert no 429 surfaced to any caller; assert TPM/RPM are never exceeded for more than 1 second; assert all interactions reach a terminal state (`completed`, `dead`, or `skipped`); assert per-customer token-usage rows sum correctly.
- Add `tests/test_acceptance_criteria.py`: one test per AC1–AC10, each test names the AC in its docstring so the rubric can be grep'd.
- Replace `tests/test_post_call.py` (which currently documents the broken behaviour) with `tests/test_post_call_pipeline.py` covering the new pipeline end to end.
- Dead-code sweep: any file marked deprecated in earlier phases gets deleted now if it has no remaining references.
- Finalise `SUBMISSION.md`:
  - Section 9 (Auditability & Observability) — the canonical query patterns against `audit_events` for debugging a specific interaction 3 days after the fact.
  - Section 10 (Data Model) — link to `data/schema.sql` and call out the migration path.
  - Section 11 (Security) — identify transcript / lead PII / call recordings as sensitive; state encryption-at-rest as a deferred item with the hooks needed to add it.
  - Section 12 (API Interface) — document the dropped premature `signal_jobs` call from Phase 0 and the durable-pipeline contract the endpoint now offers.
  - Section 13 (Trade-offs) — table form: every alternative considered (e.g., Kafka over Postgres-as-queue, sidecar rate-limiter over Redis Lua) with the rejection reason.
  - Section 14 (Known Weaknesses) — including the deferred nice-to-haves.
  - Section 15 (What I Would Do With More Time) — a prioritised list, not a wishlist.

**Files touched:** `tests/` (multiple new files, `test_post_call.py` replaced), `SUBMISSION.md` (final pass), file deletions as warranted.

**Validates:** AC1–AC10 in aggregate; AC9 (assumptions stated and trade-offs defended); AC10 (security strategy stated).

---

## Verification (per phase, run after each)

```bash
docker-compose down -v && docker-compose up -d        # if pgdata is stale, re-init the schema
pip install -r requirements.txt                        # idempotent
pytest tests/ -v                                       # all phases must keep this green at boundary
celery -A src.tasks.celery_app worker --loglevel=info  # worker for Phases 1+
uvicorn src.app:app --reload                           # API for endpoint smoke tests
```

End-to-end after Phase 6: simulate a burst of 1000 webhook calls, kill a worker mid-burst, assert all 1000 interactions reach `completed` / `dead` / `skipped` (none stuck in `pending` / `leased`), assert per-customer `token_usage` ledger sums match billing expectations, assert at least one recording-poller failure event fired and was logged with its `interaction_id`.

## Critical files (cumulative)

- `data/schema.sql` — extended in Phase 0 (processing_jobs, audit_events), Phase 2 (customer_budgets, token_usage), Phase 3 (lane/priority columns).
- `src/utils/logging.py` — Phase 0.
- `src/services/job_store.py` — Phase 1.
- `src/services/rate_limiter.py`, `src/services/budget.py` — Phase 2.
- `src/services/triage.py` — Phase 3.
- `src/tasks/recording_poller.py` — Phase 4.
- `src/services/backpressure.py` — Phase 5; `src/services/circuit_breaker.py` deleted.
- `src/api/endpoints.py` — touched in Phases 0, 1, 5.
- `src/tasks/celery_tasks.py` — rewritten in Phase 1, routing changes in Phase 3.
- `src/services/post_call_processor.py` — rate-limiter integration in Phase 2, circuit-breaker removal in Phase 5.
- `src/services/recording.py` — sleep killed in Phase 1, poller-friendly shape in Phase 4.
- `SUBMISSION.md` — written incrementally across all phases, finalised in Phase 6.
- `src/services/retry_queue.py` — deleted in Phase 1.
