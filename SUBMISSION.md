# Post-Call Processing Pipeline — Design Document

**Author:** Haseeb Zaki
**Date:** 2026-05-06

---

## 1. Assumptions

These are the assumptions the design rests on. They are explicit so they can be challenged in the follow-up — the design doesn't pretend they are facts.

1. **Workload shape.** A campaign produces ~100K calls. Calls do not arrive uniformly — there is a peak window (typically the first 1–2 hours of a campaign run) where 30–50% of calls complete. Multiple customers can run campaigns concurrently; the platform does not control their scheduling.
2. **LLM contract.** The provider exposes hard rate limits as TPM (tokens-per-minute) and RPM (requests-per-minute). Exceeding either returns HTTP 429 with a `Retry-After` header. The provider's response body includes a `usage` block with the exact token count for billing-grade accuracy.
3. **Business value of call outcomes is not uniform.** Confirmed bookings (`rebook_confirmed`, `demo_booked`) and escalations are time-sensitive — sales acts on them within minutes. "Not interested", "callback requested later", and "already purchased" are useful for CRM hygiene but tolerate hour-scale latency. The fixture file (`tests/fixtures/sample_transcripts.json`) encodes this with an `expected_lane` field per outcome (`hot` / `cold` / `skip`); we treat that taxonomy as the ground truth for triage.
4. **Telephony provider behaviour.** Exotel makes the recording URL available via a polling endpoint; delivery time varies from 10s to 120s+ under load. The status endpoint is not rate-limited and is poll-friendly. A 404 on the recording endpoint means "not yet ready", not "will never be ready".
5. **Durability surfaces.** Postgres is the durable substrate. Redis is acceptable for fast counters (rate limits) and as the Celery broker, but no durability guarantee depends on Redis surviving a restart. S3 is durable for recording artefacts.
6. **Webhook delivery semantics.** The telephony provider's webhook delivery is at-least-once with a 5-second response timeout. The endpoint must respond within that window or risk a duplicate retry. Idempotency on `interaction_id` is mandatory.
7. **Operational model.** There is an on-call engineer. They must be able to debug a specific failed interaction 3 days after the fact using `interaction_id` as the only handle. Alerts fire to a paging system; structured-log scrapes feed a metrics dashboard.
8. **Sensitive data.** Transcripts contain PII (names, phone numbers, addresses spoken during the call). Recordings contain audio of the same. Lead records carry contact information. None of this is purely public.
9. **The dialler.** A separate service (out of this codebase's tree) decides when to dial. The post-call pipeline coordinates with the dialler via a single capacity signal, not by freezing it. The dialler is responsible for translating the signal into dispatch decisions.
10. **What "urgent" means in practice.** A `hot` outcome is processed end-to-end within 60 seconds of webhook receipt. A `cold` outcome is processed within 30 minutes. A `skip` outcome bypasses the LLM entirely. These are commitments the design optimises against.

---

## 2. Problem Diagnosis

The current system fails at scale because it has no shared, enforced awareness of LLM rate limits at the request level. Compounding factors turn that single gap into cascading infrastructure failure.

**Root cause.** Every long-transcript call fires a Celery task that calls the LLM unconditionally. At burst, the inflight request count exceeds the provider's RPM budget. The provider returns 429s. Celery retries each failure with a fixed 60-second delay. Retries pile up alongside fresh work in the same queue. The Redis broker's memory grows. The retry-queue and the Celery broker both live in Redis, so a Redis blip loses both. Meanwhile the only protective mechanism — the circuit breaker — observes the inflight count crossing 90% and freezes the dialler for 1800 seconds, stopping new calls from being made. The business impact is: zero new calls for 30 minutes, *and* the existing backlog drains slowly because retries are still hitting 429s.

**Compounding factors.**

- **Recording blocks analysis.** A hardcoded `asyncio.sleep(45)` in the worker stalls the LLM step for 45 seconds even when the recording is ready in 10s. When 100K calls each pay this tax, the campaign's analysis window is artificially compressed. Recordings that arrive after 45s are silently dropped — no log, no retry, no alert.
- **No per-customer accounting.** The token budget is global. Customer A running a 50K-call burst can starve Customer B's 5K-call campaign of LLM quota even though both are paying for capacity. There is no billable-tokens-per-customer ledger; "how many tokens did Customer X consume this hour?" is not answerable without grepping logs.
- **Triage is missing.** A "wrong number" 5-second call and a confirmed rebook get the same 1500-token full analysis. The fixture data clearly shows that outcome distribution is bimodal — cheap calls dominate, valuable calls are rare. The current system spends quota on the cheap calls, leaving none for the valuable ones during burst.
- **Failure semantics are weak.** Signal jobs and lead-stage updates are fired from `asyncio.create_task` inside FastAPI — fire-and-forget with no ack and no record. They are also called twice (once from the endpoint with an empty `analysis_result={}`, once from Celery with the real result), so downstream systems receive duplicated triggers. Recording failures log at DEBUG level and are invisible in production.
- **The circuit breaker measures the wrong thing.** It tracks RPM via a Redis counter incremented *after* the decision to fire. By the time it sees the spike, the requests are in flight. It also freezes at agent granularity, so one campaign's overrun stops dialling for unrelated campaigns sharing the same agent pool.

**The redesign reorders these concerns.** The pipeline is rebuilt as durable, leasable, idempotent jobs in Postgres (Phase 1). Rate-limit reservations are pre-flight, atomic, and bound to the durable job (Phase 2) so a worker crash returns the budget rather than leaking it. A cheap classifier routes work into hot and cold lanes (Phase 3) so the LLM quota chases value. A real recording poller replaces the 45s sleep (Phase 4) and emits structured failure events. The circuit breaker is replaced with a continuous-utilisation signal that the dialler consumes to dispatch fewer calls, not zero (Phase 5).

---

## 3. Architecture Overview

```
┌────────────────────────┐
│ Telephony provider     │
│ (Exotel) webhook       │
└────────────┬───────────┘
             │ POST /session/{sid}/interaction/{iid}/end
             ▼
┌────────────────────────────────────────────────────────────────┐
│ FastAPI endpoint                                               │
│  • SELECT interactions row                                     │
│  • UPDATE status = ENDED                                       │
│  • triage classifier → hot / cold / skip lane           [P3]   │
│  • OUTBOX: write processing_jobs rows + audit_events           │
│    (pipeline_created) atomically, then commit                  │
│  • Celery dispatch entry-point stages                          │
│  Returns 200 within Exotel's 5s window.                        │
└────────────┬───────────────────────────────────────────────────┘
             │
             ▼
        ┌────────────────────────────────────────┐
        │ processing_jobs (Postgres, durable)    │
        │  one row per (interaction_id, stage)   │
        │  state: pending / blocked / leased /   │
        │   completed / failed / dead / skipped  │
        └────────────────────────────────────────┘
             │
   ┌─────────┴─────────────┬──────────────────────────────┐
   ▼                       ▼                              ▼
┌──────────────┐    ┌─────────────────┐         ┌──────────────────┐
│ recording    │    │ analysis        │         │ Phases 2+:       │
│ stage [P4]   │    │ stage           │         │  rate limiter +  │
│ exp backoff  │    │  • triage skip  │         │  per-customer    │
│ poller, audit│    │    short calls  │         │  budget, leases  │
│ on every step│    │  • RL reserve ─────────────│  bound to job    │
└──────┬───────┘    │  • LLM call     │         │  id (Phase 2)    │
       │            │  • true-up      │         └──────────────────┘
       │            │    token_usage  │
       │            └────────┬────────┘
       │                     │ on completion → unblock dependents
       │                     ▼
       │           ┌─────────────────────┐
       │           │ signal_jobs stage   │   ┌──────────────────┐
       │           └─────────┬───────────┘   │ lead_stage stage │
       │                     │               └──────────────────┘
       ▼                     ▼                       ▼
┌──────────┐       ┌─────────────────┐     ┌────────────────────┐
│ S3       │       │ Downstream:     │     │ leads table:       │
│ recording│       │ WhatsApp/CRM/   │     │ stage column       │
│ artefact │       │ callback book   │     │ updated            │
└──────────┘       └─────────────────┘     └────────────────────┘

           ┌────────────────────────────────────────────────┐
Throughout: │ audit_events (append-only, indexed by         │
           │ interaction_id + correlation_id) — every state  │
           │ transition writes a row; on-call queries it     │
           │ to reconstruct what happened to a given         │
           │ interaction days later.                         │
           └────────────────────────────────────────────────┘

Capacity feedback loop (Phase 5):
  rate-limiter Redis counters → utilisation float → dialler
  dispatch-probability curve. No binary freeze.
```

### Key design decisions

1. **Postgres is the source of truth for pipeline state, not the Celery broker.** Celery is an executor — it delivers a "go work on job X" message and the worker leases the row. A Redis broker restart loses messages but not work; the stale-lease reaper re-queues anything left in `leased` state.
2. **Idempotency is structural, not procedural.** The `(interaction_id, stage)` unique index prevents duplicate pipelines. The `lease_token` column makes a stale-worker completion a no-op via conditional UPDATE — there is no "did I already do this?" check inside the worker.
3. **Recording and analysis run in parallel.** They have no data dependency. Splitting them into separate stages eliminates the 45s blocking sleep and parallelises the campaign's wall-clock-time end-to-end.
4. **Rate-limit reservations are bound to the durable job.** A worker crash mid-LLM-call means the lease on `processing_jobs.id` expires; the stale-lease reaper picks it up; the rate-limiter's reservation TTL is sized to outlive a single attempt but expire well before infinity. Budget is recovered on crash, not leaked.
5. **Triage drives reservation size, not just routing.** A `cold` summary uses a smaller prompt and gets a smaller reservation. The rate limiter sees fewer tokens consumed for low-value work, freeing headroom for hot-lane reservations.
6. **Backpressure is a continuous signal, not a switch.** The dialler reads `current_utilization` from the rate limiter's counters and applies a smooth curve to its dispatch probability. No single threshold creates a cliff.

---

## 4. Rate Limit Management

The system has one fundamental commitment: **never surface a 429 from the LLM provider to the API caller.** Internally, we track LLM capacity, gate reservations before the LLM call, and when capacity is exhausted, *defer* the work durably rather than fail it.

### How rate-limit usage is tracked

Two-tier counter structure in Redis, keyed by epoch-minute (auto-roll every 60s, 65s TTL for read-side slack):

- **Per-customer reserved counters** (`rl:tpm:rsv:{customer_id}:{epoch_minute}` and the RPM counterpart). Each customer has its own reserved share — written only by reservations that draw from that customer's quota. **A customer's reserved counter is never written by another customer's reservation; this is the structural enforcement of AC2.**
- **Global spillover counters** (`rl:tpm:spill:{epoch_minute}`, `rl:rpm:spill:{epoch_minute}`). The shared headroom = `LLM_TOKENS_PER_MINUTE − sum(per-customer reserved_tpm)`. Any customer can dip into this pool *after* their own reserved share is exhausted.

The reservation operation itself is a single atomic Lua script (`_RESERVE_LUA` in `src/services/rate_limiter.py`). The script reads four counters, computes the split between reserved-source and spillover-source for both TPM and RPM, checks that the spillover side fits within `spillover_limit − spillover_used`, and either commits all four INCRBYs or returns a rejection — atomically, with no other client interleaving. A Python pipeline with WATCH/MULTI would race under burst.

### How the system decides what to process now vs. later

A reservation request can resolve three ways:

1. **Granted from reserved share.** Returns `Reservation(source="reserved")`. Worker proceeds to the LLM call.
2. **Granted from spillover** (or mixed reserved+spillover). Returns `Reservation(source="spillover" | "mixed")`. Worker proceeds; the audit log records which pool the tokens came from for billing reconciliation.
3. **Rejected.** Raises `RateLimitDeferred(retry_after_seconds, reason)`. The worker catches this in `_run_stage_async` and calls `job_store.defer_job` — the job state goes back to `pending`, `scheduled_at` is bumped by `retry_after_seconds`, and **`attempts` is decremented to undo the lease's auto-increment**. Rate-limit deferrals do NOT consume the work-attempt budget. A separate `defer_count` (bounded by `max_defers`) prevents a permanently-constrained job from spinning forever.

The retry-after suggestion is `max(1, seconds_to_next_minute)` — by the next minute boundary the bucket has fully refilled, so deferred jobs naturally cluster at minute boundaries with light jitter from worker scheduling. For a backlog scenario, that means "everything that fits in the next minute fires at the boundary, the rest defers another minute."

### How the system recovers gracefully when limits are hit

Three recovery surfaces:

- **Limiter rejection (capacity exhausted):** the reservation is the gate. The provider never sees the request. The job defers and re-dispatches via Celery countdown.
- **Provider 429 despite reservation:** can happen if our estimate of capacity was off, or another instance shares the same provider key and we're miscounting. We `refund(reservation)` (return tokens to the bucket — they were never consumed), then `apply_provider_429_backoff(customer_id, factor=0.8)`: the customer's *effective* reserved TPM is multiplicatively decreased to 80% of nominal for the next 60 seconds (a Redis flag with TTL). Subsequent reservations for that customer use the reduced effective limit until the flag expires. Then we raise `RateLimitDeferred(reason="provider_429")` with the provider's `Retry-After` value. The 429 itself is absorbed locally — `src/services/post_call_processor.py::process_post_call` translates it into a `RateLimitDeferred`, which the worker turns into `defer_job`. **The 429 never propagates to the API caller (AC1).**
- **Worker crash mid-LLM call:** the lease on `processing_jobs.id` expires; `reap_stale_leases` (Phase 1's janitor) re-queues the row. The reservation in Redis leaks for at most one minute (until the bucket auto-rolls); we accept this brief over-counting because the alternative (two-phase commit between Postgres reservation state and Redis counters) is operationally complex for limited gain. Documented in section 14.

### True-up

Rate-limit reservations operate on an *estimate* (`chars/4 + 500` overhead for prompt + JSON wrapper); the LLM's response carries the exact `usage.total_tokens`. After the call:

- If `actual > estimated`: `charge_extra(reservation, delta)` adds the overage to the same source the original reservation came from. May briefly push the bucket above its nominal limit by the variance (typically <10%); the work is already done, so this is accounting, not gating.
- If `actual < estimated`: `refund_tokens(reservation, |delta|)` returns the unused capacity. Spillover is refunded first (so the customer's reserved counter stays accurate to "tokens actually committed for this customer's work"), then reserved.

Every LLM call writes a row to `token_usage` with `(interaction_id, customer_id, campaign_id, job_id, tokens_estimated, tokens_actual, source)`. This is the billing-grade source of truth — queryable by customer for "how many tokens did Customer X consume this hour?"

### What loads the limits

`src/services/budget.py::get_spillover_capacity` reads `SUM(reserved_tpm)` from `customer_budgets` and subtracts from `settings.LLM_TOKENS_PER_MINUTE`. Cached in-process for 30 seconds (the underlying SQL is cheap, but called per-reservation at burst). `invalidate_spillover_cache()` is exposed for tests and for the customer-budget admin path. `get_customer_budget(customer_id)` returns the customer's row, or a zero-reserved default for unbudgeted customers (they run on spillover only).

### Dialler coordination — continuous backpressure (Phase 5)

The original system tripped a binary circuit breaker at 90% utilisation that froze the dialler for 1800 seconds. Phase 5 replaces that with a continuous capacity signal.

**The signal.** `src/services/backpressure.py::current_utilization` returns `max(global_tpm_used / cap, global_rpm_used / cap)` in `[0, 1+]`. The values are the dedicated global aggregate counters (`rl:tpm:global:{minute}` and `rl:rpm:global:{minute}`) that the Phase 2 reservation Lua script writes alongside the per-customer and spillover keys. One Redis pipeline read; O(1) regardless of customer count. Refunds and true-ups keep the global counter consistent so the dialler sees capacity recover immediately rather than waiting on the minute roll-over.

**The curve.** `dialler_dispatch_probability(utilization) = 1 − sigmoid((utilization − 0.7) × 10)`. Inflection at 0.7 utilisation (`p = 0.5`); slope steep enough to shed aggressively above 90% but with a soft asymptote (~5% at 100%) so a brief overshoot never produces a hard stop. Concrete points:

| Utilisation | Dispatch probability |
|---|---|
| 0.0 | ~1.00 |
| 0.5 | ~0.88 |
| 0.7 | 0.50 |
| 0.9 | ~0.12 |
| 1.0 | ~0.05 |

The dialler reads `current_dispatch_probability()` per dispatch decision and draws a uniform `[0, 1)` — dial if `draw < p`, otherwise skip. The expected dispatch rate is therefore `p × baseline`. There is no breaker state, no freeze duration, no half-open transition. As soon as utilisation drops, dispatch probability climbs back automatically — so a transient spike heals in seconds, not minutes.

**Why this is structurally better than the breaker.** The breaker's failure mode was bistable: 89% → full speed, 90% → zero. The new curve has no edge — the dispatch rate moves smoothly with the underlying signal, so the dialler self-tunes without ever colliding with a hard threshold. Operationally the difference shows up most at ~85–95% utilisation, where the breaker would frequently flip and freeze; the curve simply produces moderate shedding.

**Why the post-call processor doesn't talk to backpressure.** The rate limiter's reservations are the load-bearing gate for LLM calls. Backpressure is the *outbound* signal for the dialler — it shapes inflow before reservations are even attempted. There is no double-gating: workers reserve unconditionally, and a deferred reservation flows through `defer_job` (see above). The two systems share counters, not control flow.

**Per-customer queue depth + Prometheus.** `src/api/metrics.py` exposes a `/metrics` endpoint in Prometheus exposition format with:

- `llm_utilization` (gauge) — the float above
- `dialler_dispatch_probability` (gauge) — the curve output
- `llm_tokens_used_minute`, `llm_requests_used_minute` (gauges) — raw aggregates
- `llm_tokens_per_minute_cap`, `llm_requests_per_minute_cap` (gauges) — config visibility
- `postcall_jobs{customer_id, state}` (gauge) — per-customer queue depth from `processing_jobs`, served by `idx_processing_jobs_customer_state`. Cardinality is bounded by `customers × states` (≤ ~7 states), so this is safe at any realistic customer count.

Alert thresholds (the alerting infrastructure itself is out of scope; the metric names are stable contracts):

| Signal | Threshold | Severity |
|---|---|---|
| `llm_utilization` | ≥ 0.75 | warn — capacity tight |
| `llm_utilization` | ≥ 0.90 | page — sustained 90%+ correlates with defer-spike |
| growth rate of `sum(postcall_jobs{state="pending"})` | > 1000/min | page — backlog growing faster than workers drain |
| reaped-stale-leases per hour (from `reaped_stale_leases` log line) | > 100 | warn — persistent worker churn |

The `/metrics` endpoint deliberately falls back to an empty queue-depth section if Postgres is unavailable: utilisation and dispatch probability remain available, which is the load-bearing path for backpressure.

---

## 5. Per-Customer Token Budgeting

The platform allocates LLM capacity across concurrent customers via a hard reservation + shared spillover model, expressed in the `customer_budgets` table (see section 8 for the full schema).

### Allocation across customers

Each customer has a row with `reserved_tpm` and `reserved_rpm` — their guaranteed minimum share. The platform guarantees that **the sum of reserved across all customers does not exceed the global limit**: `sum(reserved_tpm) ≤ LLM_TOKENS_PER_MINUTE`. The remainder is the spillover pool: `spillover = LLM_TOKENS_PER_MINUTE − sum(reserved_tpm)`. Any customer can draw from spillover after their own reserved is exhausted.

A customer with no `customer_budgets` row gets `reserved=0` and runs on spillover only — useful for trial customers or new accounts that haven't been onboarded to billed capacity yet.

### What guarantees does a customer with a pre-allocated budget receive?

Concretely: **a customer with `reserved_tpm = X` will always be granted at least X tokens per minute**, regardless of any other customer's behaviour, as long as their requests fit within the minute window. The reservation Lua script's structure makes this load-bearing — the reserved counter for Customer A is a different Redis key than for Customer B; one can never write the other.

If `LLM_TOKENS_PER_MINUTE = 100`, Customer A reserves 20, Customer B reserves 30:

- A gets **at least** 20 TPM, always. Even if B + spillover have fully drained the rest, A's first 20 tokens of the next minute land instantly.
- B gets at least 30 TPM, always.
- Anyone (A, B, or any unbudgeted customer) can draw from the 50-TPM spillover after their own reserved is exhausted.

### What happens when a customer exceeds their budget?

- **Within their reserved share:** granted from `rl:tpm:rsv:{cid}:{minute}`. Source label = `"reserved"`.
- **Above reserved, but spillover available:** granted with mixed accounting. Source label = `"spillover"` (or `"mixed"` if the request crossed the boundary). Later requests from that customer in the same minute keep eating spillover until it's exhausted.
- **Spillover exhausted:** request rejected with `RateLimitDeferred`. Job defers, worker re-tries near the next minute boundary. If the customer is *consistently* over their reserved (e.g., during a burst), they'll keep deferring through the spillover-rejection path until either spillover frees up (other customers paused) or the new minute fully replenishes their reserved share.

A customer that *permanently* operates above their reserved (because reserved was set too low) accumulates `defer_count` on every job that gets rate-limited. After `max_defers` (default 100), that job goes `dead`. The dead state preserves the payload for replay; it's also the alerting surface for "this customer needs their reserved increased" as part of capacity planning.

### What happens to unallocated headroom?

Spillover is the unallocated headroom, available to anyone. There is no reservation discipline within spillover — first-come-first-served at the Lua-atomic level. Under contention (multiple customers all hitting spillover simultaneously), Phase 5 backpressure shapes dialler dispatch probability so the dialler reduces inflow before spillover saturates.

The `priority_weight` column on `customer_budgets` is reserved for Phase 5: when the dialler is choosing whom to dispatch under load, higher-weight customers get more of the limited dispatch slots. Within the rate limiter itself, weight is not used — the structural reserved/spillover split provides hard isolation, which is stronger than weighted draws.

### Trade-off acknowledged

This model gives **per-customer floor guarantees** but no per-customer ceiling — a customer can use 100% of spillover if no one else is competing. That's deliberate. The alternative (per-customer ceilings) would leave the LLM under-utilised whenever some customers are quiet, which costs revenue. The dead-state ceiling on `defer_count` is the only hard upper bound, and it triggers only when a customer is *consistently* over budget for the duration of `max_defers × retry_after`.

---

## 6. Differentiated Processing

The fixture data shows outcome distribution is bimodal — a small fraction of calls drive the business value (confirmed bookings, demos, escalations) while most calls are batch-tolerant (not interested, callback later, already done) or worthless (wrong number, hangup). The current system spends identical 1500-token full analysis on all of them, so under burst the LLM quota gets consumed by cheap calls and starves the valuable ones. The redesign introduces a cheap classifier in front of the LLM so quota chases value.

### Three lanes

| Lane  | Priority | What it gets                                  | Latency commitment |
|-------|----------|-----------------------------------------------|--------------------|
| `hot` | 1        | Full analysis prompt (call_stage + entities + summary), ~1500-token reservation, dedicated `postcall_hot` queue | end-to-end < 60s of webhook |
| `cold`| 9        | Summary-only prompt, ~500-token reservation, dedicated `postcall_cold` queue, optional defer of `scheduled_at` | end-to-end < 30 min |
| `skip`| –        | No LLM call. Analysis row written in `skipped` state with `{"call_stage": "short_call"}`; downstream stages run immediately | < 5s |

The lane label is set at pipeline-creation time and persisted on every row of the interaction's pipeline. It drives three things: the analysis row's `priority` column (which decides ordering inside `lease_next`'s `ORDER BY priority ASC, scheduled_at ASC`), the Celery queue used for dispatch (so workers can be sized per lane independently), and the prompt + reservation hint inside `PostCallProcessor`.

### Two-stage classifier

**Stage A — keyword / regex.** Cheap, deterministic, runs in <1ms. Three pattern sets:

- `_SKIP_PATTERNS`: "wrong number", "galat number", obvious-hangup signals.
- `_HOT_PATTERNS`: "confirmed", "book(ed)", "demo", "appointment", "schedule", "tomorrow at", weekday names, explicit time references ("3:30 PM", "6 baje"), escalation words ("manager", "complaint", "escalate", "senior executive"), calendar-invite intent.
- `_COLD_PATTERNS`: "not interested", "don't call", "callback / call me back", "baad mein call", "already booked / purchased / done", "through your website".
- `_AMBIGUOUS_PATTERNS`: hedge words — "thinking", "let me see", "considering", "soch raha hoon", "Dekhta hoon", "budget tight", "next week".

Customer turns are weighted 2× agent turns — what the *customer* says decides the disposition; what the *agent* says is largely scripted. Stage A commits to a lane only when the dominant score (hot or cold) is at least 2 *and* beats the runner-up by at least 2; otherwise the ambiguous bucket triggers Stage B fallback. If neither hot nor cold scores a hit at all, the default is cold — safe (won't burn hot-lane reservation) and recoverable (still produces an analysis result, just at lower priority).

**Stage B — small/cheap LLM fallback.** Invoked only when Stage A signals ambiguity (mixed evidence or a hedge-heavy transcript). The prompt is a tight "hot or cold? one word" classifier against a small fast model — ~50 tokens per call instead of ~1500. The canonical case is the `hinglish_ambiguous` fixture: Hindi/English code-switching with "Dekhta hoon", "soch raha hoon", "next week" — the customer is *considering*, not committing, and the right answer depends on tone the keyword path can't see. The current implementation mocks Stage B with a deterministic heuristic biased toward cold for "considering" transcripts; in production we'd point this at a real small model. Tests can monkey-patch `triage._stage_b_llm_fallback` for deterministic outcomes.

### Per-customer overrides

The `customer_budgets.customer_overrides` JSONB column lets a customer opt specific dispositions into a different lane without a deployment. Example: `{"already_done": "hot"}` for a customer that wants every "already purchased" call routed to a sales rep for upsell. The endpoint reads the override map per request and passes it to `triage.classify`, which applies it after the stage-A/B disposition is resolved — so the override sees the *normalised* disposition, not the raw transcript text.

### Reservation size, not just routing

Triage drives more than queue selection. The `lane` column is read by `_do_analysis` and passed through to `PostCallProcessor.process_post_call`, which does two things with it:

1. **Prompt variant.** Cold lane uses a summary-only system prompt (no entity extraction) — entities don't drive any cold-lane downstream action, so paying for them at 100K calls is waste.
2. **Reservation cap.** The token estimate fed into the rate limiter is bounded by `COLD_LANE_RESERVATION_TOKENS` (default 500) for cold work and `HOT_LANE_RESERVATION_TOKENS` (default 1500) for hot work. Under contention this frees headroom for hot-lane reservations rather than burning quota on summary-only calls that will return ~500 tokens regardless.

### Hot work jumps the queue

`processing_jobs.idx_processing_jobs_pending` is a partial index `(stage, priority, scheduled_at) WHERE state = 'pending'`. `lease_next` orders `ORDER BY priority ASC, scheduled_at ASC`, so a `hot` row with `priority=1` is leased ahead of a backlog of `cold` rows at `priority=9` — even if the cold rows landed first. The integration test `tests/test_triage.py::test_hot_row_leased_ahead_of_cold_backlog` confirms this against a real Postgres.

For the cold lane there's an additional knob: `cold_defer_seconds` pushes the `scheduled_at` of the cold-lane analysis row into the future so cold work can be batched into a low-utilisation window. Default is zero (process when a cold worker is free); operators can raise it to actively batch into off-peak periods. Recording / signal_jobs / lead_stage stay on `NOW()` because they're not the LLM-quota bottleneck.

### Lane-aware Celery routing

`src/tasks/celery_app.py` registers four queues: `postcall_processing` (default), `postcall_hot`, `postcall_cold`, `recording_poll`. The helper `queue_for_lane(lane, stage)` in `src/tasks/celery_tasks.py` maps a (lane, stage) pair to the right queue:

- analysis + hot → `postcall_hot`
- analysis + cold → `postcall_cold`
- everything else → `postcall_processing`

signal_jobs and lead_stage stay on the default queue regardless of lane, because they're short, IO-bound, and shouldn't compete with analysis for hot-lane worker slots. Operators run `celery -A src.tasks.celery_app worker -Q postcall_hot` to scale hot capacity independently of cold backlog.

### Trade-offs accepted

- **Stage B latency.** A cold-but-actually-hot call routed to Stage B then to the cold lane sits behind cold backlog. The `hinglish_ambiguous` case is the one we expect this on; if the customer turns out hot, the lead-stage update still lands within the 30-minute cold commitment, which is below the threshold where sales falls off. Documented as a known weakness in §14.
- **Keyword brittleness.** A new outcome type introduced by a campaign change won't match any of the curated patterns and will route to cold by the "no signals → cold" fallback. The mitigation is the customer override path: a customer can pin a new disposition to hot with a single config change, no deploy. Long-term we replace the keyword stage with a small LLM but the cost calculus only works once we measure full keyword-coverage rate.
- **Customer overrides ≠ lane policies.** A customer cannot raise their global priority via overrides; only specific dispositions. Global prioritisation is `customer_budgets.priority_weight` and is owned by Phase 5's backpressure layer.

---

## 7. Recording Pipeline

The recording stage runs as a long-lived in-job poll loop on a dedicated `recording_poll` queue, separate from the analysis pipeline. The stage is owned by `src/tasks/recording_poller.py:poll_recording`; it leases the row once, runs the entire backoff schedule inside that single Celery delivery, and either completes or marks the job for retry/dead.

### Why a long-lived in-job loop, not per-attempt redeliveries

Phase 1 wired the recording stage into the generic `run_stage` dispatcher, which meant each "still not ready" outcome consumed an attempts budget and required a fresh Celery message to retry. At 100K calls per campaign that's 300K–500K extra dispatches in the slow case — each carrying its own broker round-trip, lease acquire/release, and audit-row write. A poll that takes seconds becomes a poll that takes minutes-to-hours of pipeline overhead.

The poller runs the whole backoff schedule inside one leased job. The lease is sized to cover the full schedule plus slack (`max(sum(BACKOFF) + 60s, 180s)`); if the worker crashes mid-loop the row stays in `leased` until the lease expires, after which the reconciler re-queues it. From the durability side, this is the same guarantee Phase 1 already had — we just amortise the overhead across one delivery instead of per attempt.

### Backoff schedule

Default (configurable in `src/config.py`): **5s, 10s, 20s, 40s, 80s** with **±20% jitter**, max 6 polls (one initial + five backoff entries). The total wall-clock budget — about 155s — exceeds the 120s tail of Exotel's normal delivery window, so a recording that's going to arrive will be caught inside a single job lease.

```
poll #1 ──┐ NOT_READY → sleep 5s ±20%
poll #2 ──┤ NOT_READY → sleep 10s ±20%
poll #3 ──┤ NOT_READY → sleep 20s ±20%
poll #4 ──┤ NOT_READY → sleep 40s ±20%
poll #5 ──┤ NOT_READY → sleep 80s ±20%
poll #6 ──┘ NOT_READY → exit loop, fail_job (retry or dead)
            READY     → upload to S3, complete_job
            PERMANENT_FAILURE → exit loop immediately, fail_job
```

Jitter prevents 100K simultaneously-webhooked recordings from synchronising on the same poll instants and creating a thundering-herd against the Exotel API.

### Per-attempt observability

Every poll attempt writes one `audit_events` row with `event_type='recording_poll_attempt'`. The row carries the attempt number, the outcome (`ready` / `not_ready` / `permanent_failure` / `transport_error`), and — for non-terminal attempts — the next scheduled delay (post-jitter, so the audit reflects what the worker actually waited). The on-call query for "what happened to recording X" is one ORDER BY against `audit_events` filtered to `interaction_id` and `stage='recording'`.

Outcomes worth alerting on:

| event_type | severity | when |
|---|---|---|
| `recording_completed` | info | success — recording uploaded to S3 |
| `recording_poll_attempt` | warning | per attempt that didn't return READY |
| `recording_failed_will_retry` | warning | poll budget exhausted; job will retry on the schedule |
| `recording_failed_terminal` | **error** | terminal — job moved to `dead`, payload preserved for replay |
| `recording_reconciled_to_pending` | warning | reconciler reset a stuck `leased` row |

`recording_failed_terminal` is the alert-shaped event. A metrics scrape over `audit_events WHERE event_type='recording_failed_terminal' AND created_at > NOW() - INTERVAL '1 hour'`, grouped by `customer_id`, gives per-customer terminal failure counts. Documented thresholds (alert wiring is out of scope for this assessment):

- **`recording_failure_rate_warn`** — >5% terminal failures per customer per hour. Page-secondary or warn-channel.
- **`recording_failure_rate_page`** — >20% per customer per hour. Page primary on-call.

A spike of `recording_poll_attempt` rows with `outcome='transport_error'` means Exotel itself is unhealthy, not the call data — useful for distinguishing infrastructure outages from data problems on the same dashboard.

### Permanent failure short-circuit

A poll attempt with no `call_sid` (the call dropped before pickup, so there's no recording to fetch) returns `PERMANENT_FAILURE` on the first iteration and exits the loop without burning the backoff budget. The job state machine marks it failed; after `max_attempts` it moves to `dead`. This separates "Exotel is slow" from "this call genuinely has no recording".

### Reconciliation: catching the "uploaded but DB write failed" gap

A worker that uploaded the recording to S3 but crashed before `complete_job` would leave the row in `leased` indefinitely if we relied solely on the lease-expiry path — `lease_expires_at` is sized for the *full poll schedule plus slack*, which can be 3-4 minutes. That's a 3-4 minute window where the recording is durable in S3 but the pipeline doesn't know.

`reconcile_recording_jobs` runs on Celery beat every 60s (`RECORDING_RECONCILE_INTERVAL_SECONDS`). It scans for recording rows in `leased` state where either:

- `lease_expires_at < NOW()` — generic stale-lease catch (also covered by `reap_stale_leases_task`, redundant by design so the recording lane stays responsive even if the global reaper is slow), OR
- `leased_at < NOW() - 5 minutes` (`RECORDING_STUCK_AFTER_SECONDS`) — caught a row that's been leased longer than any legitimate poll loop should take.

Each reset row gets a `recording_reconciled_to_pending` audit event at severity=warning, and a fresh `poll_recording` Celery message is dispatched immediately so the row doesn't have to wait for the next external trigger. S3 uploads are keyed on `interaction_id`, so re-running the upload step on the new attempt overwrites the same key — idempotent at the artefact layer.

### What the implementation deletes

The 45-second `asyncio.sleep` was already gone in Phase 1; Phase 4 also removes `_do_recording` from `src/tasks/celery_tasks.py` (the per-attempt single-shot handler) and stops importing `fetch_and_upload_recording` from the dispatcher path. `fetch_and_upload_recording` itself stays in `src/services/recording.py` as a thin wrapper around `poll_recording_once + upload_recording_to_s3` for callers that want one-shot semantics — the poll loop owns the production path. If a stale Celery message routes a recording job to `run_stage`, it's forwarded to `poll_recording` without leasing so the redelivery doesn't burn an attempt.

### What this validates from the rubric

- **AC4** — recording failures retry with backoff (jittered exponential, not fixed delay) and are never silently dropped (every attempt and every terminal failure is an audit row).
- **AC6** — every error path emits a structured log carrying `interaction_id` as `correlation_id` (set automatically inside the `correlation_context` block that wraps the loop).

---

## 8. Data Model

The redesign adds two tables now (Phase 0 / Phase 1 — durable execution + audit trail) and two more later (Phase 2 — budgets + token usage). Existing tables (`leads`, `sessions`, `interactions`) are unchanged in surface area; `interactions.interaction_metadata` continues to be the dashboard's hot cache.

### `processing_jobs` (Phase 0/1) — durable pipeline state

One row per `(interaction_id, stage)`. The unique index on that pair is the idempotency guarantee for duplicate webhook deliveries. The `lease_token` column makes a stale-worker completion a no-op via a conditional UPDATE.

```sql
CREATE TABLE processing_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    stage VARCHAR(32) NOT NULL,                    -- recording / analysis / signal_jobs / lead_stage
    state VARCHAR(16) NOT NULL DEFAULT 'pending',  -- pending / blocked / leased / completed / failed / dead / skipped
    priority SMALLINT NOT NULL DEFAULT 5,          -- Phase 3 sets 1 (hot) or 9 (cold)
    lane VARCHAR(16),                              -- Phase 3
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    lease_token UUID,
    leased_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    worker_id VARCHAR(128),
    payload JSONB NOT NULL DEFAULT '{}',
    result JSONB,
    last_error TEXT,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    depends_on_job_id UUID REFERENCES processing_jobs(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_processing_jobs_interaction_stage
    ON processing_jobs(interaction_id, stage);

CREATE INDEX idx_processing_jobs_pending
    ON processing_jobs(stage, priority, scheduled_at)
    WHERE state = 'pending';

CREATE INDEX idx_processing_jobs_leased
    ON processing_jobs(lease_expires_at)
    WHERE state = 'leased';

CREATE INDEX idx_processing_jobs_customer_state
    ON processing_jobs(customer_id, state);
```

State transitions (and their guard conditions):

```
pending  → leased     [lease_job: lease_token = uuid_generate_v4(), attempts += 1]
blocked  → pending    [complete_job of dependency: WHERE depends_on_job_id = $1]
leased   → completed  [complete_job: WHERE lease_token = $1]
leased   → failed     [fail_job, attempts < max → state := pending]
leased   → dead       [fail_job, attempts >= max → terminal, payload preserved]
leased   → skipped    [mark_skipped: dependents still unblock]
leased   → pending    [reap_stale_leases: lease_expires_at < NOW()]
```

### `audit_events` (Phase 0/1) — append-only audit trail

Every state transition writes a row. An on-call engineer queries this table by `interaction_id` to reconstruct what happened to a specific call. The table is indexed for both the per-interaction debug query and aggregate alerting queries by `event_type`.

```sql
CREATE TABLE audit_events (
    id BIGSERIAL PRIMARY KEY,
    interaction_id UUID NOT NULL,
    correlation_id UUID NOT NULL,
    customer_id UUID,
    campaign_id UUID,
    job_id UUID,
    stage VARCHAR(32),
    event_type VARCHAR(64) NOT NULL,
    severity VARCHAR(8) NOT NULL DEFAULT 'info',
    details JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_events_interaction ON audit_events(interaction_id, created_at);
CREATE INDEX idx_audit_events_correlation ON audit_events(correlation_id, created_at);
CREATE INDEX idx_audit_events_type ON audit_events(event_type, created_at);
CREATE INDEX idx_audit_events_customer ON audit_events(customer_id, created_at);
```

`event_type` values used so far: `pipeline_created`, `stage_leased`, `stage_completed`, `stage_skipped`, `stage_failed_will_retry`, `stage_dead`. Phase 2 adds `rate_limit_reserved`, `rate_limit_429_absorbed`, `token_usage_recorded`. Phase 4 adds `recording_poll_attempt`, `recording_failed_terminal`.

### `customer_budgets` (Phase 2 — placeholder)

```sql
-- Phase 2 deliverable. Documented here for completeness.
CREATE TABLE customer_budgets (
    customer_id UUID PRIMARY KEY,
    reserved_tpm INTEGER NOT NULL DEFAULT 0,
    reserved_rpm INTEGER NOT NULL DEFAULT 0,
    priority_weight SMALLINT NOT NULL DEFAULT 1,
    burst_multiplier NUMERIC(4,2) NOT NULL DEFAULT 1.5,
    customer_overrides JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### `token_usage` (Phase 2 — placeholder)

Append-only ledger; source of truth for billing.

```sql
-- Phase 2 deliverable.
CREATE TABLE token_usage (
    id BIGSERIAL PRIMARY KEY,
    interaction_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    job_id UUID NOT NULL,
    tokens_estimated INTEGER NOT NULL,
    tokens_actual INTEGER NOT NULL,
    cost_micros BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_token_usage_customer_time ON token_usage(customer_id, created_at);
CREATE INDEX idx_token_usage_interaction ON token_usage(interaction_id);
```

### Schema migration path

The current `data/schema.sql` is loaded by `docker-compose` on init from a fresh `pgdata` volume. For local development, a stale volume is re-initialised with `docker-compose down -v && docker-compose up -d`. In production, this would be split into versioned migrations (`alembic` or equivalent); each table addition is non-destructive and can be applied online without downtime. No existing column on `interactions`, `leads`, or `sessions` is altered.

---

## 9. Auditability & Observability

The on-call test for this system is: **3 days after a customer reports "interaction X never showed up in the dashboard", an engineer who has never seen this interaction before can reconstruct what happened from the durable record using `interaction_id` as the only handle.** Every design decision in this section flows from that test.

### What gets logged

Two surfaces, with different durability guarantees:

| Surface | Mechanism | Durability | Use |
|---|---|---|---|
| **Application logs** | `src/utils/logging.py::JsonFormatter` over stdout, keyed by `correlation_id = interaction_id` (set by `correlation_context(...)` contextvar — every log call inside the block auto-injects it without the caller threading anything through) | Best-effort. Stdout → log shipper → search index. Lost on shipper failure. | High-volume, low-importance — every step the worker takes. Useful for detail; not the source of truth. |
| **`audit_events` table** | `write_audit_event(...)` writes a row in the **caller's transaction**, so the audit row and the state change land atomically — there is no audit-vs-state divergence on crashes. | Durable. Append-only Postgres. | Pipeline state-transition log — the source of truth for "what happened to this interaction". |

`correlation_context(interaction_id)` is wrapped around every worker entry point (`_run_stage_async` in `src/tasks/celery_tasks.py`, `_poll_recording_async` in `src/tasks/recording_poller.py`, the FastAPI endpoint handler). Every log line and every audit-event row from inside the block carries the same `correlation_id`. A scrape across the whole system can be filtered by interaction_id without coordination.

### The canonical debug query

> "Interaction `e4b3...` looks broken. What happened?"

```sql
SELECT created_at, event_type, severity, stage, details, customer_id
FROM audit_events
WHERE interaction_id = 'e4b3...'
ORDER BY created_at;
```

This single query reconstructs the timeline from `pipeline_created` through every `stage_leased` / `stage_completed` / `stage_failed_will_retry` / `stage_rate_limit_deferred` / `recording_poll_attempt` / `stage_dead`, with a `details` JSONB carrying the per-event context (which worker, which attempt, what error string, what reservation source, retry-after seconds, etc.). Nothing is implicit.

The supporting indexes are in `data/schema.sql`:

- `idx_audit_events_interaction (interaction_id, created_at)` — this query.
- `idx_audit_events_correlation (correlation_id, created_at)` — same query when correlation diverges from interaction (rare; reserved for cross-interaction joins).
- `idx_audit_events_type (event_type, created_at)` — for "how many `recording_failed_terminal` rows last hour" alerts.
- `idx_audit_events_customer (customer_id, created_at)` — for per-customer dashboards.

### Other useful query patterns

| Question | Query |
|---|---|
| What's stuck in `pending` right now? | `SELECT customer_id, stage, COUNT(*) FROM processing_jobs WHERE state = 'pending' GROUP BY customer_id, stage;` |
| Per-customer LLM spend in the last hour | `SELECT customer_id, SUM(tokens_actual) FROM token_usage WHERE created_at > NOW() - INTERVAL '1 hour' GROUP BY customer_id;` |
| Recording terminal-failure rate | `SELECT customer_id, COUNT(*) FROM audit_events WHERE event_type='recording_failed_terminal' AND created_at > NOW() - INTERVAL '1 hour' GROUP BY customer_id;` |
| Worker churn signal | `reaped_stale_leases` log lines per hour (`celery beat` runs the reaper every 30s and logs the reaped count). |

### Live operational signals

Phase 5's `/metrics` endpoint (`src/api/metrics.py`) exposes the live numbers as Prometheus gauges: `llm_utilization`, `dialler_dispatch_probability`, `llm_tokens_used_minute` / `llm_requests_used_minute`, `postcall_jobs{customer_id, state}`. Alert thresholds documented in §4.

### What this validates from the rubric

- **AC5** — every interaction has a complete audit trail; the canonical query above returns one row per state transition.
- **AC6** — every error path emits a structured log carrying `interaction_id` as `correlation_id`. The contextvar approach makes "every call carries the id" structural rather than discipline-dependent.

---

## 10. Data Model

The full schema lives in `data/schema.sql` and is loaded into Postgres on `docker-compose up -d` from a fresh `pgdata` volume. Section 8 above carries the table-level reference; this section covers the migration path and what changed at each phase.

### What this redesign added (vs. the original schema)

| Phase | Tables / columns added | Why |
|---|---|---|
| Phase 1 | `processing_jobs` (full table), `audit_events` (full table) | Durable pipeline state + append-only audit trail. |
| Phase 2 | `customer_budgets` (full table), `token_usage` (full table), `processing_jobs.defer_count` / `max_defers` | Per-customer reserved capacity, billing-grade ledger, distinct soft-retry counter for rate-limited jobs. |
| Phase 3 | `processing_jobs.priority` / `lane`, `customer_budgets.customer_overrides` | Hot/cold routing + per-customer no-deploy disposition overrides. |
| Phase 4 | (none — `processing_jobs` already had what it needed) | The recording poller is purely a worker-loop change. |
| Phase 5 | (none) | Backpressure reads existing Redis counters. |

Existing tables (`leads`, `sessions`, `interactions`) are unchanged in surface area. `interactions.interaction_metadata` JSONB continues to serve as the dashboard's hot cache and is updated atomically with the `token_usage` ledger row in `process_post_call`.

### Migration path

The local development workflow re-initialises the schema with `docker-compose down -v && docker-compose up -d`. In production this would be split into versioned migrations (Alembic or equivalent). Each addition is **non-destructive and online-applicable**:

- Adding new tables (`processing_jobs`, `audit_events`, `customer_budgets`, `token_usage`) is `CREATE TABLE` + `CREATE INDEX` — locks are short and per-table. No existing reads or writes touch these.
- Adding columns (`processing_jobs.priority`, `lane`, `defer_count`, `max_defers`; `customer_budgets.customer_overrides`) is `ALTER TABLE … ADD COLUMN` with defaults. On Postgres 11+ this is a metadata-only operation for nullable columns and columns with constant defaults — no full table rewrite, no row lock.

The order of migrations corresponds to the phase order. Each phase's tests run green against the previous phase's schema plus that phase's additions.

### Idempotency at the schema level

`uq_processing_jobs_interaction_stage UNIQUE (interaction_id, stage)` is the load-bearing idempotency guarantee. A duplicate webhook delivery hits this constraint and the `ON CONFLICT (interaction_id, stage) DO NOTHING` in `create_pipeline` makes the second attempt a no-op without surfacing an error. This is structural, not procedural — there is no "have I seen this interaction before?" check anywhere in application code.

---

## 11. Security

### What's sensitive

The system handles three classes of sensitive data, each with its own protection requirement:

| Data | Where it lives | Sensitivity |
|---|---|---|
| **Call transcripts** | `interactions.conversation_data` JSONB; `processing_jobs.payload` JSONB (snapshot per pipeline) | Spoken PII (names, phone numbers, addresses, sometimes payment / health details). The richest single PII surface in the system. |
| **Lead PII** | `leads.name`, `leads.phone`, `leads.email`, `leads.lead_data` JSONB | Direct identifiers, regulated under data-protection law (DPDP / GDPR / equivalent). |
| **Recording audio** | S3 bucket (`S3_BUCKET`); URL stored in `interactions.recording_url`, `interactions.recording_s3_key` | Same content as transcripts, in audio form. Voice biometrics are themselves PII in some jurisdictions. |
| **LLM prompts and responses** | Transient (logged at debug level only); not persisted in full | Contains transcript text. Treated as transcripts for handling. |
| **Audit events** | `audit_events.details` JSONB | Carries small bits of context (event type, attempt number) — does **not** copy transcripts or PII fields. |

### Strategy in place today

- **No transcripts in audit logs.** `_safe_summary(...)` in `src/tasks/celery_tasks.py` strips large fields out of `audit_events.details` (the raw LLM response can carry ~1500 tokens of transcript context). Audit rows keep stable, non-PII fields: `call_stage`, `tokens_used`, `latency_ms`, `s3_key`, `skipped`. The full LLM response stays in `processing_jobs.result`, where access is gated through Postgres role grants.
- **No transcripts in application logs.** The structured logger never emits transcript text — the worker log lines carry `interaction_id`, stage names, and counts. Anyone with log access cannot reconstruct call content.
- **No transcripts in `/metrics`.** The Prometheus endpoint emits aggregates only; cardinality is bounded by `customers × states`.
- **Postgres on a private network.** Per the deployment assumption (§1, item 7), Postgres is reachable only from the application network. Connection strings carry credentials and live in environment variables, not source control.
- **TLS everywhere in transit.** Webhook receipt (HTTPS), Postgres (TLS), Redis (TLS), Exotel API (HTTPS), LLM provider (HTTPS), S3 (TLS).

### Encryption at rest — deferred, with hooks

Encryption at rest is the principal "nice-to-have" item the brief calls out, and it is deferred from this implementation. The hooks needed to add it without rewriting are:

- **Recording artefacts (S3).** Switch the recording S3 bucket to SSE-KMS with a per-customer KMS key. The recording S3 key already incorporates `customer_id` (`{customer_id}/{interaction_id}.mp3` is the production layout); the upload path passes `customer_id` so attaching `ServerSideEncryption=aws:kms, SSEKMSKeyId=...` is a single config addition in `src/services/recording.py::upload_recording_to_s3`. No application reads the raw bytes — only signed URLs are handed to dashboards.
- **Transcripts and lead PII (Postgres).** Column-level encryption via `pgcrypto`'s symmetric envelope (`pgp_sym_encrypt(...)` / `pgp_sym_decrypt(...)`) over `interactions.conversation_data` and `leads.lead_data`. The encryption key is fetched from a KMS at app start and held in process memory, never written to disk. Adding this requires touching: (a) the schema (no column type change — `bytea` instead of `jsonb` for the encrypted blob, with a thin view restoring the JSONB shape on read), (b) the load/store paths in `src/api/endpoints.py` and `src/services/post_call_processor.py`, (c) operational tooling for key rotation. This is real work, hence deferred.
- **Backups.** Postgres logical backups would inherit the encrypted-column ciphertext directly. Physical backups of the data volume already get OS-level encryption from the cloud provider's disk encryption.

### Access patterns

- The dashboard reads `interaction_metadata` (analysis result, dashboard cache) — no direct PII; transcript text is fetched only on operator drill-down through a separate authenticated endpoint (out of scope for this assessment).
- The on-call audit query (§9) reads `audit_events` — by design, no transcript data lives there.
- Billing reads `token_usage` — counts and source labels only, no transcripts.

### Secrets handling

The LLM provider key, Exotel credentials, S3 credentials, and database credentials are environment variables. The mock `LLM_API_KEY = "sk-mock-key-for-assessment"` in `src/config.py` exists only for the tests; production deployments inject the real value via the secret manager. Nothing in the source tree references the real key directly.

### What this validates from the rubric

- **AC10** — sensitive data is identified explicitly (transcripts, recordings, lead PII) and the protection strategy covers in-transit (TLS), at-rest-today (no PII in logs / audit / metrics), and at-rest-future (KMS / pgcrypto hooks documented for the deferred encryption work).

---

## 12. API Interface

### Externally — unchanged

```
POST /session/{session_id}/interaction/{interaction_id}/end
Content-Type: application/json

{
  "call_sid": "exotel-call-001",
  "duration_seconds": 180,
  "call_status": "completed",
  "additional_data": {…}
}

200 OK
{
  "status": "ok",
  "interaction_id": "f0000000-...",
  "message": "Interaction ended, durable pipeline created"
}
```

The shape on the wire is identical to what the telephony provider sent before this redesign. No customer integration needs to change.

### Internally — the contract changed

What the endpoint promises has shifted from "best-effort, fire-and-forget" to **"the pipeline is durable before this 200 returns"**.

**Before:**
1. The endpoint optionally fired `signal_jobs` and `update_lead_stage` from `asyncio.create_task` with `analysis_result={}` (an empty payload), then scheduled a Celery task and returned. If the FastAPI process restarted in between, the in-memory tasks were lost. If Celery's Redis broker was down, the task was lost. Downstream services received duplicate triggers (one empty, one real). No record was kept.
2. The 200 meant nothing about durability.

**Now:**
1. The endpoint runs `triage.classify` over the transcript inside the request handler.
2. In a single Postgres transaction: it `UPDATE`s `interactions.status = 'ENDED'`, `INSERT`s the four `processing_jobs` rows for this interaction's pipeline (`recording`, `analysis`, `signal_jobs`, `lead_stage`), and `INSERT`s a `pipeline_created` row in `audit_events`. Either everything lands or nothing does.
3. After commit, the endpoint dispatches the entry-point Celery messages — `poll_recording.apply_async` for the recording stage and `run_stage.apply_async` for the analysis stage (or directly for `signal_jobs` / `lead_stage` if the transcript is short).
4. The 200 means: the durable pipeline rows exist in Postgres. Even if every Celery worker crashes immediately, the rows remain and will be picked up by the stale-lease reaper / reconciler / a fresh worker.

The duplicate empty-payload `signal_jobs` and `update_lead_stage` calls that ran from the long-transcript branch with `analysis_result={}` are gone. Both are now durable stages owned by the worker pipeline; the worker reads the parent's analysis result via `depends_on_job_id` rather than receiving a (possibly stale) snapshot.

### Idempotency on the wire

A duplicate webhook delivery for the same `interaction_id` hits `uq_processing_jobs_interaction_stage` and the `ON CONFLICT (interaction_id, stage) DO NOTHING` path in `create_pipeline` returns the existing IDs unchanged. The endpoint returns 200 to both deliveries; downstream sees one pipeline.

### `/metrics` endpoint (Phase 5, new)

```
GET /metrics
200 OK
Content-Type: text/plain; version=0.0.4

# HELP llm_utilization …
# TYPE llm_utilization gauge
llm_utilization 0.412345
…
postcall_jobs{customer_id="...", state="pending"} 27
…
```

Prometheus exposition format. No auth — runs behind the cluster's network policy. Exposing it externally would leak per-customer queue depth, which is mildly sensitive.

### What's deliberately not part of the API

- **No "get analysis result" endpoint.** The dashboard reads `interactions.interaction_metadata` directly (the existing pattern); adding a separate endpoint would duplicate the source of truth.
- **No status query endpoint per interaction.** The audit-events query (§9) is the support-engineer interface; we'd build a thin operator UI over that query if/when an external customer needs it.

---

## 13. Trade-offs & Alternatives Considered

| Option considered | Why it was tempting | Why we chose otherwise |
|---|---|---|
| **Kafka as the durable substrate** instead of Postgres `processing_jobs` | Kafka's natural at-least-once + ordered-per-key delivery is a clean fit for "one interaction, one pipeline, one progress stream". Higher throughput than Postgres for write-heavy workloads. | Operational cost. Adding Kafka means adding a Kafka cluster, schema registry, consumer-offset management, and a separate alerting surface. Postgres is already in the stack; the working set fits comfortably (100K calls × 4 stages = 400K rows per campaign — trivial for a single Postgres instance). The `FOR UPDATE SKIP LOCKED` lease query handles concurrent workers correctly. We pay an extra millisecond per state transition vs. Kafka in exchange for not running another stateful service. |
| **Sidecar rate limiter** (Envoy + a token-bucket plugin, or a dedicated rate-limit microservice) | Rate-limit logic out of application code; reusable across services. | Coordinating per-customer reserved + spillover with a sidecar requires replicating the data model into the sidecar's config. The Lua-in-Redis approach co-locates the policy with the counters and runs as a single atomic op — no plugin / RPC. The trade-off accepted is that this rate limiter is voicebot-specific; if a second service ever needs the same shape, we'd extract it. |
| **Per-customer Celery queues** (one queue per customer) for AC2's isolation guarantee | Topology-level guarantee — Customer A's queue can never starve Customer B's. | Queue cardinality scales with customer count; at 100+ customers operators are managing 100+ queue specs. The reservation-counter approach gives the same structural guarantee (Customer A's reserved counter is a different Redis key from Customer B's) without the topology cost. Confirmed by `tests/test_rate_limiter.py::test_customer_a_burst_does_not_consume_customer_b_reserved`. |
| **Synchronous LLM calls in the endpoint** (no Celery) | Simpler architecture, fewer moving parts. The endpoint blocks until analysis completes. | The webhook has a 5-second response timeout. LLM calls take 1–10 seconds. A burst of 100K calls would saturate the API process pool while waiting. Decoupling the request response from the work is structural, not optional. |
| **Per-attempt Celery redelivery for the recording poll** | Simpler — each retry is a fresh Celery message; the broker handles scheduling. | At 100K calls × 5 attempts in the slow case = 500K extra dispatches. Each carries a broker round-trip plus a lease acquire/release plus an audit-row write. Phase 4's in-job loop runs the entire backoff schedule inside one leased job, with the lease sized to cover the full schedule. Same durability (lease expiry → reconciler), much less overhead. |
| **Retry-After header → `time.sleep(N)` inside the worker** for provider 429s | Simplest possible retry. | A worker holding the lease while sleeping 30s blocks one worker slot. Multiplying by burst, the worker pool gets gridlocked on sleeps. The chosen approach `defer_job(retry_in_seconds=...)` releases the lease and lets the row be re-leased after `scheduled_at` passes — workers stay productive on other jobs. |
| **A binary breaker + a "half-open" probing state** (the conventional circuit-breaker pattern) | Recover from breaker-open without manually opening. | The whole point of replacing the breaker (Phase 5) was to remove the bistability. A continuous shed curve has no "open" state to recover from — utilisation drops, dispatch probability rises. Fewer states to reason about, no probing logic to misjudge. |
| **Triage as a small-LLM call only** (skip the keyword stage) | Higher accuracy ceiling than regex; one mental model instead of two. | At ~50 tokens per Stage B call × 100K calls = 5M tokens just for triage. Stage A handles >90% of the fixture corpus deterministically in <1ms; Stage B only fires on the ambiguous ones. Cost and latency win. |
| **Encrypt PII at rest by default** in this implementation | Compliance posture stronger out of the gate. | The plumbing is non-trivial: pgcrypto symmetric envelopes, KMS bootstrap, key rotation, performance impact on JSONB queries that the dashboard depends on. Section 11 documents the hooks; deferred to keep AC1–AC10 verifiable on a clean environment. Honest about the trade-off rather than hiding it. |

---

## 14. Known Weaknesses

Items the design is aware of but did not fix in this iteration. Each is a concrete follow-up, not a wishlist.

1. **Encryption at rest is not implemented.** Hooks documented in §11 (S3 SSE-KMS for recordings, pgcrypto column-level for transcripts and lead PII). Deferred — see §13's last row.
2. **CRM push retry / status tracking is not implemented.** `signal_jobs.trigger_signal_jobs` is durable at the *stage* level (it runs inside `processing_jobs.signal_jobs`), but a partial failure across the fan-out (e.g., WhatsApp succeeded, CRM failed) is not separately tracked. The fix is a `signal_action_attempts` table keyed by `(interaction_id, action_kind)` with the same lease/retry semantics as `processing_jobs`. Same shape as Phase 1, just at the action level.
3. **Per-customer no-deploy config UI is not implemented.** Customer overrides live in `customer_budgets.customer_overrides` JSONB and are read by the triage call; a customer-facing UI to edit this would require an admin authn/authz surface and audit logging on the writes. Deferred.
4. **Triage Stage B latency on the hot-by-tone case.** A "considering" hinglish transcript routes through Stage B (~50 token / ~200ms LLM call) before it reaches the analysis stage. If the customer turns out to be hot, this adds latency. The "60-second hot end-to-end" commitment from §1 absorbs this comfortably for a single call; we have not stress-tested it under burst when both Stage A and Stage B are saturated. Documented in §6.
5. **Spillover prioritisation is first-come-first-served.** Two customers both bursting into spillover get whichever's reservation hits the Lua script first. `customer_budgets.priority_weight` is read by Phase 5 backpressure for *dialler* prioritisation, not by the rate limiter for *reservation* prioritisation. Adding weighted draws to the Lua script is straightforward but increases its complexity; deferred until contention is observed in production.
6. **Global counter mutual divergence on worker crash.** The Phase 5 global TPM counter is incremented inside the reservation Lua script atomically, but is decremented from Python in `refund` / `refund_tokens`. A worker that crashed *after* the script ran but *before* its Python `refund` ran would leak global-counter capacity for up to one minute (until the bucket auto-rolls). This briefly under-counts available capacity — i.e., the dialler sheds slightly more than it should — which is the safe direction. Acceptable; not a correctness bug.
7. **`/metrics` has no authentication.** Mounted at the root of the API; mounted-with-auth would be one decorator addition. Documented in §12.
8. **`tests/test_post_call_pipeline.py` end-to-end through Celery is mocked, not real.** The pipeline is driven by calling `_run_stage_async` directly with a stubbed `run_stage.apply_async`. Running against a live Celery worker pool would catch wiring issues this test path can't (queue routing typos, serialiser mismatches). Acceptable for the assessment; production would add a smoke test against a deployed worker.
9. **`src/models/` SQLAlchemy ORM classes are vestigial.** The pipeline uses raw SQL via `text(...)` everywhere because the JSONB merge patterns are awkward through the ORM and the queries are stable. The models are kept as schema documentation; they have no runtime references.

---

## 15. What I Would Do With More Time

Prioritised. The first three are the ones I'd ship next; everything below the line is real but lower-leverage.

1. **Encryption at rest, end to end.** Wire SSE-KMS on the recording bucket, pgcrypto on transcripts and lead PII, KMS bootstrap and rotation. Concrete next step: a migration that reads existing `interactions.conversation_data` rows in batches, encrypts them, writes back; followed by application-side reads going through the decrypt path. Estimated 1.5–2 days including operational tooling.
2. **Real end-to-end test against a live Celery worker** for `tests/test_post_call_pipeline.py`. Spin up a Celery worker as part of the test fixture, run the full burst through `apply_async` instead of `_run_stage_async`. Catches queue-routing typos and serialisation issues. Half a day.
3. **Operator dashboard over `audit_events`.** A small Flask/FastAPI admin app with one page: "find an interaction by ID, see its complete timeline". This is the most useful tool the on-call doesn't have today. Half a day.
4. **CRM push action tracking.** `signal_action_attempts` table per §14 item 2. Adds the action-level retry/dead state machine that today is implicit in the stage-level one. One day.
5. **Triage stage A measurement.** Instrument the keyword classifier to record per-disposition match rate against actual analysis outcomes (Stage A says hot, did the LLM agree?). Drives both pattern tuning and the eventual decision to replace Stage A with a small LLM. Half a day to instrument; production data drives the conclusions.
6. **Per-customer no-deploy admin UI.** Lives inside the operator dashboard from item 3; one form per customer to edit `customer_budgets` (reserved tpm/rpm, priority_weight, customer_overrides). Audit every write. One day.
7. **Stage B Bayesian calibration.** Right now Stage A's confidence threshold is hardcoded; Stage B's outcome bypasses it. Given enough data, a calibration step (Platt-scaling on Stage A confidences) would reduce Stage B invocations on transcripts where Stage A is *probably* right but didn't clear the gate. Cost is ~50 tokens × ambiguous-rate × campaign size, so the savings scale.
8. **Per-region rate limiter.** If we ever fan out to multiple regions, the global Redis becomes a coordination chokepoint. A two-tier limiter — per-region buckets that periodically reconcile against a global budget — keeps the fast path local. Real but only matters at multi-region scale.
9. **Adaptive backoff schedule for the recording poller.** Today's schedule is static (5/10/20/40/80s). Recording delivery time is bimodal (fast under load light, slow under load heavy); an adaptive schedule that learns the recent delivery distribution would reduce wasted polls under normal load. Marginal win.

The shape of the prioritisation: **observability and correctness first** (1, 2, 3, 4), then **measurement and tuning** (5, 7, 9), then **multi-region scale** (8), then **convenience UI** (6). Items low on the list are still real work — but they pay off later or in narrower conditions, and the load-bearing items above them deserve the next engineer's attention first.
