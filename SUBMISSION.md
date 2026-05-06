# Post-Call Processing Pipeline — Design Document

**Author:** [Candidate Name]
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

_Detail in Phase 2. Summary placeholder for now: token bucket via Redis Lua, pre-flight reservation bound to job_id, true-up against actual `usage.total_tokens`, 429 absorbed locally as a backoff signal, never propagated to the API caller._

---

## 5. Per-Customer Token Budgeting

_Detail in Phase 2. Summary placeholder for now: `customer_budgets` table with reserved TPM/RPM and priority weight; spillover bucket allocates unallocated headroom by weighted draw; hard isolation under contention._

---

## 6. Differentiated Processing

_Detail in Phase 3. Summary placeholder for now: keyword/regex classifier first, small-LLM fallback for ambiguous transcripts; `lane` and `priority` columns on `processing_jobs`; per-customer overrides via `customer_budgets.customer_overrides` JSONB._

---

## 7. Recording Pipeline

_Detail in Phase 4. Summary placeholder for now: dedicated polling stage with exponential backoff (5s → 80s, 5 attempts, jittered), every state transition emits an audit event, terminal failure produces an alert-shaped event, reconciliation job catches "uploaded but DB write failed" gaps._

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

_Detail in Phase 6. Summary placeholder: every error path emits a structured log via `src/utils/logging.py` with `interaction_id` as `correlation_id`. The `audit_events` table is the durable record; canonical query is `SELECT event_type, severity, details FROM audit_events WHERE interaction_id = $1 ORDER BY created_at`._

---

## 10. Data Model

_See Section 8 above._

---

## 11. Security

_Detail in Phase 6. Summary placeholder: transcript and recording are PII-bearing; encryption-at-rest is a deferred nice-to-have with the hooks needed to add it (separate KMS key per customer, recording S3 bucket SSE-KMS, transcripts encrypted in `interactions.conversation_data` via column-level pgcrypto)._

---

## 12. API Interface

_Detail in Phase 6. Summary placeholder: webhook contract `POST /session/{sid}/interaction/{iid}/end` is unchanged externally. Internally, the duplicate empty-payload `signal_jobs` / `update_lead_stage` calls that ran from this endpoint with `analysis_result={}` were removed in Phase 0/1; both are now durable stages owned by the worker pipeline._

---

## 13. Trade-offs & Alternatives Considered

_Detail in Phase 6._

| Option | Why Considered | Why Rejected / What You Chose Instead |
|--------|---------------|--------------------------------------|
| _to be filled in_ | _per phase_ | _per phase_ |

---

## 14. Known Weaknesses

_Detail in Phase 6. Will include:_
- Encryption at rest (deferred nice-to-have).
- CRM push retry (deferred nice-to-have).
- Per-customer no-deploy config UI (deferred nice-to-have).
- Triage classifier accuracy on the `hinglish_ambiguous` case — small-LLM fallback adds latency that may push some hot-lane calls into the 60s commitment window. Trade-off documented in Phase 3.

---

## 15. What I Would Do With More Time

_Detail in Phase 6._

1. _to be filled in_
