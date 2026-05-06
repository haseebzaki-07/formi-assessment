-- VoiceBot Post-Call Processing — Database Schema
-- This schema represents the CURRENT state of the system.
-- Candidates should propose schema changes as part of their solution.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE leads (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    name VARCHAR(255),
    phone VARCHAR(50),
    email VARCHAR(255),
    stage VARCHAR(100) DEFAULT 'new',
    lead_data JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_leads_campaign ON leads(campaign_id);
CREATE INDEX idx_leads_customer ON leads(customer_id);

CREATE TABLE sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lead_id UUID NOT NULL REFERENCES leads(id),
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    agent_id UUID NOT NULL,
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_sessions_lead ON sessions(lead_id);
CREATE INDEX idx_sessions_campaign ON sessions(campaign_id);

CREATE TABLE interactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES sessions(id),
    lead_id UUID NOT NULL REFERENCES leads(id),
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    agent_id UUID NOT NULL,

    status VARCHAR(20) DEFAULT 'INITIATED',
    call_sid VARCHAR(255),
    call_provider VARCHAR(50) DEFAULT 'exotel',

    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    duration_seconds INTEGER,

    -- Transcript stored here: conversation_data->'transcript' is a JSON array
    -- of {"role": "agent"|"customer", "content": "..."}
    conversation_data JSONB DEFAULT '{}',

    -- Hot cache for dashboard. Contains extracted entities, analysis status,
    -- call_stage, and other dashboard-facing fields.
    -- Structure: {"entities": {...}, "call_stage": "...", "analysis_status": "..."}
    interaction_metadata JSONB DEFAULT '{}',

    recording_url TEXT,
    recording_s3_key VARCHAR(512),

    -- Current Celery task tracking (no workflow visibility)
    postcall_celery_task_id VARCHAR(255),

    retry_count INTEGER DEFAULT 0,
    error_log JSONB DEFAULT '[]',

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_interactions_session ON interactions(session_id);
CREATE INDEX idx_interactions_lead ON interactions(lead_id);
CREATE INDEX idx_interactions_campaign ON interactions(campaign_id);
CREATE INDEX idx_interactions_customer ON interactions(customer_id);
CREATE INDEX idx_interactions_call_sid ON interactions(call_sid);
CREATE INDEX idx_interactions_status ON interactions(status);

-- ─── Phase 1: Durable execution ────────────────────────────────────────────
-- processing_jobs is the source of truth for post-call pipeline state. One
-- row per (interaction, stage). The Celery broker is the executor; this table
-- is the durable record. A worker crash leaves the row in 'leased' until the
-- lease expires, after which any worker can pick it up via lease_next_stage.
CREATE TABLE processing_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,

    -- Pipeline stage. recording and analysis run in parallel; signal_jobs and
    -- lead_stage depend on analysis completing (depends_on_job_id).
    stage VARCHAR(32) NOT NULL,

    -- pending → leased → completed | failed → (retry: pending) | dead | skipped
    state VARCHAR(16) NOT NULL DEFAULT 'pending',

    -- Lower number = more urgent. Default 5 = normal. Phase 3 will set 1 for
    -- hot lane, 9 for cold lane.
    priority SMALLINT NOT NULL DEFAULT 5,
    lane VARCHAR(16),

    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,

    -- Phase 2: rate-limit deferrals are distinct from work failures.
    -- defer_count tracks "rate-limited; please try later" deferrals; it does
    -- not consume the work-attempt budget. A job that defers more than
    -- max_defers times is moved to `dead` so a permanently-constrained
    -- customer can't spin forever.
    defer_count INTEGER NOT NULL DEFAULT 0,
    max_defers INTEGER NOT NULL DEFAULT 100,


    -- Optimistic lease: a worker writes lease_token on lease, and every
    -- subsequent state transition checks WHERE lease_token = $1. A second
    -- worker that picks up the same row after lease expiry gets a different
    -- token, so the original worker's late completion is a no-op.
    lease_token UUID,
    leased_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    worker_id VARCHAR(128),

    payload JSONB NOT NULL DEFAULT '{}',
    result JSONB,
    last_error TEXT,

    -- scheduled_at is when this stage becomes eligible. Phase 2's rate
    -- limiter pushes this forward when budget is exhausted; Phase 3's cold
    -- lane uses it for batched-window deferral.
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    depends_on_job_id UUID REFERENCES processing_jobs(id),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Idempotency at the table level: one (interaction, stage) row, ever. A
-- duplicate webhook delivery cannot create a second pipeline.
CREATE UNIQUE INDEX uq_processing_jobs_interaction_stage
    ON processing_jobs(interaction_id, stage);

-- Lease-next-pending query: state='pending' AND scheduled_at <= now ORDER BY
-- priority, scheduled_at. Partial index keeps it small at 100K-row scale.
CREATE INDEX idx_processing_jobs_pending
    ON processing_jobs(stage, priority, scheduled_at)
    WHERE state = 'pending';

-- Find stale leases (worker crashed mid-stage) for the reaper.
CREATE INDEX idx_processing_jobs_leased
    ON processing_jobs(lease_expires_at)
    WHERE state = 'leased';

-- Per-customer queue depth and active-work queries.
CREATE INDEX idx_processing_jobs_customer_state
    ON processing_jobs(customer_id, state);

CREATE INDEX idx_processing_jobs_interaction
    ON processing_jobs(interaction_id);

-- audit_events is append-only. Every state transition and external-system
-- interaction (LLM call, recording fetch, signal job dispatch) writes a row.
-- This is the table an on-call engineer queries to debug a specific
-- interaction 3 days later (AC5).
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

-- ─── Phase 2: Per-customer rate-limit budgets and token-usage ledger ───────
-- customer_budgets defines each customer's reserved share of TPM/RPM. The
-- sum of all reserved_tpm should be <= LLM_TOKENS_PER_MINUTE; the remainder
-- is the global "spillover" pool that any customer can dip into after their
-- reserved share is exhausted (see src/services/rate_limiter.py for the
-- enforcement logic). priority_weight is used by Phase 5 backpressure to
-- bias dispatch under contention.
--
-- A customer with no row here gets a default of zero reserved — they can
-- still run, but every reservation comes from spillover and gets deprioritised.
CREATE TABLE customer_budgets (
    customer_id UUID PRIMARY KEY,
    reserved_tpm INTEGER NOT NULL DEFAULT 0,
    reserved_rpm INTEGER NOT NULL DEFAULT 0,
    priority_weight SMALLINT NOT NULL DEFAULT 1,
    burst_multiplier NUMERIC(4,2) NOT NULL DEFAULT 1.5,

    -- Phase 3 places per-customer lane overrides here so a customer can
    -- opt specific outcomes from cold→hot or vice versa without a deploy.
    customer_overrides JSONB NOT NULL DEFAULT '{}',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- token_usage is the append-only billing-grade ledger. Every LLM call
-- writes one row with the actual usage.total_tokens from the provider's
-- response. tokens_estimated is what the rate limiter reserved up-front;
-- the delta (actual - estimated) is the true-up amount applied to the
-- bucket. source distinguishes which pool the reservation came from.
CREATE TABLE token_usage (
    id BIGSERIAL PRIMARY KEY,
    interaction_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    job_id UUID NOT NULL,
    tokens_estimated INTEGER NOT NULL,
    tokens_actual INTEGER NOT NULL,
    cost_micros BIGINT,
    source VARCHAR(16) NOT NULL DEFAULT 'reserved',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_token_usage_customer_time ON token_usage(customer_id, created_at);
CREATE INDEX idx_token_usage_interaction ON token_usage(interaction_id);
CREATE INDEX idx_token_usage_campaign_time ON token_usage(campaign_id, created_at);

-- Seed data: sample customer budgets matching the seeded customer IDs.
-- Customer 1 (campaigns d0000000…0001): mid-tier, 30k TPM reserved.
-- Customer 2 (campaigns d0000000…0002): smaller, 15k TPM reserved.
-- Sum = 45k, leaving 45k spillover out of LLM_TOKENS_PER_MINUTE = 90k default.
INSERT INTO customer_budgets (customer_id, reserved_tpm, reserved_rpm, priority_weight) VALUES
    ('d0000000-0000-0000-0000-000000000001', 30000, 150, 2),
    ('d0000000-0000-0000-0000-000000000002', 15000, 75, 1);

-- Seed data: sample interactions for testing
-- (Uses fixed UUIDs for reproducibility)

INSERT INTO leads (id, campaign_id, customer_id, name, phone, stage) VALUES
    ('a0000000-0000-0000-0000-000000000001', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'Rahul Sharma', '+919876543210', 'contacted'),
    ('a0000000-0000-0000-0000-000000000002', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'Priya Gupta', '+919876543211', 'new'),
    ('a0000000-0000-0000-0000-000000000003', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'Amit Verma', '+919876543212', 'contacted'),
    ('a0000000-0000-0000-0000-000000000004', 'c0000000-0000-0000-0000-000000000002', 'd0000000-0000-0000-0000-000000000002', 'Neha Patel', '+919876543213', 'new'),
    ('a0000000-0000-0000-0000-000000000005', 'c0000000-0000-0000-0000-000000000002', 'd0000000-0000-0000-0000-000000000002', 'Rajesh Kumar', '+919876543214', 'contacted');

INSERT INTO sessions (id, lead_id, campaign_id, customer_id, agent_id, status) VALUES
    ('b0000000-0000-0000-0000-000000000001', 'a0000000-0000-0000-0000-000000000001', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'e0000000-0000-0000-0000-000000000001', 'COMPLETED'),
    ('b0000000-0000-0000-0000-000000000002', 'a0000000-0000-0000-0000-000000000002', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'e0000000-0000-0000-0000-000000000001', 'COMPLETED'),
    ('b0000000-0000-0000-0000-000000000003', 'a0000000-0000-0000-0000-000000000003', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'e0000000-0000-0000-0000-000000000001', 'COMPLETED');

INSERT INTO interactions (id, session_id, lead_id, campaign_id, customer_id, agent_id, status, call_sid, duration_seconds, started_at, ended_at, conversation_data, interaction_metadata) VALUES
    (
        'f0000000-0000-0000-0000-000000000001',
        'b0000000-0000-0000-0000-000000000001',
        'a0000000-0000-0000-0000-000000000001',
        'c0000000-0000-0000-0000-000000000001',
        'd0000000-0000-0000-0000-000000000001',
        'e0000000-0000-0000-0000-000000000001',
        'ENDED',
        'exotel-call-001',
        180,
        NOW() - INTERVAL '10 minutes',
        NOW() - INTERVAL '7 minutes',
        '{"transcript": [{"role": "agent", "content": "Hello, am I speaking with Mr. Sharma?"}, {"role": "customer", "content": "Haan ji"}, {"role": "agent", "content": "I am calling from Cashify regarding your phone evaluation. Can we reschedule?"}, {"role": "customer", "content": "Tomorrow 3:30 PM works"}, {"role": "agent", "content": "Confirmed, our executive will visit tomorrow at 3:30 PM"}, {"role": "customer", "content": "Okay, confirmed. Bye."}]}',
        '{"analysis_status": "pending"}'
    ),
    (
        'f0000000-0000-0000-0000-000000000002',
        'b0000000-0000-0000-0000-000000000002',
        'a0000000-0000-0000-0000-000000000002',
        'c0000000-0000-0000-0000-000000000001',
        'd0000000-0000-0000-0000-000000000001',
        'e0000000-0000-0000-0000-000000000001',
        'ENDED',
        'exotel-call-002',
        45,
        NOW() - INTERVAL '15 minutes',
        NOW() - INTERVAL '14 minutes',
        '{"transcript": [{"role": "agent", "content": "Hello, am I speaking with Ms. Gupta?"}, {"role": "customer", "content": "Not interested, dont call again"}, {"role": "agent", "content": "Sorry for the inconvenience. Have a good day."}]}',
        '{"analysis_status": "pending"}'
    ),
    (
        'f0000000-0000-0000-0000-000000000003',
        'b0000000-0000-0000-0000-000000000003',
        'a0000000-0000-0000-0000-000000000003',
        'c0000000-0000-0000-0000-000000000001',
        'd0000000-0000-0000-0000-000000000001',
        'e0000000-0000-0000-0000-000000000001',
        'ENDED',
        'exotel-call-003',
        15,
        NOW() - INTERVAL '20 minutes',
        NOW() - INTERVAL '19 minutes',
        '{"transcript": [{"role": "agent", "content": "Hello—"}, {"role": "customer", "content": "Wrong number"}]}',
        '{"analysis_status": "pending"}'
    );
