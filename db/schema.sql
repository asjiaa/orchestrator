CREATE TABLE tenants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    plan TEXT NOT NULL CHECK (plan IN ('free', 'pro', 'enterprise')),
    rate_limit_rps INT NOT NULL DEFAULT 5,
    max_concurrent INT NOT NULL DEFAULT 2,
    dispatch_weight INT NOT NULL DEFAULT 1
);

CREATE TABLE keys (
    hash TEXT PRIMARY KEY,
    tenant_id UUID  NOT NULL REFERENCES tenants(id)
);

CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'complete', 'failed', 'dead')),
    attempts  INT NOT NULL DEFAULT 0,
    payload JSONB,
    input_key TEXT,
    result_key TEXT,
    worker_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX jobs_tenant_status ON jobs (tenant_id, status);

CREATE INDEX jobs_processing_updated ON jobs (updated_at) WHERE status = 'processing';

ALTER TABLE jobs ADD COLUMN processing_started_at TIMESTAMPTZ;