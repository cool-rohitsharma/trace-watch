--liquibase formatted sql

--changeset rohit:001-telemetry-events
CREATE TABLE telemetry_events (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(100) NOT NULL,
    service_name VARCHAR(120) NOT NULL,
    endpoint VARCHAR(255) NOT NULL,
    status_code INT NOT NULL,
    latency_ms BIGINT NOT NULL,
    trace_id VARCHAR(120),
    error_message TEXT,
    event_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_telemetry_tenant_service_ts
    ON telemetry_events (tenant_id, service_name, event_timestamp DESC);

CREATE INDEX idx_telemetry_trace_id
    ON telemetry_events (trace_id) WHERE trace_id IS NOT NULL;

--changeset rohit:002-incidents
CREATE TABLE incidents (
    id BIGSERIAL PRIMARY KEY,
    incident_key VARCHAR(255) NOT NULL UNIQUE,
    tenant_id VARCHAR(100) NOT NULL,
    service_name VARCHAR(120) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'OPEN',
    error_count_in_window INT NOT NULL,
    sample_trace_id VARCHAR(120),
    sample_error_message TEXT,
    rca_summary TEXT,
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_incidents_tenant_status
    ON incidents (tenant_id, status, triggered_at DESC);