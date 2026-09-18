CREATE TABLE api_key_usage_logs (
    id              BIGSERIAL,
    api_key_id      UUID NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    endpoint        TEXT NOT NULL,
    status_code     SMALLINT NOT NULL,
    latency_ms      INTEGER NOT NULL,
    ip_address      INET,
    requested_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable('api_key_usage_logs', 'requested_at');
    END IF;
END $$;

CREATE INDEX idx_usage_logs_api_key_id ON api_key_usage_logs (api_key_id, requested_at DESC);