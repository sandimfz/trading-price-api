CREATE TABLE ticks (
    symbol_id       INTEGER NOT NULL REFERENCES symbols(id),
    last_price      NUMERIC(18, 6) NOT NULL,
    change          NUMERIC(18, 6),
    change_pct      NUMERIC(8, 4),
    high_price      NUMERIC(18, 6),
    low_price       NUMERIC(18, 6),
    ts              TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable('ticks', 'ts');
        PERFORM add_retention_policy('ticks', INTERVAL '7 days');
    END IF;
END $$;