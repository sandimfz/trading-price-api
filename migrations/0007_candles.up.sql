CREATE TABLE candles (
    symbol_id       INTEGER NOT NULL REFERENCES symbols(id),
    interval        TEXT NOT NULL
                        CHECK (interval IN ('1m', '5m', '15m', '30m', '1h', '4h', '1d')),
    open            NUMERIC(18, 6) NOT NULL,
    high            NUMERIC(18, 6) NOT NULL,
    low             NUMERIC(18, 6) NOT NULL,
    close           NUMERIC(18, 6) NOT NULL,
    volume          NUMERIC(24, 6) NOT NULL DEFAULT 0,
    bucket_start    TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (symbol_id, interval, bucket_start)
);

-- TimescaleDB: jadikan hypertable + kompresi + retensi kalau ekstensi terpasang.
-- Tanpa TimescaleDB (Postgres biasa) tabel tetap berfungsi sebagai tabel normal.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable('candles', 'bucket_start');
        ALTER TABLE candles SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'symbol_id, interval'
        );
        PERFORM add_compression_policy('candles', INTERVAL '30 days');
        PERFORM add_retention_policy('candles', INTERVAL '2 years');
    END IF;
END $$;

CREATE INDEX idx_candles_lookup
    ON candles (symbol_id, interval, bucket_start DESC);