CREATE TABLE rate_limit_tiers (
    id                      SMALLSERIAL PRIMARY KEY,
    name                    TEXT NOT NULL UNIQUE,
    requests_per_minute     INTEGER NOT NULL,
    max_symbols_per_key     INTEGER NOT NULL,
    max_concurrent_ws_conn  INTEGER NOT NULL,
    price_per_month_cents   INTEGER,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO rate_limit_tiers (name, requests_per_minute, max_symbols_per_key, max_concurrent_ws_conn, price_per_month_cents)
VALUES
    ('free', 60, 5, 1, NULL),
    ('pro', 1000, 50, 5, 4900000),
    ('enterprise', 10000, 500, 20, NULL);