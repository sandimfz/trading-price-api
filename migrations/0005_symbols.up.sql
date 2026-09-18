CREATE TABLE symbols (
    id              SERIAL PRIMARY KEY,
    code            TEXT NOT NULL UNIQUE,
    display_name    TEXT NOT NULL,
    asset_class     TEXT NOT NULL
                        CHECK (asset_class IN ('forex', 'crypto', 'stock', 'commodity', 'index')),
    scanner_code    TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_symbols_code ON symbols (code) WHERE is_active = true;