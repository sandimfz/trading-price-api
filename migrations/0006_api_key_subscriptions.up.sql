CREATE TABLE api_key_subscriptions (
    id              BIGSERIAL PRIMARY KEY,
    api_key_id      UUID NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    symbol_id       INTEGER NOT NULL REFERENCES symbols(id),
    subscribed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    unsubscribed_at TIMESTAMPTZ,

    UNIQUE (api_key_id, symbol_id, subscribed_at)
);

CREATE INDEX idx_subscriptions_active
    ON api_key_subscriptions (api_key_id, symbol_id)
    WHERE unsubscribed_at IS NULL;