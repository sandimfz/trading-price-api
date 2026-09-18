CREATE TABLE billing_events (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    event_type      TEXT NOT NULL
                        CHECK (event_type IN ('subscription_created', 'subscription_renewed',
                                                'subscription_cancelled', 'payment_failed', 'tier_changed')),
    tier_id         SMALLINT REFERENCES rate_limit_tiers(id),
    amount_cents    INTEGER,
    metadata        JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_billing_events_user_id ON billing_events (user_id, created_at DESC);