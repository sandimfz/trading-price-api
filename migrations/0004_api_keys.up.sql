CREATE TABLE api_keys (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tier_id         SMALLINT NOT NULL REFERENCES rate_limit_tiers(id),
    key_prefix      TEXT NOT NULL UNIQUE,
    hashed_key      TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL DEFAULT 'default',
    scopes          TEXT[] NOT NULL DEFAULT ARRAY['quote:read'],
    last_used_at    TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ,
    revoked_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_api_keys_user_id ON api_keys (user_id);
CREATE INDEX idx_api_keys_hashed_key ON api_keys (hashed_key) WHERE revoked_at IS NULL;