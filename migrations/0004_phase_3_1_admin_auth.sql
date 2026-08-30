CREATE TABLE admin_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash evm_hash NOT NULL UNIQUE,
    csrf_hash evm_hash NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (expires_at > created_at)
);
CREATE INDEX admin_sessions_active_idx ON admin_sessions (token_hash, expires_at)
    WHERE revoked_at IS NULL;

ALTER TABLE users
    ADD CONSTRAINT users_username_length CHECK (char_length(username) BETWEEN 3 AND 64),
    ADD COLUMN role TEXT NOT NULL DEFAULT 'ADMIN' CHECK (role = 'ADMIN');

ALTER TABLE protocol_deployments
    ADD COLUMN trust_basis TEXT NOT NULL DEFAULT 'UNTRUSTED'
        CHECK (trust_basis IN ('UNTRUSTED', 'OPERATOR_APPROVED', 'PINNED_CODE_HASH')),
    ADD COLUMN approved_by UUID REFERENCES users(id),
    ADD COLUMN approved_at TIMESTAMPTZ;

DROP INDEX protocol_deployments_active_idx;
CREATE INDEX protocol_deployments_active_idx
    ON protocol_deployments (chain_id, start_block)
    WHERE enabled AND health = 'VERIFIED'
      AND (trust_basis = 'PINNED_CODE_HASH'
           OR (trust_basis = 'OPERATOR_APPROVED' AND approved_by IS NOT NULL));
