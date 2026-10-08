CREATE TABLE events (
    id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    target_url TEXT NOT NULL,
    payload JSONB NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    last_attempt_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ,
    CONSTRAINT events_idempotency_key_unique UNIQUE (idempotency_key),
    CONSTRAINT events_status_check CHECK (
        status IN (
            'pending',
            'processing',
            'delivered',
            'retrying',
            'failed',
            'dead_letter'
        )
    )
);

CREATE INDEX events_status_idx ON events (status);

CREATE INDEX events_created_at_idx ON events (created_at);

CREATE TABLE delivery_attempts (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    status_code INTEGER,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT delivery_attempts_attempt_number_check CHECK (attempt_number > 0),
    CONSTRAINT delivery_attempts_status_check CHECK (
        status IN (
            'success',
            'transient_failure',
            'permanent_failure'
        )
    )
);

CREATE INDEX delivery_attempts_event_id_idx ON delivery_attempts (event_id);

CREATE INDEX delivery_attempts_created_at_idx ON delivery_attempts (created_at);
