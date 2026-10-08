ALTER TABLE events
    ADD COLUMN redrive_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN last_redriven_at TIMESTAMPTZ;

ALTER TABLE events
    ADD CONSTRAINT events_redrive_count_check
    CHECK (redrive_count >= 0);

CREATE INDEX events_last_redriven_at_idx ON events (last_redriven_at);
