ALTER TABLE events
    ADD CONSTRAINT events_idempotency_key_length_check
    CHECK (char_length(idempotency_key) BETWEEN 1 AND 255);
