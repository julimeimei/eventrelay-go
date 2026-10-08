package repository

import "errors"

var (
	ErrEventNotFound           = errors.New("event not found")
	ErrDuplicateIdempotencyKey = errors.New("duplicate idempotency key")
)
