package domain

import (
	"encoding/json"
	"time"
)

type EventStatus string

const (
	EventStatusPending    EventStatus = "pending"
	EventStatusProcessing EventStatus = "processing"
	EventStatusDelivered  EventStatus = "delivered"
	EventStatusRetrying   EventStatus = "retrying"
	EventStatusFailed     EventStatus = "failed"
	EventStatusDeadLetter EventStatus = "dead_letter"
)

type Event struct {
	ID             string
	Type           string
	TargetURL      string
	Payload        json.RawMessage
	IdempotencyKey string
	RequestHash    string
	Status         EventStatus
	CreatedAt      time.Time
	UpdatedAt      time.Time
	LastAttemptAt  *time.Time
	NextAttemptAt  *time.Time
	RedriveCount   int
	LastRedrivenAt *time.Time
}
