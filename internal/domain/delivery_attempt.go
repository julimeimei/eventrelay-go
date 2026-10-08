package domain

import "time"

type DeliveryAttemptStatus string

const (
	DeliveryAttemptStatusSuccess          DeliveryAttemptStatus = "success"
	DeliveryAttemptStatusTransientFailure DeliveryAttemptStatus = "transient_failure"
	DeliveryAttemptStatusPermanentFailure DeliveryAttemptStatus = "permanent_failure"
)

type DeliveryAttempt struct {
	ID            string
	EventID       string
	AttemptNumber int
	Status        DeliveryAttemptStatus
	StatusCode    *int
	ErrorMessage  *string
	CreatedAt     time.Time
}
