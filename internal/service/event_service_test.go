package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/julimeimei/eventrelay-go/internal/domain"
	"github.com/julimeimei/eventrelay-go/internal/observability"
	"github.com/julimeimei/eventrelay-go/internal/queue"
	"github.com/julimeimei/eventrelay-go/internal/repository"
)

func TestInMemoryEventServiceCreateAndGet(t *testing.T) {
	t.Parallel()

	events := NewInMemoryEventService()

	result, err := events.CreateEvent(context.Background(), CreateEventInput{
		Type:           "payment.approved",
		TargetURL:      "https://example.com/webhooks",
		Payload:        json.RawMessage(`{"order_id":"ord_123"}`),
		IdempotencyKey: "event-123",
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}

	if !result.Created {
		t.Fatal("expected event to be created")
	}

	found, err := events.GetEvent(context.Background(), result.Event.ID)
	if err != nil {
		t.Fatalf("get event: %v", err)
	}

	if found.ID != result.Event.ID {
		t.Fatalf("expected id %q, got %q", result.Event.ID, found.ID)
	}
}

func TestEventServicePublishesCreatedEvent(t *testing.T) {
	t.Parallel()

	publisher := &fakeEventPublisher{}
	events := NewEventService(
		NewInMemoryEventService().events,
		publisher,
	)

	result, err := events.CreateEvent(context.Background(), CreateEventInput{
		Type:           "payment.approved",
		TargetURL:      "https://example.com/webhooks",
		Payload:        json.RawMessage(`{"order_id":"ord_123"}`),
		IdempotencyKey: "event-123",
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}

	if len(publisher.messages) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(publisher.messages))
	}
	if publisher.messages[0].EventID != result.Event.ID {
		t.Fatalf("expected message event id %q, got %q", result.Event.ID, publisher.messages[0].EventID)
	}
}

func TestEventServicePublishesCorrelationID(t *testing.T) {
	t.Parallel()

	publisher := &fakeEventPublisher{}
	events := NewEventService(
		NewInMemoryEventService().events,
		publisher,
	)
	ctx := observability.WithRequestID(context.Background(), "req-123")

	_, err := events.CreateEvent(ctx, CreateEventInput{
		Type:           "payment.approved",
		TargetURL:      "https://example.com/webhooks",
		Payload:        json.RawMessage(`{"order_id":"ord_123"}`),
		IdempotencyKey: "event-123",
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}

	if len(publisher.messages) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(publisher.messages))
	}
	if publisher.messages[0].CorrelationID != "req-123" {
		t.Fatalf("expected correlation id %q, got %q", "req-123", publisher.messages[0].CorrelationID)
	}
}

func TestEventServiceDoesNotPublishDuplicateIdempotencyKey(t *testing.T) {
	t.Parallel()

	publisher := &fakeEventPublisher{}
	events := NewEventService(
		NewInMemoryEventService().events,
		publisher,
	)

	input := CreateEventInput{
		Type:           "payment.approved",
		TargetURL:      "https://example.com/webhooks",
		Payload:        json.RawMessage(`{"order_id":"ord_123"}`),
		IdempotencyKey: "event-123",
	}

	if _, err := events.CreateEvent(context.Background(), input); err != nil {
		t.Fatalf("create event: %v", err)
	}
	if _, err := events.CreateEvent(context.Background(), input); err != nil {
		t.Fatalf("create duplicate event: %v", err)
	}

	if len(publisher.messages) != 1 {
		t.Fatalf("expected duplicate idempotency key to publish once, got %d", len(publisher.messages))
	}
}

func TestEventServiceTreatsCanonicalJSONAsSameIdempotencyRequest(t *testing.T) {
	t.Parallel()

	publisher := &fakeEventPublisher{}
	events := NewEventService(
		NewInMemoryEventService().events,
		publisher,
	)

	first, err := events.CreateEvent(context.Background(), CreateEventInput{
		Type:           "payment.approved",
		TargetURL:      "https://example.com/webhooks",
		Payload:        json.RawMessage(`{"amount":9900,"order_id":"ord_123"}`),
		IdempotencyKey: "event-123",
	})
	if err != nil {
		t.Fatalf("create first event: %v", err)
	}

	second, err := events.CreateEvent(context.Background(), CreateEventInput{
		Type:      "payment.approved",
		TargetURL: "https://example.com/webhooks",
		Payload: json.RawMessage(`{
			"order_id": "ord_123",
			"amount": 9900
		}`),
		IdempotencyKey: "event-123",
	})
	if err != nil {
		t.Fatalf("create duplicate event with canonical payload: %v", err)
	}

	if second.Created {
		t.Fatal("expected duplicate canonical request to return existing event")
	}
	if first.Event.ID != second.Event.ID {
		t.Fatalf("expected duplicate id %q, got %q", first.Event.ID, second.Event.ID)
	}
	if len(publisher.messages) != 1 {
		t.Fatalf("expected duplicate canonical request to publish once, got %d", len(publisher.messages))
	}
}

func TestEventServiceReturnsErrorWhenPublishFails(t *testing.T) {
	t.Parallel()

	events := NewEventService(
		NewInMemoryEventService().events,
		&fakeEventPublisher{err: errors.New("publish failed")},
	)

	_, err := events.CreateEvent(context.Background(), CreateEventInput{
		Type:           "payment.approved",
		TargetURL:      "https://example.com/webhooks",
		Payload:        json.RawMessage(`{"order_id":"ord_123"}`),
		IdempotencyKey: "event-123",
	})
	if err == nil {
		t.Fatal("expected publish error")
	}
}

func TestInMemoryEventServiceRejectsDuplicateKeyWithDifferentPayload(t *testing.T) {
	t.Parallel()

	events := NewInMemoryEventService()

	_, err := events.CreateEvent(context.Background(), CreateEventInput{
		Type:           "payment.approved",
		TargetURL:      "https://example.com/webhooks",
		Payload:        json.RawMessage(`{"order_id":"ord_123"}`),
		IdempotencyKey: "event-123",
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}

	_, err = events.CreateEvent(context.Background(), CreateEventInput{
		Type:           "payment.approved",
		TargetURL:      "https://example.com/webhooks",
		Payload:        json.RawMessage(`{"order_id":"ord_456"}`),
		IdempotencyKey: "event-123",
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
}

func TestEventServiceRedrivesDeadLetterEvent(t *testing.T) {
	t.Parallel()

	ctx := observability.WithRequestID(context.Background(), "req-123")
	store := repository.NewInMemoryEventRepository()
	publisher := &fakeEventPublisher{}
	events := NewEventService(store, publisher)

	result, err := events.CreateEvent(ctx, CreateEventInput{
		Type:           "payment.approved",
		TargetURL:      "https://example.com/webhooks",
		Payload:        json.RawMessage(`{"order_id":"ord_123"}`),
		IdempotencyKey: "event-123",
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	if err := store.UpdateEventStatus(ctx, result.Event.ID, domain.EventStatusDeadLetter, nil, nil); err != nil {
		t.Fatalf("mark dead letter: %v", err)
	}

	redriven, err := events.RedriveEvent(ctx, result.Event.ID)
	if err != nil {
		t.Fatalf("redrive event: %v", err)
	}

	if redriven.Status != domain.EventStatusPending {
		t.Fatalf("expected status pending, got %q", redriven.Status)
	}
	if redriven.RedriveCount != 1 {
		t.Fatalf("expected redrive count 1, got %d", redriven.RedriveCount)
	}
	if redriven.LastRedrivenAt == nil {
		t.Fatal("expected last redriven time")
	}
	if redriven.LastAttemptAt != nil || redriven.NextAttemptAt != nil {
		t.Fatal("expected redrive to clear attempt timestamps")
	}
	if len(publisher.messages) != 2 {
		t.Fatalf("expected create and redrive messages, got %d", len(publisher.messages))
	}
	lastMessage := publisher.messages[len(publisher.messages)-1]
	if lastMessage.EventID != result.Event.ID {
		t.Fatalf("expected redrive message event id %q, got %q", result.Event.ID, lastMessage.EventID)
	}
	if lastMessage.CorrelationID != "req-123" {
		t.Fatalf("expected correlation id req-123, got %q", lastMessage.CorrelationID)
	}
}

func TestEventServiceRejectsRedriveForNonDeadLetterEvent(t *testing.T) {
	t.Parallel()

	store := repository.NewInMemoryEventRepository()
	publisher := &fakeEventPublisher{}
	events := NewEventService(store, publisher)

	result, err := events.CreateEvent(context.Background(), CreateEventInput{
		Type:           "payment.approved",
		TargetURL:      "https://example.com/webhooks",
		Payload:        json.RawMessage(`{"order_id":"ord_123"}`),
		IdempotencyKey: "event-123",
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}

	_, err = events.RedriveEvent(context.Background(), result.Event.ID)
	if !errors.Is(err, ErrEventNotRedrivable) {
		t.Fatalf("expected ErrEventNotRedrivable, got %v", err)
	}
	if len(publisher.messages) != 1 {
		t.Fatalf("expected no redrive message, got %d messages", len(publisher.messages))
	}
}

func TestEventServiceReturnsNotFoundForMissingRedriveEvent(t *testing.T) {
	t.Parallel()

	events := NewEventService(repository.NewInMemoryEventRepository())

	_, err := events.RedriveEvent(context.Background(), "missing")
	if !errors.Is(err, ErrEventNotFound) {
		t.Fatalf("expected ErrEventNotFound, got %v", err)
	}
}

type fakeEventPublisher struct {
	messages []queue.EventCreatedMessage
	err      error
}

func TestInMemoryEventRepositoryCountsAttemptsSinceRedrive(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := repository.NewInMemoryEventRepository()
	event := domain.Event{
		ID:             "7d87aa1a-c8d7-45f7-9f67-e9d9204f7111",
		Type:           "payment.approved",
		TargetURL:      "https://example.com/webhooks",
		Payload:        json.RawMessage(`{"order_id":"ord_123"}`),
		IdempotencyKey: "event-123",
		RequestHash:    "hash",
		Status:         domain.EventStatusPending,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
	if err := store.CreateEvent(ctx, event); err != nil {
		t.Fatalf("create event: %v", err)
	}

	redrivenAt := time.Now().UTC()
	before := redrivenAt.Add(-time.Second)
	after := redrivenAt.Add(time.Second)

	for _, createdAt := range []time.Time{before, after} {
		attemptID, err := domain.NewID()
		if err != nil {
			t.Fatalf("new id: %v", err)
		}
		if err := store.CreateDeliveryAttempt(ctx, domain.DeliveryAttempt{
			ID:            attemptID,
			EventID:       event.ID,
			AttemptNumber: 1,
			Status:        domain.DeliveryAttemptStatusTransientFailure,
			CreatedAt:     createdAt,
		}); err != nil {
			t.Fatalf("create attempt: %v", err)
		}
	}

	count, err := store.CountDeliveryAttemptsSince(ctx, event.ID, &redrivenAt)
	if err != nil {
		t.Fatalf("count attempts since redrive: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 attempt after redrive, got %d", count)
	}
}

func (p *fakeEventPublisher) PublishEventCreated(_ context.Context, message queue.EventCreatedMessage) error {
	if p.err != nil {
		return p.err
	}

	p.messages = append(p.messages, message)
	return nil
}

func TestInMemoryEventServiceRequiresValidInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input CreateEventInput
		err   error
	}{
		{
			name: "missing idempotency key",
			input: CreateEventInput{
				Type:      "payment.approved",
				TargetURL: "https://example.com/webhooks",
				Payload:   json.RawMessage(`{}`),
			},
			err: ErrIdempotencyKeyMissing,
		},
		{
			name: "idempotency key too long",
			input: CreateEventInput{
				Type:           "payment.approved",
				TargetURL:      "https://example.com/webhooks",
				Payload:        json.RawMessage(`{}`),
				IdempotencyKey: strings.Repeat("a", maxIdempotencyKeyLength+1),
			},
			err: ErrInvalidIdempotencyKey,
		},
		{
			name: "idempotency key with control character",
			input: CreateEventInput{
				Type:           "payment.approved",
				TargetURL:      "https://example.com/webhooks",
				Payload:        json.RawMessage(`{}`),
				IdempotencyKey: "event-\x7f",
			},
			err: ErrInvalidIdempotencyKey,
		},
		{
			name: "missing type",
			input: CreateEventInput{
				TargetURL:      "https://example.com/webhooks",
				Payload:        json.RawMessage(`{}`),
				IdempotencyKey: "event-123",
			},
			err: ErrInvalidEvent,
		},
		{
			name: "invalid target url",
			input: CreateEventInput{
				Type:           "payment.approved",
				TargetURL:      "ftp://example.com/webhooks",
				Payload:        json.RawMessage(`{}`),
				IdempotencyKey: "event-123",
			},
			err: ErrInvalidEvent,
		},
		{
			name: "invalid payload",
			input: CreateEventInput{
				Type:           "payment.approved",
				TargetURL:      "https://example.com/webhooks",
				Payload:        json.RawMessage(`{`),
				IdempotencyKey: "event-123",
			},
			err: ErrInvalidEvent,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			events := NewInMemoryEventService()
			_, err := events.CreateEvent(context.Background(), test.input)
			if !errors.Is(err, test.err) {
				t.Fatalf("expected %v, got %v", test.err, err)
			}
		})
	}
}
