package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/julimeimei/eventrelay-go/internal/delivery"
	"github.com/julimeimei/eventrelay-go/internal/domain"
	"github.com/julimeimei/eventrelay-go/internal/queue"
	"github.com/julimeimei/eventrelay-go/internal/repository"
)

func TestWorkerProcessEventCreatedSuccess(t *testing.T) {
	t.Parallel()

	store := repository.NewInMemoryEventRepository()
	event := testWorkerEvent(t)
	if err := store.CreateEvent(context.Background(), event); err != nil {
		t.Fatalf("create event: %v", err)
	}

	worker := NewWorker(store, &fakeDeliveryClient{
		result: delivery.Result{StatusCode: httpStatusNoContent, Success: true},
	})

	if err := worker.ProcessEventCreated(context.Background(), queue.EventCreatedMessage{EventID: event.ID}); err != nil {
		t.Fatalf("process event: %v", err)
	}

	found, err := store.GetEventByID(context.Background(), event.ID)
	if err != nil {
		t.Fatalf("get event: %v", err)
	}
	if found.Status != domain.EventStatusDelivered {
		t.Fatalf("expected status delivered, got %q", found.Status)
	}

	count, err := store.CountDeliveryAttempts(context.Background(), event.ID)
	if err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 attempt, got %d", count)
	}
}

func TestWorkerProcessEventCreatedTransientFailureSchedulesRetry(t *testing.T) {
	t.Parallel()

	store := repository.NewInMemoryEventRepository()
	event := testWorkerEvent(t)
	if err := store.CreateEvent(context.Background(), event); err != nil {
		t.Fatalf("create event: %v", err)
	}

	worker := NewWorker(store, &fakeDeliveryClient{
		result: delivery.Result{StatusCode: 500, Success: false},
	}, delivery.NewRetryPolicy(3, time.Second, time.Second))
	worker.sleep = func(_ context.Context, _ time.Duration) error { return nil }

	err := worker.ProcessEventCreated(context.Background(), queue.EventCreatedMessage{EventID: event.ID})
	if !errors.Is(err, ErrRetryScheduled) {
		t.Fatalf("expected retry scheduled error, got %v", err)
	}

	found, err := store.GetEventByID(context.Background(), event.ID)
	if err != nil {
		t.Fatalf("get event: %v", err)
	}
	if found.Status != domain.EventStatusRetrying {
		t.Fatalf("expected status retrying, got %q", found.Status)
	}
	if found.NextAttemptAt == nil {
		t.Fatal("expected next attempt time")
	}
}

func TestWorkerProcessEventCreatedPermanentFailureDeadLetters(t *testing.T) {
	t.Parallel()

	store := repository.NewInMemoryEventRepository()
	event := testWorkerEvent(t)
	if err := store.CreateEvent(context.Background(), event); err != nil {
		t.Fatalf("create event: %v", err)
	}

	worker := NewWorker(store, &fakeDeliveryClient{
		result: delivery.Result{StatusCode: 400, Success: false},
	})

	err := worker.ProcessEventCreated(context.Background(), queue.EventCreatedMessage{EventID: event.ID})
	if !errors.Is(err, queue.ErrDeadLetter) {
		t.Fatalf("expected dead letter error, got %v", err)
	}

	found, err := store.GetEventByID(context.Background(), event.ID)
	if err != nil {
		t.Fatalf("get event: %v", err)
	}
	if found.Status != domain.EventStatusDeadLetter {
		t.Fatalf("expected status dead_letter, got %q", found.Status)
	}
	if found.NextAttemptAt != nil {
		t.Fatal("expected no next attempt time")
	}
}

func TestWorkerProcessEventCreatedTransientFailureDeadLettersAtMaxAttempts(t *testing.T) {
	t.Parallel()

	store := repository.NewInMemoryEventRepository()
	event := testWorkerEvent(t)
	if err := store.CreateEvent(context.Background(), event); err != nil {
		t.Fatalf("create event: %v", err)
	}

	worker := NewWorker(store, &fakeDeliveryClient{
		result: delivery.Result{StatusCode: 503, Success: false},
	}, delivery.NewRetryPolicy(1, time.Second, time.Second))
	worker.sleep = func(_ context.Context, _ time.Duration) error { return nil }

	err := worker.ProcessEventCreated(context.Background(), queue.EventCreatedMessage{EventID: event.ID})
	if !errors.Is(err, queue.ErrDeadLetter) {
		t.Fatalf("expected dead letter error, got %v", err)
	}

	found, err := store.GetEventByID(context.Background(), event.ID)
	if err != nil {
		t.Fatalf("get event: %v", err)
	}
	if found.Status != domain.EventStatusDeadLetter {
		t.Fatalf("expected status dead_letter, got %q", found.Status)
	}
	if found.NextAttemptAt != nil {
		t.Fatal("expected no next attempt time")
	}
}

func TestWorkerProcessEventCreatedIgnoresMissingEvent(t *testing.T) {
	t.Parallel()

	worker := NewWorker(repository.NewInMemoryEventRepository(), &fakeDeliveryClient{
		err: errors.New("should not be called"),
	})

	if err := worker.ProcessEventCreated(context.Background(), queue.EventCreatedMessage{EventID: "missing"}); err != nil {
		t.Fatalf("expected missing event to be ignored, got %v", err)
	}
}

type fakeDeliveryClient struct {
	result delivery.Result
	err    error
}

func (c *fakeDeliveryClient) Deliver(_ context.Context, _ domain.Event) (delivery.Result, error) {
	return c.result, c.err
}

func testWorkerEvent(t *testing.T) domain.Event {
	t.Helper()

	id, err := domain.NewID()
	if err != nil {
		t.Fatalf("new id: %v", err)
	}

	now := time.Now().UTC()
	return domain.Event{
		ID:             id,
		Type:           "payment.approved",
		TargetURL:      "https://example.com/webhooks",
		Payload:        json.RawMessage(`{"order_id":"ord_123"}`),
		IdempotencyKey: "event-123",
		RequestHash:    "hash",
		Status:         domain.EventStatusPending,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

const httpStatusNoContent = 204
