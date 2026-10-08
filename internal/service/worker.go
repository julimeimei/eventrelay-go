package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/julimeimei/eventrelay-go/internal/delivery"
	"github.com/julimeimei/eventrelay-go/internal/domain"
	"github.com/julimeimei/eventrelay-go/internal/observability"
	"github.com/julimeimei/eventrelay-go/internal/queue"
	"github.com/julimeimei/eventrelay-go/internal/repository"
)

var ErrRetryScheduled = errors.New("delivery retry scheduled")

type DeliveryStore interface {
	GetEventByID(ctx context.Context, id string) (domain.Event, error)
	UpdateEventStatus(ctx context.Context, id string, status domain.EventStatus, lastAttemptAt *time.Time, nextAttemptAt *time.Time) error
	CreateDeliveryAttempt(ctx context.Context, attempt domain.DeliveryAttempt) error
	CountDeliveryAttemptsSince(ctx context.Context, eventID string, since *time.Time) (int, error)
}

type DeliveryClient interface {
	Deliver(ctx context.Context, event domain.Event) (delivery.Result, error)
}

type Worker struct {
	store       DeliveryStore
	client      DeliveryClient
	retryPolicy delivery.RetryPolicy
	now         func() time.Time
	sleep       func(ctx context.Context, delay time.Duration) error
}

func NewWorker(store DeliveryStore, client DeliveryClient, policies ...delivery.RetryPolicy) *Worker {
	policy := delivery.DefaultRetryPolicy()
	if len(policies) > 0 {
		policy = policies[0]
	}

	return &Worker{
		store:       store,
		client:      client,
		retryPolicy: policy,
		now:         func() time.Time { return time.Now().UTC() },
		sleep:       sleepContext,
	}
}

func (w *Worker) ProcessEventCreated(ctx context.Context, message queue.EventCreatedMessage) error {
	eventID := strings.TrimSpace(message.EventID)
	if eventID == "" {
		return nil
	}

	event, err := w.store.GetEventByID(ctx, eventID)
	if errors.Is(err, repository.ErrEventNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load event: %w", err)
	}

	if isTerminalEventStatus(event.Status) {
		return nil
	}

	if err := w.store.UpdateEventStatus(ctx, event.ID, domain.EventStatusProcessing, event.LastAttemptAt, event.NextAttemptAt); err != nil {
		return fmt.Errorf("mark event processing: %w", err)
	}

	attemptNumber, err := w.nextAttemptNumber(ctx, event)
	if err != nil {
		return err
	}
	if attemptNumber > w.retryPolicy.MaxAttempts() {
		if err := w.store.UpdateEventStatus(ctx, event.ID, domain.EventStatusDeadLetter, event.LastAttemptAt, nil); err != nil {
			return fmt.Errorf("mark event dead letter after max attempts: %w", err)
		}
		observability.DefaultMetrics.IncDeliveryFailures()
		observability.DefaultMetrics.IncDeadLetters()
		return fmt.Errorf("%w: event_id=%s attempts_exceeded=%d", queue.ErrDeadLetter, event.ID, w.retryPolicy.MaxAttempts())
	}

	result, deliveryErr := w.client.Deliver(ctx, event)
	attemptedAt := w.now()
	attempt := domain.DeliveryAttempt{
		EventID:       event.ID,
		AttemptNumber: attemptNumber,
		CreatedAt:     attemptedAt,
	}

	decision := w.retryPolicy.Classify(result, deliveryErr, attemptNumber)
	attempt.Status = decision.AttemptStatus
	if deliveryErr == nil {
		attempt.StatusCode = &result.StatusCode
	}
	if decision.ErrorMessage != "" {
		attempt.ErrorMessage = &decision.ErrorMessage
	}

	attemptID, err := domain.NewID()
	if err != nil {
		return fmt.Errorf("generate delivery attempt id: %w", err)
	}
	attempt.ID = attemptID

	if err := w.store.CreateDeliveryAttempt(ctx, attempt); err != nil {
		return fmt.Errorf("record delivery attempt: %w", err)
	}

	if decision.ShouldRetry {
		nextAttemptAt := attemptedAt.Add(decision.Backoff)
		if err := w.store.UpdateEventStatus(ctx, event.ID, decision.EventStatus, &attemptedAt, &nextAttemptAt); err != nil {
			return fmt.Errorf("mark event retrying: %w", err)
		}
		observability.DefaultMetrics.IncDeliveryFailures()
		observability.DefaultMetrics.IncDeliveryRetries()
		if err := w.sleep(ctx, decision.Backoff); err != nil {
			return fmt.Errorf("wait retry backoff: %w", err)
		}
		return fmt.Errorf("%w: event_id=%s attempt=%d next_attempt_at=%s", ErrRetryScheduled, event.ID, attemptNumber, nextAttemptAt.Format(time.RFC3339))
	}

	if err := w.store.UpdateEventStatus(ctx, event.ID, decision.EventStatus, &attemptedAt, nil); err != nil {
		return fmt.Errorf("update event status: %w", err)
	}
	if decision.EventStatus == domain.EventStatusDeadLetter {
		observability.DefaultMetrics.IncDeliveryFailures()
		observability.DefaultMetrics.IncDeadLetters()
		return fmt.Errorf("%w: event_id=%s attempt=%d reason=%s", queue.ErrDeadLetter, event.ID, attemptNumber, decision.ErrorMessage)
	}
	if decision.EventStatus == domain.EventStatusDelivered {
		observability.DefaultMetrics.IncDeliveriesSucceeded()
	}

	return nil
}

func (w *Worker) nextAttemptNumber(ctx context.Context, event domain.Event) (int, error) {
	count, err := w.store.CountDeliveryAttemptsSince(ctx, event.ID, event.LastRedrivenAt)
	if err != nil {
		return 0, fmt.Errorf("count delivery attempts: %w", err)
	}

	return count + 1, nil
}

func isTerminalEventStatus(status domain.EventStatus) bool {
	return status == domain.EventStatusDelivered ||
		status == domain.EventStatusFailed ||
		status == domain.EventStatusDeadLetter
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
