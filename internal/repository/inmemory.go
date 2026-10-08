package repository

import (
	"context"
	"sync"
	"time"

	"github.com/julimeimei/eventrelay-go/internal/domain"
)

type InMemoryEventRepository struct {
	mu              sync.RWMutex
	eventsByID      map[string]domain.Event
	idByIdempotency map[string]string
	attemptsByEvent map[string][]domain.DeliveryAttempt
}

func NewInMemoryEventRepository() *InMemoryEventRepository {
	return &InMemoryEventRepository{
		eventsByID:      make(map[string]domain.Event),
		idByIdempotency: make(map[string]string),
		attemptsByEvent: make(map[string][]domain.DeliveryAttempt),
	}
}

func (r *InMemoryEventRepository) CreateEvent(ctx context.Context, event domain.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.idByIdempotency[event.IdempotencyKey]; ok {
		return ErrDuplicateIdempotencyKey
	}

	r.eventsByID[event.ID] = event
	r.idByIdempotency[event.IdempotencyKey] = event.ID
	return nil
}

func (r *InMemoryEventRepository) GetEventByID(ctx context.Context, id string) (domain.Event, error) {
	if err := ctx.Err(); err != nil {
		return domain.Event{}, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	event, ok := r.eventsByID[id]
	if !ok {
		return domain.Event{}, ErrEventNotFound
	}

	return event, nil
}

func (r *InMemoryEventRepository) GetEventByIdempotencyKey(ctx context.Context, key string) (domain.Event, error) {
	if err := ctx.Err(); err != nil {
		return domain.Event{}, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	id, ok := r.idByIdempotency[key]
	if !ok {
		return domain.Event{}, ErrEventNotFound
	}

	return r.eventsByID[id], nil
}

func (r *InMemoryEventRepository) UpdateEventStatus(ctx context.Context, id string, status domain.EventStatus, lastAttemptAt *time.Time, nextAttemptAt *time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	event, ok := r.eventsByID[id]
	if !ok {
		return ErrEventNotFound
	}

	event.Status = status
	event.UpdatedAt = time.Now().UTC()
	event.LastAttemptAt = lastAttemptAt
	event.NextAttemptAt = nextAttemptAt
	r.eventsByID[id] = event

	return nil
}

func (r *InMemoryEventRepository) RedriveEvent(ctx context.Context, id string, redrivenAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	event, ok := r.eventsByID[id]
	if !ok {
		return ErrEventNotFound
	}

	event.Status = domain.EventStatusPending
	event.UpdatedAt = redrivenAt
	event.LastAttemptAt = nil
	event.NextAttemptAt = nil
	event.RedriveCount++
	event.LastRedrivenAt = &redrivenAt
	r.eventsByID[id] = event

	return nil
}

func (r *InMemoryEventRepository) CreateDeliveryAttempt(ctx context.Context, attempt domain.DeliveryAttempt) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.eventsByID[attempt.EventID]; !ok {
		return ErrEventNotFound
	}

	r.attemptsByEvent[attempt.EventID] = append(r.attemptsByEvent[attempt.EventID], attempt)
	return nil
}

func (r *InMemoryEventRepository) CountDeliveryAttempts(ctx context.Context, eventID string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, ok := r.eventsByID[eventID]; !ok {
		return 0, ErrEventNotFound
	}

	return len(r.attemptsByEvent[eventID]), nil
}

func (r *InMemoryEventRepository) CountDeliveryAttemptsSince(ctx context.Context, eventID string, since *time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, ok := r.eventsByID[eventID]; !ok {
		return 0, ErrEventNotFound
	}

	count := 0
	for _, attempt := range r.attemptsByEvent[eventID] {
		if since == nil || attempt.CreatedAt.After(*since) {
			count++
		}
	}

	return count, nil
}
