package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/julimeimei/eventrelay-go/internal/domain"
	"github.com/julimeimei/eventrelay-go/internal/observability"
	"github.com/julimeimei/eventrelay-go/internal/queue"
	"github.com/julimeimei/eventrelay-go/internal/repository"
)

var (
	ErrInvalidEvent          = errors.New("invalid event")
	ErrInvalidIdempotencyKey = errors.New("invalid idempotency key")
	ErrEventNotFound         = errors.New("event not found")
	ErrEventNotRedrivable    = errors.New("event is not eligible for redrive")
	ErrIdempotencyConflict   = errors.New("idempotency key was already used with different content")
	ErrIdempotencyKeyMissing = errors.New("idempotency key is required")
)

const maxIdempotencyKeyLength = 255

type CreateEventInput struct {
	Type           string
	TargetURL      string
	Payload        json.RawMessage
	IdempotencyKey string
}

type CreateEventResult struct {
	Event   domain.Event
	Created bool
}

type EventService interface {
	CreateEvent(ctx context.Context, input CreateEventInput) (CreateEventResult, error)
	GetEvent(ctx context.Context, id string) (domain.Event, error)
	RedriveEvent(ctx context.Context, id string) (domain.Event, error)
}

type EventRepository interface {
	CreateEvent(ctx context.Context, event domain.Event) error
	GetEventByID(ctx context.Context, id string) (domain.Event, error)
	GetEventByIdempotencyKey(ctx context.Context, key string) (domain.Event, error)
	RedriveEvent(ctx context.Context, id string, redrivenAt time.Time) error
}

type EventPublisher interface {
	PublishEventCreated(ctx context.Context, message queue.EventCreatedMessage) error
}

type EventServiceImpl struct {
	events    EventRepository
	publisher EventPublisher
	now       func() time.Time
}

func NewEventService(events EventRepository, publishers ...EventPublisher) *EventServiceImpl {
	service := &EventServiceImpl{
		events: events,
		now:    func() time.Time { return time.Now().UTC() },
	}

	if len(publishers) > 0 {
		service.publisher = publishers[0]
	}

	return service
}

func NewInMemoryEventService() *EventServiceImpl {
	return NewEventService(repository.NewInMemoryEventRepository())
}

func (s *EventServiceImpl) CreateEvent(ctx context.Context, input CreateEventInput) (CreateEventResult, error) {
	if err := ctx.Err(); err != nil {
		return CreateEventResult{}, err
	}

	normalized, err := normalizeCreateEventInput(input)
	if err != nil {
		return CreateEventResult{}, err
	}

	requestHash := hashCreateEventInput(normalized)

	existing, err := s.events.GetEventByIdempotencyKey(ctx, normalized.IdempotencyKey)
	if err == nil {
		if existing.RequestHash != requestHash {
			slog.WarnContext(ctx, "idempotency conflict",
				slog.String("request_id", observability.RequestID(ctx)),
				slog.String("event_id", existing.ID),
				slog.String("event_type", existing.Type),
			)
			return CreateEventResult{}, ErrIdempotencyConflict
		}
		observability.DefaultMetrics.IncEventsReplayed()
		slog.InfoContext(ctx, "idempotent event replayed",
			slog.String("request_id", observability.RequestID(ctx)),
			slog.String("event_id", existing.ID),
			slog.String("event_type", existing.Type),
			slog.String("status", string(existing.Status)),
		)
		return CreateEventResult{Event: existing, Created: false}, nil
	}
	if !errors.Is(err, repository.ErrEventNotFound) {
		return CreateEventResult{}, fmt.Errorf("get event by idempotency key: %w", err)
	}

	id, err := domain.NewID()
	if err != nil {
		return CreateEventResult{}, fmt.Errorf("generate event id: %w", err)
	}

	now := s.now()
	event := domain.Event{
		ID:             id,
		Type:           normalized.Type,
		TargetURL:      normalized.TargetURL,
		Payload:        normalized.Payload,
		IdempotencyKey: normalized.IdempotencyKey,
		RequestHash:    requestHash,
		Status:         domain.EventStatusPending,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if err := s.events.CreateEvent(ctx, event); err != nil {
		if errors.Is(err, repository.ErrDuplicateIdempotencyKey) {
			existing, getErr := s.events.GetEventByIdempotencyKey(ctx, normalized.IdempotencyKey)
			if getErr != nil {
				return CreateEventResult{}, fmt.Errorf("get duplicate event by idempotency key: %w", getErr)
			}
			if existing.RequestHash != requestHash {
				slog.WarnContext(ctx, "idempotency conflict after duplicate insert",
					slog.String("request_id", observability.RequestID(ctx)),
					slog.String("event_id", existing.ID),
					slog.String("event_type", existing.Type),
				)
				return CreateEventResult{}, ErrIdempotencyConflict
			}
			observability.DefaultMetrics.IncEventsReplayed()
			slog.InfoContext(ctx, "idempotent event replayed after duplicate insert",
				slog.String("request_id", observability.RequestID(ctx)),
				slog.String("event_id", existing.ID),
				slog.String("event_type", existing.Type),
				slog.String("status", string(existing.Status)),
			)
			return CreateEventResult{Event: existing, Created: false}, nil
		}
		return CreateEventResult{}, fmt.Errorf("create event: %w", err)
	}

	observability.DefaultMetrics.IncEventsReceived()
	slog.InfoContext(ctx, "event persisted",
		slog.String("request_id", observability.RequestID(ctx)),
		slog.String("event_id", event.ID),
		slog.String("event_type", event.Type),
		slog.String("status", string(event.Status)),
	)

	if err := s.publishEventCreated(ctx, event); err != nil {
		return CreateEventResult{}, err
	}

	slog.InfoContext(ctx, "event queued for delivery",
		slog.String("request_id", observability.RequestID(ctx)),
		slog.String("event_id", event.ID),
		slog.String("event_type", event.Type),
	)

	return CreateEventResult{Event: event, Created: true}, nil
}

func (s *EventServiceImpl) GetEvent(ctx context.Context, id string) (domain.Event, error) {
	if err := ctx.Err(); err != nil {
		return domain.Event{}, err
	}

	id = strings.TrimSpace(id)
	if id == "" {
		return domain.Event{}, ErrEventNotFound
	}

	event, err := s.events.GetEventByID(ctx, id)
	if errors.Is(err, repository.ErrEventNotFound) {
		return domain.Event{}, ErrEventNotFound
	}
	if err != nil {
		return domain.Event{}, fmt.Errorf("get event by id: %w", err)
	}

	return event, nil
}

func (s *EventServiceImpl) RedriveEvent(ctx context.Context, id string) (domain.Event, error) {
	if err := ctx.Err(); err != nil {
		return domain.Event{}, err
	}

	event, err := s.GetEvent(ctx, id)
	if err != nil {
		return domain.Event{}, err
	}
	if event.Status != domain.EventStatusDeadLetter {
		return domain.Event{}, ErrEventNotRedrivable
	}

	redrivenAt := s.now()
	if err := s.events.RedriveEvent(ctx, event.ID, redrivenAt); err != nil {
		if errors.Is(err, repository.ErrEventNotFound) {
			return domain.Event{}, ErrEventNotFound
		}
		return domain.Event{}, fmt.Errorf("redrive event: %w", err)
	}

	redriven, err := s.GetEvent(ctx, event.ID)
	if err != nil {
		return domain.Event{}, err
	}
	if err := s.publishEventCreated(ctx, redriven); err != nil {
		return domain.Event{}, err
	}

	slog.InfoContext(ctx, "event redriven",
		slog.String("request_id", observability.RequestID(ctx)),
		slog.String("event_id", redriven.ID),
		slog.String("event_type", redriven.Type),
		slog.Int("redrive_count", redriven.RedriveCount),
	)

	return redriven, nil
}

func (s *EventServiceImpl) publishEventCreated(ctx context.Context, event domain.Event) error {
	if s.publisher == nil {
		return nil
	}

	if err := s.publisher.PublishEventCreated(ctx, queue.EventCreatedMessage{
		EventID:       event.ID,
		EventType:     event.Type,
		CorrelationID: observability.RequestID(ctx),
		QueuedAt:      time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("publish event created: %w", err)
	}

	return nil
}

func normalizeCreateEventInput(input CreateEventInput) (CreateEventInput, error) {
	input.Type = strings.TrimSpace(input.Type)
	input.TargetURL = strings.TrimSpace(input.TargetURL)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)

	if input.IdempotencyKey == "" {
		return CreateEventInput{}, ErrIdempotencyKeyMissing
	}
	if len(input.IdempotencyKey) > maxIdempotencyKeyLength {
		return CreateEventInput{}, fmt.Errorf("%w: must be at most %d characters", ErrInvalidIdempotencyKey, maxIdempotencyKeyLength)
	}
	if hasControlCharacter(input.IdempotencyKey) {
		return CreateEventInput{}, fmt.Errorf("%w: must not contain control characters", ErrInvalidIdempotencyKey)
	}
	if input.Type == "" {
		return CreateEventInput{}, fmt.Errorf("%w: type is required", ErrInvalidEvent)
	}
	if len(input.Type) > 128 {
		return CreateEventInput{}, fmt.Errorf("%w: type must be at most 128 characters", ErrInvalidEvent)
	}
	if input.TargetURL == "" {
		return CreateEventInput{}, fmt.Errorf("%w: target_url is required", ErrInvalidEvent)
	}
	if !isHTTPURL(input.TargetURL) {
		return CreateEventInput{}, fmt.Errorf("%w: target_url must be an http or https URL", ErrInvalidEvent)
	}
	if len(input.Payload) == 0 {
		return CreateEventInput{}, fmt.Errorf("%w: payload is required", ErrInvalidEvent)
	}
	if !json.Valid(input.Payload) {
		return CreateEventInput{}, fmt.Errorf("%w: payload must be valid JSON", ErrInvalidEvent)
	}

	canonicalPayload, err := canonicalJSON(input.Payload)
	if err != nil {
		return CreateEventInput{}, fmt.Errorf("%w: payload must be valid JSON", ErrInvalidEvent)
	}
	input.Payload = canonicalPayload

	return input, nil
}

func isHTTPURL(rawURL string) bool {
	parsed, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return false
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return false
	}
	return parsed.Host != ""
}

func hashCreateEventInput(input CreateEventInput) string {
	hash := sha256.New()
	hash.Write([]byte(input.Type))
	hash.Write([]byte{0})
	hash.Write([]byte(input.TargetURL))
	hash.Write([]byte{0})
	hash.Write(input.Payload)
	return hex.EncodeToString(hash.Sum(nil))
}

func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}

	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return nil, errors.New("payload contains multiple JSON values")
	} else if !errors.Is(err, io.EOF) {
		return nil, err
	}

	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	return json.RawMessage(canonical), nil
}

func hasControlCharacter(value string) bool {
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return true
		}
	}
	return false
}
