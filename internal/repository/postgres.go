package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/julimeimei/eventrelay-go/internal/domain"
)

type PostgresEventRepository struct {
	db *sql.DB
}

func NewPostgresEventRepository(db *sql.DB) *PostgresEventRepository {
	return &PostgresEventRepository{db: db}
}

func (r *PostgresEventRepository) CreateEvent(ctx context.Context, event domain.Event) error {
	const query = `
		INSERT INTO events (
			id,
			event_type,
			target_url,
			payload,
			idempotency_key,
			request_hash,
			status,
			created_at,
			updated_at,
			last_attempt_at,
			next_attempt_at,
			redrive_count,
			last_redriven_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (idempotency_key) DO NOTHING
	`

	result, err := r.db.ExecContext(ctx, query,
		event.ID,
		event.Type,
		event.TargetURL,
		[]byte(event.Payload),
		event.IdempotencyKey,
		event.RequestHash,
		string(event.Status),
		event.CreatedAt,
		event.UpdatedAt,
		event.LastAttemptAt,
		event.NextAttemptAt,
		event.RedriveCount,
		event.LastRedrivenAt,
	)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read insert result: %w", err)
	}
	if rowsAffected == 0 {
		return ErrDuplicateIdempotencyKey
	}

	return nil
}

func (r *PostgresEventRepository) GetEventByID(ctx context.Context, id string) (domain.Event, error) {
	const query = `
		SELECT
			id,
			event_type,
			target_url,
			payload,
			idempotency_key,
			request_hash,
			status,
			created_at,
			updated_at,
			last_attempt_at,
			next_attempt_at,
			redrive_count,
			last_redriven_at
		FROM events
		WHERE id = $1
	`

	return r.scanEvent(r.db.QueryRowContext(ctx, query, id))
}

func (r *PostgresEventRepository) GetEventByIdempotencyKey(ctx context.Context, key string) (domain.Event, error) {
	const query = `
		SELECT
			id,
			event_type,
			target_url,
			payload,
			idempotency_key,
			request_hash,
			status,
			created_at,
			updated_at,
			last_attempt_at,
			next_attempt_at,
			redrive_count,
			last_redriven_at
		FROM events
		WHERE idempotency_key = $1
	`

	return r.scanEvent(r.db.QueryRowContext(ctx, query, key))
}

func (r *PostgresEventRepository) UpdateEventStatus(ctx context.Context, id string, status domain.EventStatus, lastAttemptAt *time.Time, nextAttemptAt *time.Time) error {
	const query = `
		UPDATE events
		SET
			status = $2,
			updated_at = $3,
			last_attempt_at = $4,
			next_attempt_at = $5
		WHERE id = $1
	`

	result, err := r.db.ExecContext(ctx, query, id, string(status), time.Now().UTC(), lastAttemptAt, nextAttemptAt)
	if err != nil {
		return fmt.Errorf("update event status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read update event status result: %w", err)
	}
	if rowsAffected == 0 {
		return ErrEventNotFound
	}

	return nil
}

func (r *PostgresEventRepository) RedriveEvent(ctx context.Context, id string, redrivenAt time.Time) error {
	const query = `
		UPDATE events
		SET
			status = $2,
			updated_at = $3,
			last_attempt_at = NULL,
			next_attempt_at = NULL,
			redrive_count = redrive_count + 1,
			last_redriven_at = $3
		WHERE id = $1
	`

	result, err := r.db.ExecContext(ctx, query, id, string(domain.EventStatusPending), redrivenAt)
	if err != nil {
		return fmt.Errorf("redrive event: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read redrive event result: %w", err)
	}
	if rowsAffected == 0 {
		return ErrEventNotFound
	}

	return nil
}

func (r *PostgresEventRepository) CreateDeliveryAttempt(ctx context.Context, attempt domain.DeliveryAttempt) error {
	const query = `
		INSERT INTO delivery_attempts (
			id,
			event_id,
			attempt_number,
			status,
			status_code,
			error_message,
			created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	if _, err := r.db.ExecContext(ctx, query,
		attempt.ID,
		attempt.EventID,
		attempt.AttemptNumber,
		string(attempt.Status),
		attempt.StatusCode,
		attempt.ErrorMessage,
		attempt.CreatedAt,
	); err != nil {
		return fmt.Errorf("insert delivery attempt: %w", err)
	}

	return nil
}

func (r *PostgresEventRepository) CountDeliveryAttempts(ctx context.Context, eventID string) (int, error) {
	const query = `
		SELECT COUNT(*)
		FROM delivery_attempts
		WHERE event_id = $1
	`

	var count int
	if err := r.db.QueryRowContext(ctx, query, eventID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count delivery attempts: %w", err)
	}

	return count, nil
}

func (r *PostgresEventRepository) CountDeliveryAttemptsSince(ctx context.Context, eventID string, since *time.Time) (int, error) {
	const query = `
		SELECT COUNT(*)
		FROM delivery_attempts
		WHERE event_id = $1
			AND ($2::timestamptz IS NULL OR created_at > $2)
	`

	var sinceValue any
	if since != nil {
		sinceValue = *since
	}

	var count int
	if err := r.db.QueryRowContext(ctx, query, eventID, sinceValue).Scan(&count); err != nil {
		return 0, fmt.Errorf("count delivery attempts since redrive: %w", err)
	}

	return count, nil
}

func (r *PostgresEventRepository) scanEvent(row interface {
	Scan(dest ...any) error
}) (domain.Event, error) {
	var event domain.Event
	var payload []byte
	var status string

	err := row.Scan(
		&event.ID,
		&event.Type,
		&event.TargetURL,
		&payload,
		&event.IdempotencyKey,
		&event.RequestHash,
		&status,
		&event.CreatedAt,
		&event.UpdatedAt,
		&event.LastAttemptAt,
		&event.NextAttemptAt,
		&event.RedriveCount,
		&event.LastRedrivenAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Event{}, ErrEventNotFound
	}
	if err != nil {
		return domain.Event{}, fmt.Errorf("scan event: %w", err)
	}

	event.Payload = payload
	event.Status = domain.EventStatus(status)

	return event, nil
}
