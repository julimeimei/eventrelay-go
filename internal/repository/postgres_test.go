package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/julimeimei/eventrelay-go/internal/domain"
)

func TestPostgresEventRepositoryIntegration(t *testing.T) {
	databaseURL := os.Getenv("EVENTRELAY_INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set EVENTRELAY_INTEGRATION_DATABASE_URL to run PostgreSQL integration tests")
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}
	if err := RunMigrations(ctx, db, "../../migrations"); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	repo := NewPostgresEventRepository(db)
	event := domain.Event{
		ID:             "7d87aa1a-c8d7-45f7-9f67-e9d9204f7111",
		Type:           "payment.approved",
		TargetURL:      "https://example.com/webhooks",
		Payload:        json.RawMessage(`{"order_id":"ord_123"}`),
		IdempotencyKey: "integration-event-1",
		RequestHash:    "request-hash-1",
		Status:         domain.EventStatusPending,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	_, _ = db.ExecContext(ctx, `DELETE FROM events WHERE idempotency_key = $1`, event.IdempotencyKey)

	if err := repo.CreateEvent(ctx, event); err != nil {
		t.Fatalf("create event: %v", err)
	}

	found, err := repo.GetEventByID(ctx, event.ID)
	if err != nil {
		t.Fatalf("get event by id: %v", err)
	}
	if found.ID != event.ID {
		t.Fatalf("expected id %q, got %q", event.ID, found.ID)
	}

	err = repo.CreateEvent(ctx, event)
	if !errors.Is(err, ErrDuplicateIdempotencyKey) {
		t.Fatalf("expected ErrDuplicateIdempotencyKey, got %v", err)
	}
}
