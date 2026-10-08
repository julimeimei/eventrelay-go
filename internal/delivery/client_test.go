package delivery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/julimeimei/eventrelay-go/internal/domain"
)

func TestHTTPClientDeliverSuccess(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected method POST, got %s", r.Method)
		}

		var payload WebhookPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if payload.ID != "event-123" {
			t.Fatalf("expected event id event-123, got %q", payload.ID)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewHTTPClient(time.Second)
	result, err := client.Deliver(context.Background(), testEvent(server.URL))
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}

	if !result.Success {
		t.Fatal("expected delivery success")
	}
	if result.StatusCode != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d", http.StatusNoContent, result.StatusCode)
	}
}

func TestHTTPClientDeliverNonSuccessStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewHTTPClient(time.Second)
	result, err := client.Deliver(context.Background(), testEvent(server.URL))
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}

	if result.Success {
		t.Fatal("expected delivery failure")
	}
	if result.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, result.StatusCode)
	}
}

func TestHTTPClientDeliverTimeout(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewHTTPClient(5 * time.Millisecond)
	_, err := client.Deliver(context.Background(), testEvent(server.URL))
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestHTTPClientDoesNotExposeTargetURLInNetworkError(t *testing.T) {
	t.Parallel()

	client := NewHTTPClient(5 * time.Millisecond)
	targetURL := "http://127.0.0.1:1/webhook?token=secret-token"

	_, err := client.Deliver(context.Background(), testEvent(targetURL))
	if err == nil {
		t.Fatal("expected delivery error")
	}
	if strings.Contains(err.Error(), targetURL) || strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("expected sanitized delivery error, got %q", err.Error())
	}
}

func testEvent(targetURL string) domain.Event {
	return domain.Event{
		ID:        "event-123",
		Type:      "payment.approved",
		TargetURL: targetURL,
		Payload:   json.RawMessage(`{"order_id":"ord_123"}`),
		Status:    domain.EventStatusPending,
	}
}
