package observability

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMetricsSnapshot(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	metrics.IncEventsReceived()
	metrics.IncEventsReplayed()
	metrics.IncDeliveriesSucceeded()
	metrics.IncDeliveryFailures()
	metrics.IncDeliveryRetries()
	metrics.IncDeadLetters()

	got := metrics.Snapshot()
	want := MetricsSnapshot{
		EventsReceivedTotal:      1,
		EventsReplayedTotal:      1,
		DeliveriesSucceededTotal: 1,
		DeliveryFailuresTotal:    1,
		DeliveryRetriesTotal:     1,
		DeliveryDeadLettersTotal: 1,
	}

	if got != want {
		t.Fatalf("expected %+v, got %+v", want, got)
	}
}

func TestMetricsHandler(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	metrics.IncEventsReceived()
	metrics.IncDeliveryRetries()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	metrics.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected application/json content type, got %q", got)
	}

	var snapshot MetricsSnapshot
	if err := json.NewDecoder(rec.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if snapshot.EventsReceivedTotal != 1 {
		t.Fatalf("expected events received total 1, got %d", snapshot.EventsReceivedTotal)
	}
	if snapshot.DeliveryRetriesTotal != 1 {
		t.Fatalf("expected delivery retries total 1, got %d", snapshot.DeliveryRetriesTotal)
	}
}
