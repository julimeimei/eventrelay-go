package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/julimeimei/eventrelay-go/internal/domain"
	"github.com/julimeimei/eventrelay-go/internal/repository"
	"github.com/julimeimei/eventrelay-go/internal/service"
)

func TestCreateWebhook(t *testing.T) {
	t.Parallel()

	router := newTestRouter()
	body := `{
		"type": "payment.approved",
		"target_url": "https://example.com/webhooks",
		"payload": {
			"order_id": "ord_123",
			"amount": 9900
		}
	}`

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/webhooks", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(idempotencyKeyHeader, "event-123")

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response EventResponse
	decodeResponse(t, recorder, &response)

	if response.ID == "" {
		t.Fatal("expected response id to be set")
	}
	if response.Status != "pending" {
		t.Fatalf("expected status pending, got %q", response.Status)
	}
	if response.Type != "payment.approved" {
		t.Fatalf("expected type payment.approved, got %q", response.Type)
	}
	assertEventResponseDoesNotExposeSensitiveFields(t, recorder.Body.String())
	if recorder.Header().Get("X-Request-ID") == "" {
		t.Fatal("expected X-Request-ID header")
	}
}

func TestCreateWebhookPreservesRequestID(t *testing.T) {
	t.Parallel()

	router := newTestRouter()
	body := `{"type":"payment.approved","target_url":"https://example.com/webhooks","payload":{}}`

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/webhooks", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(idempotencyKeyHeader, "event-123")
	request.Header.Set("X-Request-ID", "req-123")

	router.ServeHTTP(recorder, request)

	if recorder.Header().Get("X-Request-ID") != "req-123" {
		t.Fatalf("expected X-Request-ID %q, got %q", "req-123", recorder.Header().Get("X-Request-ID"))
	}
}

func TestCreateWebhookReplacesInvalidRequestID(t *testing.T) {
	t.Parallel()

	router := newTestRouter()
	body := `{"type":"payment.approved","target_url":"https://example.com/webhooks","payload":{}}`

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/webhooks", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(idempotencyKeyHeader, "event-123")
	request.Header.Set("X-Request-ID", strings.Repeat("a", 129))

	router.ServeHTTP(recorder, request)

	requestID := recorder.Header().Get("X-Request-ID")
	if requestID == "" {
		t.Fatal("expected generated X-Request-ID header")
	}
	if requestID == strings.Repeat("a", 129) {
		t.Fatal("expected invalid X-Request-ID to be replaced")
	}
}

func TestGetWebhookByID(t *testing.T) {
	t.Parallel()

	router := newTestRouter()
	event := createTestWebhook(t, router, "event-123", `{"order_id":"ord_123"}`)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/webhooks/"+event.ID, nil)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response EventResponse
	decodeResponse(t, recorder, &response)

	if response.ID != event.ID {
		t.Fatalf("expected id %q, got %q", event.ID, response.ID)
	}
	assertEventResponseDoesNotExposeSensitiveFields(t, recorder.Body.String())
}

func TestCreateWebhookRequiresIdempotencyKey(t *testing.T) {
	t.Parallel()

	router := newTestRouter()
	body := `{"type":"payment.approved","target_url":"https://example.com/webhooks","payload":{}}`

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/webhooks", strings.NewReader(body))

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestCreateWebhookRejectsDuplicateIdempotencyKeyHeaders(t *testing.T) {
	t.Parallel()

	router := newTestRouter()
	body := `{"type":"payment.approved","target_url":"https://example.com/webhooks","payload":{}}`

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/webhooks", strings.NewReader(body))
	request.Header.Add(idempotencyKeyHeader, "event-123")
	request.Header.Add(idempotencyKeyHeader, "event-456")

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestCreateWebhookRejectsInvalidTargetURL(t *testing.T) {
	t.Parallel()

	router := newTestRouter()
	body := `{"type":"payment.approved","target_url":"ftp://example.com/webhooks","payload":{}}`

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/webhooks", strings.NewReader(body))
	request.Header.Set(idempotencyKeyHeader, "event-123")

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestCreateWebhookReturnsExistingEventForDuplicateIdempotencyKey(t *testing.T) {
	t.Parallel()

	router := newTestRouter()
	first := createTestWebhook(t, router, "event-123", `{"order_id":"ord_123"}`)
	second := createTestWebhook(t, router, "event-123", `{"order_id":"ord_123"}`)

	if first.ID != second.ID {
		t.Fatalf("expected duplicate idempotency key to return id %q, got %q", first.ID, second.ID)
	}
}

func TestCreateWebhookReturnsExistingEventForCanonicalPayload(t *testing.T) {
	t.Parallel()

	router := newTestRouter()
	first := createTestWebhook(t, router, "event-123", `{"amount":9900,"order_id":"ord_123"}`)
	second := createTestWebhook(t, router, "event-123", `{"order_id":"ord_123","amount":9900}`)

	if first.ID != second.ID {
		t.Fatalf("expected duplicate canonical payload to return id %q, got %q", first.ID, second.ID)
	}
}

func TestCreateWebhookRejectsIdempotencyConflict(t *testing.T) {
	t.Parallel()

	router := newTestRouter()
	_ = createTestWebhook(t, router, "event-123", `{"order_id":"ord_123"}`)

	body := `{"type":"payment.approved","target_url":"https://example.com/webhooks","payload":{"order_id":"ord_456"}}`

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/webhooks", strings.NewReader(body))
	request.Header.Set(idempotencyKeyHeader, "event-123")

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d", http.StatusConflict, recorder.Code)
	}
}

func TestCreateWebhookRejectsBodyAboveLimit(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	router := NewRouter(logger, service.NewInMemoryEventService(), 32, nil)

	body := `{"type":"payment.approved","target_url":"https://example.com/webhooks","payload":{"order_id":"ord_123"}}`

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/webhooks", strings.NewReader(body))
	request.Header.Set(idempotencyKeyHeader, "event-123")

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status %d, got %d", http.StatusRequestEntityTooLarge, recorder.Code)
	}
}

func TestGetWebhookByIDReturnsNotFound(t *testing.T) {
	t.Parallel()

	router := newTestRouter()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/webhooks/missing", nil)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, recorder.Code)
	}
}

func TestRedriveWebhook(t *testing.T) {
	t.Parallel()

	store := repository.NewInMemoryEventRepository()
	router := newTestRouterWithService(service.NewEventService(store))
	event := createTestWebhook(t, router, "event-123", `{"order_id":"ord_123"}`)

	if err := store.UpdateEventStatus(t.Context(), event.ID, domain.EventStatusDeadLetter, nil, nil); err != nil {
		t.Fatalf("mark event dead letter: %v", err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/webhooks/"+event.ID+"/redrive", nil)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response EventResponse
	decodeResponse(t, recorder, &response)

	if response.ID != event.ID {
		t.Fatalf("expected id %q, got %q", event.ID, response.ID)
	}
	if response.Status != "pending" {
		t.Fatalf("expected status pending, got %q", response.Status)
	}
	if response.RedriveCount != 1 {
		t.Fatalf("expected redrive count 1, got %d", response.RedriveCount)
	}
}

func TestRedriveWebhookRejectsNonDeadLetterEvent(t *testing.T) {
	t.Parallel()

	router := newTestRouter()
	event := createTestWebhook(t, router, "event-123", `{"order_id":"ord_123"}`)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/webhooks/"+event.ID+"/redrive", nil)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusConflict, recorder.Code, recorder.Body.String())
	}
}

func TestRedriveWebhookReturnsNotFound(t *testing.T) {
	t.Parallel()

	router := newTestRouter()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/webhooks/missing/redrive", nil)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusNotFound, recorder.Code, recorder.Body.String())
	}
}

func TestMetricsEndpoint(t *testing.T) {
	t.Parallel()

	router := newTestRouter()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response map[string]int64
	decodeResponse(t, recorder, &response)

	if _, ok := response["events_received_total"]; !ok {
		t.Fatal("expected events_received_total metric")
	}
	if _, ok := response["delivery_dead_letters_total"]; !ok {
		t.Fatal("expected delivery_dead_letters_total metric")
	}
}

func newTestRouter() http.Handler {
	return newTestRouterWithService(service.NewInMemoryEventService())
}

func newTestRouterWithService(eventService service.EventService) http.Handler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewRouter(logger, eventService, 1_048_576, nil)
}

func createTestWebhook(t *testing.T, router http.Handler, idempotencyKey string, payload string) EventResponse {
	t.Helper()

	body := bytes.NewBufferString(`{"type":"payment.approved","target_url":"https://example.com/webhooks","payload":` + payload + `}`)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/webhooks", body)
	request.Header.Set(idempotencyKeyHeader, idempotencyKey)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted && recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d or %d, got %d with body %s", http.StatusAccepted, http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response EventResponse
	decodeResponse(t, recorder, &response)
	return response
}

func decodeResponse(t *testing.T, recorder *httptest.ResponseRecorder, target any) {
	t.Helper()

	if err := json.NewDecoder(recorder.Body).Decode(target); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

func assertEventResponseDoesNotExposeSensitiveFields(t *testing.T, body string) {
	t.Helper()

	if strings.Contains(body, "payload") {
		t.Fatalf("expected event response not to expose payload, got %s", body)
	}
	if strings.Contains(body, "target_url") {
		t.Fatalf("expected event response not to expose target URL, got %s", body)
	}
	if strings.Contains(body, "idempotency_key") {
		t.Fatalf("expected event response not to expose idempotency key, got %s", body)
	}
}
