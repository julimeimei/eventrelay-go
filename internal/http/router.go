package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/julimeimei/eventrelay-go/internal/domain"
	"github.com/julimeimei/eventrelay-go/internal/observability"
	"github.com/julimeimei/eventrelay-go/internal/service"
)

const idempotencyKeyHeader = "Idempotency-Key"

type HealthResponse struct {
	Status    string `json:"status"`
	Service   string `json:"service"`
	Timestamp string `json:"timestamp"`
}

type EventResponse struct {
	ID           string `json:"id"`
	Type         string `json:"type"`
	Status       string `json:"status"`
	RedriveCount int    `json:"redrive_count"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type createWebhookRequest struct {
	Type      string          `json:"type"`
	TargetURL string          `json:"target_url"`
	Payload   json.RawMessage `json:"payload"`
}

type ReadinessCheck func(ctx context.Context) error

func NewRouter(logger *slog.Logger, eventService service.EventService, maxRequestBodyBytes int64, readinessCheck ReadinessCheck) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, HealthResponse{
			Status:    "ok",
			Service:   "eventrelay-api",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	})

	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if readinessCheck != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()

			if err := readinessCheck(ctx); err != nil {
				writeError(w, http.StatusServiceUnavailable, "service is not ready")
				return
			}
		}

		writeJSON(w, http.StatusOK, HealthResponse{
			Status:    "ready",
			Service:   "eventrelay-api",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	})
	mux.Handle("GET /metrics", observability.DefaultMetrics.Handler())

	webhookHandler := NewWebhookHandler(eventService, maxRequestBodyBytes)
	mux.HandleFunc("POST /webhooks", webhookHandler.Create)
	mux.HandleFunc("GET /webhooks/{id}", webhookHandler.GetByID)
	mux.HandleFunc("POST /webhooks/{id}/redrive", webhookHandler.Redrive)

	return requestIDMiddleware(requestLogger(logger, mux))
}

type WebhookHandler struct {
	events              service.EventService
	maxRequestBodyBytes int64
}

func NewWebhookHandler(events service.EventService, maxRequestBodyBytes int64) WebhookHandler {
	return WebhookHandler{
		events:              events,
		maxRequestBodyBytes: maxRequestBodyBytes,
	}
}

func (h WebhookHandler) Create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, h.maxRequestBodyBytes)
	defer r.Body.Close()

	var request createWebhookRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&request); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body is too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "request body must contain a single JSON object")
		return
	}

	idempotencyKey, err := idempotencyKeyFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	result, err := h.events.CreateEvent(r.Context(), service.CreateEventInput{
		Type:           request.Type,
		TargetURL:      request.TargetURL,
		Payload:        request.Payload,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		switch {
		case errors.Is(err, service.ErrIdempotencyKeyMissing), errors.Is(err, service.ErrInvalidIdempotencyKey), errors.Is(err, service.ErrInvalidEvent):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, service.ErrIdempotencyConflict):
			writeError(w, http.StatusConflict, err.Error())
		default:
			loggerError(r, err)
			writeError(w, http.StatusInternalServerError, "internal server error")
		}
		return
	}

	statusCode := http.StatusAccepted
	if !result.Created {
		statusCode = http.StatusOK
	}

	writeJSON(w, statusCode, eventToResponse(result.Event))
}

func idempotencyKeyFromRequest(r *http.Request) (string, error) {
	values := r.Header.Values(idempotencyKeyHeader)
	if len(values) > 1 {
		return "", fmt.Errorf("%w: provide exactly one %s header", service.ErrInvalidIdempotencyKey, idempotencyKeyHeader)
	}
	if len(values) == 0 {
		return "", nil
	}
	return values[0], nil
}

func loggerError(r *http.Request, err error) {
	slog.ErrorContext(r.Context(), "webhook request failed",
		slog.String("request_id", observability.RequestID(r.Context())),
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("error", err.Error()),
	)
}

func (h WebhookHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	event, err := h.events.GetEvent(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, service.ErrEventNotFound) {
			writeError(w, http.StatusNotFound, "event not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusOK, eventToResponse(event))
}

func (h WebhookHandler) Redrive(w http.ResponseWriter, r *http.Request) {
	event, err := h.events.RedriveEvent(r.Context(), r.PathValue("id"))
	if err != nil {
		switch {
		case errors.Is(err, service.ErrEventNotFound):
			writeError(w, http.StatusNotFound, "event not found")
		case errors.Is(err, service.ErrEventNotRedrivable):
			writeError(w, http.StatusConflict, "event is not eligible for redrive")
		default:
			loggerError(r, err)
			writeError(w, http.StatusInternalServerError, "internal server error")
		}
		return
	}

	writeJSON(w, http.StatusAccepted, eventToResponse(event))
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, ErrorResponse{Error: message})
}

func eventToResponse(event domain.Event) EventResponse {
	return EventResponse{
		ID:           event.ID,
		Type:         event.Type,
		Status:       string(event.Status),
		RedriveCount: event.RedriveCount,
		CreatedAt:    event.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:    event.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func requestLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		recorder := newResponseRecorder(w)

		next.ServeHTTP(recorder, r)

		logger.InfoContext(r.Context(), "http request completed",
			slog.String("request_id", observability.RequestID(r.Context())),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", recorder.statusCode),
			slog.Int("bytes", recorder.bytesWritten),
			slog.Duration("duration", time.Since(startedAt)),
		)
	})
}

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID, ok := observability.NormalizeRequestID(r.Header.Get(observability.RequestIDHeader))
		if !ok {
			requestID = observability.NewRequestID()
		}

		w.Header().Set(observability.RequestIDHeader, requestID)
		ctx := observability.WithRequestID(r.Context(), requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type responseRecorder struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
}

func newResponseRecorder(w http.ResponseWriter) *responseRecorder {
	return &responseRecorder{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
	}
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *responseRecorder) Write(body []byte) (int, error) {
	written, err := r.ResponseWriter.Write(body)
	r.bytesWritten += written
	return written, err
}
