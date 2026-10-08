package observability

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

var DefaultMetrics = NewMetrics()

type Metrics struct {
	eventsReceived      atomic.Int64
	eventsReplayed      atomic.Int64
	deliveriesSucceeded atomic.Int64
	deliveryFailures    atomic.Int64
	deliveryRetries     atomic.Int64
	deadLetters         atomic.Int64
}

type MetricsSnapshot struct {
	EventsReceivedTotal      int64 `json:"events_received_total"`
	EventsReplayedTotal      int64 `json:"events_replayed_total"`
	DeliveriesSucceededTotal int64 `json:"deliveries_succeeded_total"`
	DeliveryFailuresTotal    int64 `json:"delivery_failures_total"`
	DeliveryRetriesTotal     int64 `json:"delivery_retries_total"`
	DeliveryDeadLettersTotal int64 `json:"delivery_dead_letters_total"`
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) IncEventsReceived() {
	m.eventsReceived.Add(1)
}

func (m *Metrics) IncEventsReplayed() {
	m.eventsReplayed.Add(1)
}

func (m *Metrics) IncDeliveriesSucceeded() {
	m.deliveriesSucceeded.Add(1)
}

func (m *Metrics) IncDeliveryFailures() {
	m.deliveryFailures.Add(1)
}

func (m *Metrics) IncDeliveryRetries() {
	m.deliveryRetries.Add(1)
}

func (m *Metrics) IncDeadLetters() {
	m.deadLetters.Add(1)
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		EventsReceivedTotal:      m.eventsReceived.Load(),
		EventsReplayedTotal:      m.eventsReplayed.Load(),
		DeliveriesSucceededTotal: m.deliveriesSucceeded.Load(),
		DeliveryFailuresTotal:    m.deliveryFailures.Load(),
		DeliveryRetriesTotal:     m.deliveryRetries.Load(),
		DeliveryDeadLettersTotal: m.deadLetters.Load(),
	}
}

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(m.Snapshot())
	})
}
