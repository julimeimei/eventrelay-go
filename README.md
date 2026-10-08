# EventRelay

EventRelay is a production-oriented webhook delivery service built in Go. It receives events over HTTP, persists them in PostgreSQL, publishes delivery jobs to RabbitMQ, and processes those jobs asynchronously with retry, backoff, idempotency, observability, and Dead Letter Queue handling.

This is a small/medium portfolio project focused on backend engineering practices that show up in real Go roles: REST APIs, database modeling, asynchronous workers, message queues, reliability patterns, Docker Compose, tests, and security-aware defaults.

## What It Demonstrates

- HTTP API design with `net/http`.
- PostgreSQL persistence with migrations and constraints.
- RabbitMQ publishing, consuming, retries, and DLQ routing.
- Idempotency keys to prevent duplicate event creation.
- Worker-based asynchronous processing.
- Delivery attempt history and event status transitions.
- Structured JSON logs with request/correlation IDs.
- Dependency-light JSON metrics endpoints.
- Docker Compose demo environment.
- Unit, handler, repository, worker, retry, and integration-style tests.

## Architecture

```mermaid
flowchart LR
    Client[Client] -->|POST /webhooks| API[EventRelay API]
    API -->|persist event| Postgres[(PostgreSQL)]
    API -->|publish event_id| RabbitMQ[(RabbitMQ)]
    RabbitMQ -->|consume job| Worker[EventRelay Worker]
    Worker -->|load/update event| Postgres
    Worker -->|POST payload| Target[Target Webhook]
    Worker -->|record attempt| Postgres
    Worker -->|exhausted/permanent failure| DLQ[(Dead Letter Queue)]
```

The database is the source of truth. RabbitMQ carries only the minimum message needed for the worker to load the event and process delivery.

## Event Lifecycle

```text
POST /webhooks
  -> validate request and Idempotency-Key
  -> persist event as pending
  -> publish RabbitMQ message
  -> worker marks event as processing
  -> worker delivers to target_url
  -> success: delivered
  -> transient failure: retrying, then requeued after backoff
  -> permanent/exhausted failure: dead_letter
```

Event statuses:

```text
pending
processing
delivered
retrying
failed
dead_letter
```

Delivery attempt statuses:

```text
success
transient_failure
permanent_failure
```

## Quick Start

Requirements:

- Go 1.26 or newer.
- Docker and Docker Compose.

Start the complete local environment:

```bash
docker compose up --build
```

Services:

```text
API: http://localhost:8080
Demo consumer: http://localhost:8081
Worker metrics: http://localhost:8082/metrics
RabbitMQ Management UI: http://localhost:15672
PostgreSQL host port: localhost:5433
```

RabbitMQ Management UI:

```text
URL: http://localhost:15672
User: guest
Password: guest
```

Check API readiness:

```bash
curl http://localhost:8080/ready
```

On PowerShell, use:

```powershell
Invoke-RestMethod http://localhost:8080/ready
```

## Local Demo

Run a successful delivery demo:

```powershell
.\scripts\demo-success.ps1
```

The response should look like:

```json
{
  "id": "generated-event-id",
  "type": "payment.approved",
  "status": "pending",
  "redrive_count": 0,
  "created_at": "2026-08-12T00:00:00Z",
  "updated_at": "2026-08-12T00:00:00Z"
}
```

Then query the event by ID:

```bash
curl http://localhost:8080/webhooks/{id}
```

After the worker processes the event, the status should become:

```json
{
  "id": "generated-event-id",
  "type": "payment.approved",
  "status": "delivered",
  "redrive_count": 0,
  "created_at": "2026-08-12T00:00:00Z",
  "updated_at": "2026-08-12T00:00:02Z"
}
```

The response intentionally omits the stored payload, target URL, and idempotency key to reduce accidental data exposure.

## Failure Demo

Run a permanent failure demo:

```powershell
.\scripts\demo-dlq.ps1
```

This sends an event to the demo consumer endpoint that returns HTTP `400`. The worker treats most HTTP `4xx` responses as permanent failures, records a delivery attempt, marks the event as `dead_letter`, and rejects the RabbitMQ message without requeueing it.

Inspect DLQ behavior:

1. Open `http://localhost:15672`.
2. Login with `guest` / `guest`.
3. Go to `Queues and Streams`.
4. Open `eventrelay.deliveries.dlq`.
5. Use `Get messages` to inspect the dead-lettered message.

Redrive a dead-lettered event by ID:

```powershell
.\scripts\demo-redrive.ps1 -EventID {id}
```

This resets the event to `pending`, increments `redrive_count`, records `last_redriven_at` internally, and publishes a new RabbitMQ delivery message. Delivery attempts from before the redrive are preserved for audit history, but retry counting starts from the latest redrive cycle.

To simulate transient retry behavior, submit an event targeting:

```text
http://demo-consumer:8081/fail-transient
```

The demo consumer returns HTTP `500`, which EventRelay treats as retryable. With the default settings, the event is retried and eventually moved to the DLQ after the maximum attempts.

Default retry settings:

```text
DELIVERY_MAX_ATTEMPTS=3
DELIVERY_RETRY_BASE_DELAY=2s
DELIVERY_RETRY_MAX_DELAY=30s
```

## API Reference

### `POST /webhooks`

Creates an event and schedules asynchronous delivery.

Headers:

```text
Content-Type: application/json
Idempotency-Key: event-123
```

Request body:

```json
{
  "type": "payment.approved",
  "target_url": "http://demo-consumer:8081/webhook",
  "payload": {
    "order_id": "ord_123",
    "amount": 9900
  }
}
```

Response:

```json
{
  "id": "generated-event-id",
  "type": "payment.approved",
  "status": "pending",
  "redrive_count": 0,
  "created_at": "2026-08-12T00:00:00Z",
  "updated_at": "2026-08-12T00:00:00Z"
}
```

Common responses:

```text
202 Accepted - new event created and queued
200 OK       - duplicate idempotent request returned existing event
400 Bad Request
409 Conflict - same idempotency key used with different content
413 Payload Too Large
500 Internal Server Error
```

### `POST /webhooks/{id}/redrive`

Requeues a dead-lettered event for another delivery cycle.

```bash
curl -X POST http://localhost:8080/webhooks/{id}/redrive
```

Response:

```json
{
  "id": "generated-event-id",
  "type": "payment.rejected",
  "status": "pending",
  "redrive_count": 1,
  "created_at": "2026-08-12T00:00:00Z",
  "updated_at": "2026-08-12T00:01:00Z"
}
```

Only events with `dead_letter` status are eligible. Other statuses return `409 Conflict`.

### `GET /webhooks/{id}`

Returns delivery status metadata for an event.

```bash
curl http://localhost:8080/webhooks/{id}
```

### `GET /health`

Returns API liveness.

### `GET /ready`

Checks API readiness. The API verifies PostgreSQL connectivity when configured with a readiness check.

### `GET /metrics`

Returns JSON counters:

```json
{
  "events_received_total": 0,
  "events_replayed_total": 0,
  "deliveries_succeeded_total": 0,
  "delivery_failures_total": 0,
  "delivery_retries_total": 0,
  "delivery_dead_letters_total": 0
}
```

API metrics:

```bash
curl http://localhost:8080/metrics
```

Worker metrics:

```bash
curl http://localhost:8082/metrics
```

## Idempotency

`POST /webhooks` requires exactly one `Idempotency-Key` header.

Validation rules:

- The key is trimmed.
- It must not be empty.
- It must be at most 255 characters.
- It must not contain control characters.

EventRelay stores a unique idempotency key in PostgreSQL and computes a request hash from the normalized event type, target URL, and canonical JSON payload.

Behavior:

- First request with a new key creates the event and publishes one RabbitMQ message.
- Repeating the same key with the same canonical request returns the original event with `200 OK` and does not publish another message.
- Reusing the same key with different content returns `409 Conflict`.
- PostgreSQL remains the final race-condition guard through a unique `idempotency_key` constraint.

## Retry And DLQ Strategy

Delivery classification:

```text
2xx response                 -> success
network error / timeout      -> transient failure
HTTP 429                     -> transient failure
HTTP 5xx                     -> transient failure
most HTTP 4xx responses      -> permanent failure
max attempts exhausted       -> dead letter
```

Transient failures use exponential backoff capped by `DELIVERY_RETRY_MAX_DELAY`. Permanent failures and exhausted transient failures are moved to the Dead Letter Queue.

Dead-lettered events can be redriven with `POST /webhooks/{id}/redrive`. Redrive preserves previous delivery attempts for auditability and starts a new retry-counting cycle from the redrive timestamp.

RabbitMQ topology:

```text
main exchange: eventrelay.events
main queue: eventrelay.deliveries
dead letter exchange: eventrelay.dlx
dead letter queue: eventrelay.deliveries.dlq
```

## Testing And Quality Gates

Run all stable tests:

```bash
go test ./...
```

Run the local quality gate:

```powershell
.\scripts\quality.ps1
```

The quality gate checks:

```text
gofmt
go test ./...
go vet ./...
go build ./cmd/api ./cmd/worker ./cmd/demo-consumer
docker compose config
```

Run with the race detector:

```powershell
.\scripts\quality.ps1 -Race
```

Skip Docker validation if Docker is not installed:

```powershell
.\scripts\quality.ps1 -SkipDocker
```

Run optional PostgreSQL integration tests:

```bash
EVENTRELAY_INTEGRATION_DATABASE_URL="postgres://eventrelay:eventrelay@127.0.0.1:5433/eventrelay?sslmode=disable" go test ./internal/repository
```

PowerShell:

```powershell
$env:EVENTRELAY_INTEGRATION_DATABASE_URL="postgres://eventrelay:eventrelay@127.0.0.1:5433/eventrelay?sslmode=disable"
go test ./internal/repository
Remove-Item Env:\EVENTRELAY_INTEGRATION_DATABASE_URL
```

These integration tests are skipped by default unless `EVENTRELAY_INTEGRATION_DATABASE_URL` is set.

## Local Development Without Full Compose

Start only PostgreSQL and RabbitMQ:

```bash
docker compose up -d postgres rabbitmq
```

Run the API:

```bash
go run ./cmd/api
```

Run the worker in another terminal:

```bash
go run ./cmd/worker
```

For local `go run`, the default PostgreSQL URL uses host port `5433`:

```text
postgres://eventrelay:eventrelay@127.0.0.1:5433/eventrelay?sslmode=disable
```

## Configuration

Configuration is environment-based. See [.env.example](.env.example).

Important values:

```text
APP_ENV=local
API_ADDR=:8080
DATABASE_URL=postgres://eventrelay:eventrelay@127.0.0.1:5433/eventrelay?sslmode=disable
RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/
MAX_REQUEST_BODY_BYTES=1048576
DELIVERY_TIMEOUT=5s
DELIVERY_MAX_ATTEMPTS=3
DELIVERY_RETRY_BASE_DELAY=2s
DELIVERY_RETRY_MAX_DELAY=30s
WORKER_METRICS_ADDR=:8082
```

In Docker Compose, services communicate through Compose service names:

```text
postgres:5432
rabbitmq:5672
demo-consumer:8081
```

From the host machine, use:

```text
localhost:5433 for PostgreSQL
localhost:5672 for RabbitMQ AMQP
localhost:15672 for RabbitMQ Management UI
```

## Reset The Demo

To remove local database and queue state:

```bash
docker compose down -v
docker compose up --build
```

If RabbitMQ reports a `PRECONDITION_FAILED` error for `eventrelay.deliveries`, delete the old `eventrelay.deliveries` queue in the management UI and restart the API/worker. RabbitMQ does not allow an existing durable queue to be redeclared with different dead-letter arguments.
