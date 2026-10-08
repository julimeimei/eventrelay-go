package delivery

import (
	"errors"
	"testing"
	"time"

	"github.com/julimeimei/eventrelay-go/internal/domain"
)

func TestRetryPolicyClassify(t *testing.T) {
	t.Parallel()

	policy := NewRetryPolicy(3, time.Second, 10*time.Second)

	tests := []struct {
		name          string
		result        Result
		err           error
		attemptNumber int
		wantAttempt   domain.DeliveryAttemptStatus
		wantEvent     domain.EventStatus
		wantRetry     bool
	}{
		{
			name:          "success",
			result:        Result{StatusCode: 204, Success: true},
			attemptNumber: 1,
			wantAttempt:   domain.DeliveryAttemptStatusSuccess,
			wantEvent:     domain.EventStatusDelivered,
		},
		{
			name:          "network error retries before max attempts",
			err:           errors.New("connection refused"),
			attemptNumber: 1,
			wantAttempt:   domain.DeliveryAttemptStatusTransientFailure,
			wantEvent:     domain.EventStatusRetrying,
			wantRetry:     true,
		},
		{
			name:          "http 500 retries before max attempts",
			result:        Result{StatusCode: 500, Success: false},
			attemptNumber: 2,
			wantAttempt:   domain.DeliveryAttemptStatusTransientFailure,
			wantEvent:     domain.EventStatusRetrying,
			wantRetry:     true,
		},
		{
			name:          "http 429 retries before max attempts",
			result:        Result{StatusCode: 429, Success: false},
			attemptNumber: 1,
			wantAttempt:   domain.DeliveryAttemptStatusTransientFailure,
			wantEvent:     domain.EventStatusRetrying,
			wantRetry:     true,
		},
		{
			name:          "http 400 is permanent",
			result:        Result{StatusCode: 400, Success: false},
			attemptNumber: 1,
			wantAttempt:   domain.DeliveryAttemptStatusPermanentFailure,
			wantEvent:     domain.EventStatusDeadLetter,
		},
		{
			name:          "transient failure stops at max attempts",
			result:        Result{StatusCode: 503, Success: false},
			attemptNumber: 3,
			wantAttempt:   domain.DeliveryAttemptStatusTransientFailure,
			wantEvent:     domain.EventStatusDeadLetter,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := policy.Classify(tt.result, tt.err, tt.attemptNumber)
			if got.AttemptStatus != tt.wantAttempt {
				t.Fatalf("expected attempt status %q, got %q", tt.wantAttempt, got.AttemptStatus)
			}
			if got.EventStatus != tt.wantEvent {
				t.Fatalf("expected event status %q, got %q", tt.wantEvent, got.EventStatus)
			}
			if got.ShouldRetry != tt.wantRetry {
				t.Fatalf("expected should retry %v, got %v", tt.wantRetry, got.ShouldRetry)
			}
		})
	}
}

func TestRetryPolicyBackoff(t *testing.T) {
	t.Parallel()

	policy := NewRetryPolicy(5, 2*time.Second, 5*time.Second)

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 2 * time.Second},
		{attempt: 2, want: 4 * time.Second},
		{attempt: 3, want: 5 * time.Second},
		{attempt: 4, want: 5 * time.Second},
	}

	for _, tt := range tests {
		got := policy.Backoff(tt.attempt)
		if got != tt.want {
			t.Fatalf("attempt %d: expected %s, got %s", tt.attempt, tt.want, got)
		}
	}
}
