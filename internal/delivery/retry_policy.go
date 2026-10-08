package delivery

import (
	"fmt"
	"net/http"
	"time"

	"github.com/julimeimei/eventrelay-go/internal/domain"
)

type RetryPolicy struct {
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
}

type RetryDecision struct {
	AttemptStatus domain.DeliveryAttemptStatus
	EventStatus   domain.EventStatus
	ShouldRetry   bool
	Backoff       time.Duration
	ErrorMessage  string
}

func NewRetryPolicy(maxAttempts int, baseDelay time.Duration, maxDelay time.Duration) RetryPolicy {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if baseDelay <= 0 {
		baseDelay = 2 * time.Second
	}
	if maxDelay <= 0 {
		maxDelay = 30 * time.Second
	}
	if maxDelay < baseDelay {
		maxDelay = baseDelay
	}

	return RetryPolicy{
		maxAttempts: maxAttempts,
		baseDelay:   baseDelay,
		maxDelay:    maxDelay,
	}
}

func DefaultRetryPolicy() RetryPolicy {
	return NewRetryPolicy(3, 2*time.Second, 30*time.Second)
}

func (p RetryPolicy) MaxAttempts() int {
	return p.maxAttempts
}

func (p RetryPolicy) Classify(result Result, err error, attemptNumber int) RetryDecision {
	if err != nil {
		return p.transientFailure(attemptNumber, err.Error())
	}

	if result.Success {
		return RetryDecision{
			AttemptStatus: domain.DeliveryAttemptStatusSuccess,
			EventStatus:   domain.EventStatusDelivered,
		}
	}

	message := fmt.Sprintf("target returned HTTP %d", result.StatusCode)
	if isRetryableStatusCode(result.StatusCode) {
		return p.transientFailure(attemptNumber, message)
	}

	return RetryDecision{
		AttemptStatus: domain.DeliveryAttemptStatusPermanentFailure,
		EventStatus:   domain.EventStatusDeadLetter,
		ErrorMessage:  message,
	}
}

func (p RetryPolicy) transientFailure(attemptNumber int, message string) RetryDecision {
	if attemptNumber >= p.maxAttempts {
		return RetryDecision{
			AttemptStatus: domain.DeliveryAttemptStatusTransientFailure,
			EventStatus:   domain.EventStatusDeadLetter,
			ErrorMessage:  message,
		}
	}

	return RetryDecision{
		AttemptStatus: domain.DeliveryAttemptStatusTransientFailure,
		EventStatus:   domain.EventStatusRetrying,
		ShouldRetry:   true,
		Backoff:       p.Backoff(attemptNumber),
		ErrorMessage:  message,
	}
}

func (p RetryPolicy) Backoff(attemptNumber int) time.Duration {
	if attemptNumber <= 1 {
		return p.baseDelay
	}

	backoff := p.baseDelay
	for i := 1; i < attemptNumber; i++ {
		if backoff >= p.maxDelay/2 {
			return p.maxDelay
		}
		backoff *= 2
	}
	if backoff > p.maxDelay {
		return p.maxDelay
	}

	return backoff
}

func isRetryableStatusCode(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests || statusCode >= http.StatusInternalServerError
}
