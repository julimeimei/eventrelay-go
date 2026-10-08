package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

const RequestIDHeader = "X-Request-ID"

const maxRequestIDLength = 128

type requestIDContextKey struct{}

func WithRequestID(ctx context.Context, requestID string) context.Context {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDContextKey{}, requestID)
}

func RequestID(ctx context.Context) string {
	value, ok := ctx.Value(requestIDContextKey{}).(string)
	if !ok {
		return ""
	}
	return value
}

func NewRequestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err == nil {
		return hex.EncodeToString(raw[:])
	}

	return fmt.Sprintf("%d", time.Now().UTC().UnixNano())
}

func NormalizeRequestID(requestID string) (string, bool) {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" || len(requestID) > maxRequestIDLength {
		return "", false
	}
	for _, char := range requestID {
		if char < 0x20 || char == 0x7f {
			return "", false
		}
	}
	return requestID, true
}
