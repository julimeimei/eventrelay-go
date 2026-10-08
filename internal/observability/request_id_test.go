package observability

import (
	"context"
	"strings"
	"testing"
)

func TestWithRequestIDStoresTrimmedNonEmptyValue(t *testing.T) {
	t.Parallel()

	ctx := WithRequestID(context.Background(), " request-123 ")

	if got := RequestID(ctx); got != "request-123" {
		t.Fatalf("expected request-123, got %q", got)
	}
}

func TestWithRequestIDIgnoresBlankValue(t *testing.T) {
	t.Parallel()

	ctx := WithRequestID(context.Background(), " ")

	if got := RequestID(ctx); got != "" {
		t.Fatalf("expected empty request id, got %q", got)
	}
}

func TestNewRequestIDReturnsNonEmptyUniqueValues(t *testing.T) {
	t.Parallel()

	first := NewRequestID()
	second := NewRequestID()

	if first == "" || second == "" {
		t.Fatal("expected non-empty request ids")
	}
	if first == second {
		t.Fatalf("expected unique request ids, got %q twice", first)
	}
}

func TestNormalizeRequestID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  string
		want   string
		wantOK bool
	}{
		{name: "valid", input: " req-123 ", want: "req-123", wantOK: true},
		{name: "blank", input: " ", wantOK: false},
		{name: "too long", input: strings.Repeat("a", maxRequestIDLength+1), wantOK: false},
		{name: "control character", input: "req\n123", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := NormalizeRequestID(tt.input)
			if ok != tt.wantOK {
				t.Fatalf("expected ok=%v, got %v", tt.wantOK, ok)
			}
			if got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}
