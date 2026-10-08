package observability

import "testing"

func TestNewLoggerAcceptsSupportedLevels(t *testing.T) {
	t.Parallel()

	for _, level := range []string{"debug", "info", "warn", "warning", "error", ""} {
		t.Run(level, func(t *testing.T) {
			t.Parallel()

			logger, err := NewLogger(level)
			if err != nil {
				t.Fatalf("expected level %q to be supported: %v", level, err)
			}
			if logger == nil {
				t.Fatal("expected logger")
			}
		})
	}
}

func TestNewLoggerRejectsUnsupportedLevel(t *testing.T) {
	t.Parallel()

	if _, err := NewLogger("verbose"); err == nil {
		t.Fatal("expected error")
	}
}
