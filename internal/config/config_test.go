package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestLoadUsesLocalDefaults(t *testing.T) {
	isolateEnv(t,
		"APP_ENV",
		"LOG_LEVEL",
		"API_ADDR",
		"READ_TIMEOUT",
		"WRITE_TIMEOUT",
		"IDLE_TIMEOUT",
		"MAX_REQUEST_BODY_BYTES",
		"DATABASE_URL",
		"DATABASE_AUTO_MIGRATE",
		"DATABASE_MIGRATIONS_DIR",
		"RABBITMQ_URL",
		"RABBITMQ_EXCHANGE",
		"RABBITMQ_DELIVERY_QUEUE",
		"RABBITMQ_DLX",
		"RABBITMQ_DLQ",
		"DELIVERY_TIMEOUT",
		"DELIVERY_MAX_ATTEMPTS",
		"DELIVERY_RETRY_BASE_DELAY",
		"DELIVERY_RETRY_MAX_DELAY",
		"WORKER_METRICS_ADDR",
	)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.AppEnv != "local" {
		t.Fatalf("expected local app env, got %q", cfg.AppEnv)
	}
	if cfg.API.Addr != ":8080" {
		t.Fatalf("expected default API addr, got %q", cfg.API.Addr)
	}
	if cfg.Database.URL != "postgres://eventrelay:eventrelay@127.0.0.1:5433/eventrelay?sslmode=disable" {
		t.Fatalf("unexpected database url: %q", cfg.Database.URL)
	}
	if cfg.RabbitMQ.URL != "amqp://guest:guest@127.0.0.1:5672/" {
		t.Fatalf("unexpected rabbitmq url: %q", cfg.RabbitMQ.URL)
	}
	if !cfg.Database.AutoMigrate {
		t.Fatal("expected local auto migration to be enabled")
	}
}

func TestLoadRequiresInfrastructureURLsOutsideLocal(t *testing.T) {
	isolateEnv(t, "APP_ENV", "DATABASE_URL", "RABBITMQ_URL")
	t.Setenv("APP_ENV", "production")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL is required") {
		t.Fatalf("expected DATABASE_URL error, got %v", err)
	}
}

func TestValidateRejectsUnsafeOrInvalidValues(t *testing.T) {
	cfg := validConfig()
	cfg.API.MaxRequestBodyBytes = 0
	cfg.Delivery.RetryBaseDelay = 5 * time.Second
	cfg.Delivery.RetryMaxDelay = time.Second
	cfg.Worker.MetricsAddr = " "

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error")
	}

	for _, want := range []string{
		"MAX_REQUEST_BODY_BYTES must be greater than zero",
		"DELIVERY_RETRY_MAX_DELAY must be greater than or equal to DELIVERY_RETRY_BASE_DELAY",
		"WORKER_METRICS_ADDR cannot be empty",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected %q in error, got %v", want, err)
		}
	}
}

func TestLoadRejectsInvalidDurationAndIntegerValues(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   string
		wantErr string
	}{
		{
			name:    "invalid duration",
			key:     "DELIVERY_TIMEOUT",
			value:   "not-a-duration",
			wantErr: "DELIVERY_TIMEOUT must be greater than zero",
		},
		{
			name:    "invalid positive integer",
			key:     "DELIVERY_MAX_ATTEMPTS",
			value:   "many",
			wantErr: "DELIVERY_MAX_ATTEMPTS must be greater than zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isolateEnv(t,
				"APP_ENV",
				"DATABASE_URL",
				"RABBITMQ_URL",
				"DELIVERY_TIMEOUT",
				"DELIVERY_MAX_ATTEMPTS",
			)
			t.Setenv("APP_ENV", "local")
			t.Setenv(tt.key, tt.value)

			_, err := Load()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected %q in error, got %v", tt.wantErr, err)
			}
		})
	}
}

func validConfig() Config {
	return Config{
		AppEnv:   "local",
		LogLevel: "info",
		API: APIConfig{
			Addr:                ":8080",
			ReadTimeout:         5 * time.Second,
			WriteTimeout:        10 * time.Second,
			IdleTimeout:         time.Minute,
			MaxRequestBodyBytes: 1_048_576,
		},
		Database: DatabaseConfig{
			URL:           "postgres://eventrelay:eventrelay@127.0.0.1:5433/eventrelay?sslmode=disable",
			AutoMigrate:   true,
			MigrationsDir: "migrations",
		},
		RabbitMQ: RabbitMQConfig{
			URL:           "amqp://guest:guest@127.0.0.1:5672/",
			Exchange:      "eventrelay.events",
			DeliveryQueue: "eventrelay.deliveries",
			DeadLetterEx:  "eventrelay.dlx",
			DeadLetterQ:   "eventrelay.deliveries.dlq",
		},
		Delivery: DeliveryConfig{
			Timeout:        5 * time.Second,
			MaxAttempts:    3,
			RetryBaseDelay: 2 * time.Second,
			RetryMaxDelay:  30 * time.Second,
		},
		Worker: WorkerConfig{
			MetricsAddr: ":8082",
		},
	}
}

func isolateEnv(t *testing.T, keys ...string) {
	t.Helper()

	originals := make(map[string]*string, len(keys))
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			copy := value
			originals[key] = &copy
		} else {
			originals[key] = nil
		}
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
	}

	t.Cleanup(func() {
		for _, key := range keys {
			value := originals[key]
			if value == nil {
				_ = os.Unsetenv(key)
				continue
			}
			_ = os.Setenv(key, *value)
		}
	})
}
