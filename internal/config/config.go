package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const localEnv = "local"

type Config struct {
	AppEnv   string
	LogLevel string
	API      APIConfig
	Database DatabaseConfig
	RabbitMQ RabbitMQConfig
	Delivery DeliveryConfig
	Worker   WorkerConfig
}

type APIConfig struct {
	Addr                string
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	IdleTimeout         time.Duration
	MaxRequestBodyBytes int64
}

type DatabaseConfig struct {
	URL           string
	AutoMigrate   bool
	MigrationsDir string
}

type RabbitMQConfig struct {
	URL           string
	Exchange      string
	DeliveryQueue string
	DeadLetterEx  string
	DeadLetterQ   string
}

type DeliveryConfig struct {
	Timeout        time.Duration
	MaxAttempts    int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

type WorkerConfig struct {
	MetricsAddr string
}

func Load() (Config, error) {
	appEnv := envString("APP_ENV", localEnv)

	cfg := Config{
		AppEnv:   appEnv,
		LogLevel: envString("LOG_LEVEL", "info"),
		API: APIConfig{
			Addr:        envString("API_ADDR", ":8080"),
			ReadTimeout: mustDuration("READ_TIMEOUT", 5*time.Second),
			// WriteTimeout includes the full response write window. Keep it short for this API.
			WriteTimeout:        mustDuration("WRITE_TIMEOUT", 10*time.Second),
			IdleTimeout:         mustDuration("IDLE_TIMEOUT", 60*time.Second),
			MaxRequestBodyBytes: int64(mustPositiveInt("MAX_REQUEST_BODY_BYTES", 1_048_576)),
		},
		Database: DatabaseConfig{
			AutoMigrate:   envBool("DATABASE_AUTO_MIGRATE", appEnv == localEnv),
			MigrationsDir: envString("DATABASE_MIGRATIONS_DIR", "migrations"),
		},
		RabbitMQ: RabbitMQConfig{
			Exchange:      envString("RABBITMQ_EXCHANGE", "eventrelay.events"),
			DeliveryQueue: envString("RABBITMQ_DELIVERY_QUEUE", "eventrelay.deliveries"),
			DeadLetterEx:  envString("RABBITMQ_DLX", "eventrelay.dlx"),
			DeadLetterQ:   envString("RABBITMQ_DLQ", "eventrelay.deliveries.dlq"),
		},
		Delivery: DeliveryConfig{
			Timeout:        mustDuration("DELIVERY_TIMEOUT", 5*time.Second),
			MaxAttempts:    mustPositiveInt("DELIVERY_MAX_ATTEMPTS", 3),
			RetryBaseDelay: mustDuration("DELIVERY_RETRY_BASE_DELAY", 2*time.Second),
			RetryMaxDelay:  mustDuration("DELIVERY_RETRY_MAX_DELAY", 30*time.Second),
		},
		Worker: WorkerConfig{
			MetricsAddr: envString("WORKER_METRICS_ADDR", ":8082"),
		},
	}

	var err error
	cfg.Database.URL, err = envRequiredOutsideLocal("DATABASE_URL", "postgres://eventrelay:eventrelay@127.0.0.1:5433/eventrelay?sslmode=disable", appEnv)
	if err != nil {
		return Config{}, err
	}

	cfg.RabbitMQ.URL, err = envRequiredOutsideLocal("RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/", appEnv)
	if err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (cfg Config) Validate() error {
	var errs []error

	if strings.TrimSpace(cfg.AppEnv) == "" {
		errs = append(errs, errors.New("APP_ENV cannot be empty"))
	}
	if strings.TrimSpace(cfg.LogLevel) == "" {
		errs = append(errs, errors.New("LOG_LEVEL cannot be empty"))
	}
	if strings.TrimSpace(cfg.API.Addr) == "" {
		errs = append(errs, errors.New("API_ADDR cannot be empty"))
	}
	if strings.TrimSpace(cfg.Database.URL) == "" {
		errs = append(errs, errors.New("DATABASE_URL cannot be empty"))
	}
	if strings.TrimSpace(cfg.Database.MigrationsDir) == "" {
		errs = append(errs, errors.New("DATABASE_MIGRATIONS_DIR cannot be empty"))
	}
	if strings.TrimSpace(cfg.RabbitMQ.URL) == "" {
		errs = append(errs, errors.New("RABBITMQ_URL cannot be empty"))
	}
	if strings.TrimSpace(cfg.RabbitMQ.Exchange) == "" {
		errs = append(errs, errors.New("RABBITMQ_EXCHANGE cannot be empty"))
	}
	if strings.TrimSpace(cfg.RabbitMQ.DeliveryQueue) == "" {
		errs = append(errs, errors.New("RABBITMQ_DELIVERY_QUEUE cannot be empty"))
	}
	if strings.TrimSpace(cfg.RabbitMQ.DeadLetterEx) == "" {
		errs = append(errs, errors.New("RABBITMQ_DLX cannot be empty"))
	}
	if strings.TrimSpace(cfg.RabbitMQ.DeadLetterQ) == "" {
		errs = append(errs, errors.New("RABBITMQ_DLQ cannot be empty"))
	}
	if cfg.API.ReadTimeout <= 0 {
		errs = append(errs, errors.New("READ_TIMEOUT must be greater than zero"))
	}
	if cfg.API.WriteTimeout <= 0 {
		errs = append(errs, errors.New("WRITE_TIMEOUT must be greater than zero"))
	}
	if cfg.API.IdleTimeout <= 0 {
		errs = append(errs, errors.New("IDLE_TIMEOUT must be greater than zero"))
	}
	if cfg.API.MaxRequestBodyBytes <= 0 {
		errs = append(errs, errors.New("MAX_REQUEST_BODY_BYTES must be greater than zero"))
	}
	if cfg.Delivery.Timeout <= 0 {
		errs = append(errs, errors.New("DELIVERY_TIMEOUT must be greater than zero"))
	}
	if cfg.Delivery.MaxAttempts <= 0 {
		errs = append(errs, errors.New("DELIVERY_MAX_ATTEMPTS must be greater than zero"))
	}
	if cfg.Delivery.RetryBaseDelay <= 0 {
		errs = append(errs, errors.New("DELIVERY_RETRY_BASE_DELAY must be greater than zero"))
	}
	if cfg.Delivery.RetryMaxDelay <= 0 {
		errs = append(errs, errors.New("DELIVERY_RETRY_MAX_DELAY must be greater than zero"))
	}
	if cfg.Delivery.RetryMaxDelay < cfg.Delivery.RetryBaseDelay {
		errs = append(errs, errors.New("DELIVERY_RETRY_MAX_DELAY must be greater than or equal to DELIVERY_RETRY_BASE_DELAY"))
	}
	if strings.TrimSpace(cfg.Worker.MetricsAddr) == "" {
		errs = append(errs, errors.New("WORKER_METRICS_ADDR cannot be empty"))
	}

	return errors.Join(errs...)
}

func envString(key string, fallback string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	return strings.TrimSpace(value)
}

func envRequiredOutsideLocal(key string, localFallback string, appEnv string) (string, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		if appEnv == localEnv {
			return localFallback, nil
		}
		return "", fmt.Errorf("%s is required when APP_ENV is not local", key)
	}

	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s cannot be empty", key)
	}

	return value, nil
}

func mustDuration(key string, fallback time.Duration) time.Duration {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}

	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return -1
	}

	return value
}

func mustPositiveInt(key string, fallback int) int {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}

	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return -1
	}

	return value
}

func envBool(key string, fallback bool) bool {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}

	value, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}

	return value
}
