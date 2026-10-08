package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/julimeimei/eventrelay-go/internal/config"
	"github.com/julimeimei/eventrelay-go/internal/delivery"
	"github.com/julimeimei/eventrelay-go/internal/observability"
	"github.com/julimeimei/eventrelay-go/internal/queue"
	"github.com/julimeimei/eventrelay-go/internal/repository"
	"github.com/julimeimei/eventrelay-go/internal/service"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("worker stopped: %v", err)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger, err := observability.NewLogger(cfg.LogLevel)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := repository.OpenPostgres(ctx, cfg.Database.URL)
	if err != nil {
		return err
	}
	defer db.Close()

	if cfg.Database.AutoMigrate {
		if err := repository.RunMigrations(ctx, db, cfg.Database.MigrationsDir); err != nil {
			return err
		}
		logger.InfoContext(ctx, "database migrations applied",
			slog.String("dir", cfg.Database.MigrationsDir),
		)
	}

	consumer, err := queue.NewRabbitMQConsumer(queue.Config{
		URL:                cfg.RabbitMQ.URL,
		Exchange:           cfg.RabbitMQ.Exchange,
		DeliveryQueue:      cfg.RabbitMQ.DeliveryQueue,
		DeadLetterExchange: cfg.RabbitMQ.DeadLetterEx,
		DeadLetterQueue:    cfg.RabbitMQ.DeadLetterQ,
	})
	if err != nil {
		return err
	}
	defer consumer.Close()

	store := repository.NewPostgresEventRepository(db)
	deliveryClient := delivery.NewHTTPClient(cfg.Delivery.Timeout)
	retryPolicy := delivery.NewRetryPolicy(
		cfg.Delivery.MaxAttempts,
		cfg.Delivery.RetryBaseDelay,
		cfg.Delivery.RetryMaxDelay,
	)
	worker := service.NewWorker(store, deliveryClient, retryPolicy)

	logger.InfoContext(ctx, "starting worker",
		slog.String("app_env", cfg.AppEnv),
		slog.String("delivery_queue", cfg.RabbitMQ.DeliveryQueue),
		slog.String("dead_letter_queue", cfg.RabbitMQ.DeadLetterQ),
		slog.Int("delivery_max_attempts", cfg.Delivery.MaxAttempts),
	)

	metricsServer := &http.Server{
		Addr:              cfg.Worker.MetricsAddr,
		Handler:           workerObservabilityHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		logger.InfoContext(ctx, "starting worker observability server",
			slog.String("addr", cfg.Worker.MetricsAddr),
		)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("worker observability server: %w", err)
			return
		}
	}()

	go func() {
		errCh <- consumer.Consume(ctx, func(ctx context.Context, message queue.EventCreatedMessage) error {
			return processMessage(ctx, logger, cfg.RabbitMQ.DeadLetterQ, worker, message)
		})
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		if err != nil {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		return err
	}

	logger.Info("worker stopped")
	return nil
}

func processMessage(ctx context.Context, logger *slog.Logger, deadLetterQueue string, worker *service.Worker, message queue.EventCreatedMessage) error {
	ctx = observability.WithRequestID(ctx, message.CorrelationID)

	logger.InfoContext(ctx, "processing event delivery",
		slog.String("event_id", message.EventID),
		slog.String("event_type", message.EventType),
		slog.String("correlation_id", message.CorrelationID),
	)

	if err := worker.ProcessEventCreated(ctx, message); err != nil {
		if errors.Is(err, service.ErrRetryScheduled) {
			logger.InfoContext(ctx, "event delivery scheduled for retry",
				slog.String("event_id", message.EventID),
				slog.String("correlation_id", message.CorrelationID),
				slog.String("error", err.Error()),
			)
			return err
		}
		if errors.Is(err, queue.ErrDeadLetter) {
			logger.WarnContext(ctx, "event delivery sent to dead letter queue",
				slog.String("event_id", message.EventID),
				slog.String("correlation_id", message.CorrelationID),
				slog.String("dead_letter_queue", deadLetterQueue),
				slog.String("error", err.Error()),
			)
			return err
		}

		logger.ErrorContext(ctx, "event delivery failed",
			slog.String("event_id", message.EventID),
			slog.String("correlation_id", message.CorrelationID),
			slog.String("error", err.Error()),
		)
		return err
	}

	logger.InfoContext(ctx, "event delivery processed",
		slog.String("event_id", message.EventID),
		slog.String("correlation_id", message.CorrelationID),
	)
	return nil
}

func workerObservabilityHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"service": "eventrelay-worker",
		})
	})
	mux.Handle("GET /metrics", observability.DefaultMetrics.Handler())
	return mux
}
