package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/julimeimei/eventrelay-go/internal/config"
	httpapi "github.com/julimeimei/eventrelay-go/internal/http"
	"github.com/julimeimei/eventrelay-go/internal/observability"
	"github.com/julimeimei/eventrelay-go/internal/queue"
	"github.com/julimeimei/eventrelay-go/internal/repository"
	"github.com/julimeimei/eventrelay-go/internal/service"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("api stopped: %v", err)
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

	eventRepository := repository.NewPostgresEventRepository(db)

	publisher, err := queue.NewRabbitMQPublisher(queue.Config{
		URL:                cfg.RabbitMQ.URL,
		Exchange:           cfg.RabbitMQ.Exchange,
		DeliveryQueue:      cfg.RabbitMQ.DeliveryQueue,
		DeadLetterExchange: cfg.RabbitMQ.DeadLetterEx,
		DeadLetterQueue:    cfg.RabbitMQ.DeadLetterQ,
	})
	if err != nil {
		return err
	}
	defer publisher.Close()

	logger.InfoContext(ctx, "rabbitmq publisher connected",
		slog.String("exchange", cfg.RabbitMQ.Exchange),
		slog.String("delivery_queue", cfg.RabbitMQ.DeliveryQueue),
		slog.String("dead_letter_exchange", cfg.RabbitMQ.DeadLetterEx),
		slog.String("dead_letter_queue", cfg.RabbitMQ.DeadLetterQ),
	)

	eventService := service.NewEventService(eventRepository, publisher)

	server := &http.Server{
		Addr:         cfg.API.Addr,
		Handler:      httpapi.NewRouter(logger, eventService, cfg.API.MaxRequestBodyBytes, db.PingContext),
		ReadTimeout:  cfg.API.ReadTimeout,
		WriteTimeout: cfg.API.WriteTimeout,
		IdleTimeout:  cfg.API.IdleTimeout,
	}

	errCh := make(chan error, 1)

	go func() {
		logger.InfoContext(ctx, "starting api server",
			slog.String("addr", cfg.API.Addr),
			slog.String("app_env", cfg.AppEnv),
		)

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}

		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}

	logger.Info("api server stopped")
	return nil
}
