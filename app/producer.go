package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"notifications/broker"
	"notifications/config"
	handlerhttp "notifications/handler/http"
	"notifications/service"
)

func RunProducer(cfg *config.Producer) error {
	logger := newLogger("producer", cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	writer := broker.NewWriter(cfg.Brokers, cfg.Topic)
	sender := service.NewSender(broker.NewPublisher(writer), cfg.KafkaWriteTimeout, logger)
	handler := handlerhttp.New(sender, logger)

	server := &http.Server{
		Addr:              ":" + cfg.HTTPPort,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: cfg.KafkaWriteTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("http server started", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErr:
		runErr = fmt.Errorf("http server: %w", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("shutdown http server: %w", err))
	}
	if err := writer.Close(); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("close kafka writer: %w", err))
	}

	logger.Info("service stopped")
	return runErr
}

func newLogger(service string, level slog.Level) *slog.Logger {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})).With("service", service)
	slog.SetDefault(logger)
	return logger
}
