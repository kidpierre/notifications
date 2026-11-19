package app

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"syscall"

	"notifications/broker"
	"notifications/config"
	"notifications/service"
	"notifications/worker"
)

func RunConsumer(cfg *config.Consumer) error {
	logger := newLogger("consumer", cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reader := broker.NewReader(cfg.Brokers, cfg.Topic, cfg.GroupID, logger)
	dlq := broker.NewWriter(cfg.Brokers, cfg.DLQTopic)
	consumer := worker.NewConsumer(reader, dlq, service.NewProcessor(logger), logger)

	runErr := consumer.Run(ctx)

	if err := reader.Close(); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("close kafka reader: %w", err))
	}
	if err := dlq.Close(); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("close dlq writer: %w", err))
	}

	logger.Info("service stopped")
	return runErr
}
