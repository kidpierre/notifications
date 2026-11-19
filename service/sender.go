package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"notifications/entity"
)

type Publisher interface {
	Publish(ctx context.Context, n entity.Notification) error
}

type Sender struct {
	publisher Publisher
	timeout   time.Duration
	logger    *slog.Logger
}

func NewSender(publisher Publisher, timeout time.Duration, logger *slog.Logger) *Sender {
	return &Sender{publisher: publisher, timeout: timeout, logger: logger}
}

func (s *Sender) Send(ctx context.Context, n entity.Notification) error {
	if err := n.Validate(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	if err := s.publisher.Publish(ctx, n); err != nil {
		return fmt.Errorf("publish notification: %w", err)
	}
	s.logger.InfoContext(ctx, "notification sent", "user_id", n.UserID, "type", n.Type)
	return nil
}
