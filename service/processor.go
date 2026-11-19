package service

import (
	"context"
	"log/slog"
	"time"

	"notifications/entity"
)

const processingTime = 500 * time.Millisecond

type Processor struct {
	logger *slog.Logger
}

func NewProcessor(logger *slog.Logger) *Processor {
	return &Processor{logger: logger}
}

func (p *Processor) Process(ctx context.Context, n entity.Notification) error {
	p.logger.InfoContext(ctx, "processing notification", "user_id", n.UserID, "type", n.Type, "content", n.Content)
	time.Sleep(processingTime)
	return nil
}
