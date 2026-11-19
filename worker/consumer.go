package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"notifications/broker"
	"notifications/entity"
)

const commitTimeout = 5 * time.Second

type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Processor interface {
	Process(ctx context.Context, n entity.Notification) error
}

type Consumer struct {
	reader    Reader
	dlq       Writer
	processor Processor
	logger    *slog.Logger
}

func NewConsumer(reader Reader, dlq Writer, processor Processor, logger *slog.Logger) *Consumer {
	return &Consumer{reader: reader, dlq: dlq, processor: processor, logger: logger}
}

func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("consumer started")
	defer c.logger.Info("consumer stopped")

	for ctx.Err() == nil {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		if err := c.handle(ctx, m); err != nil {
			return err
		}

		commitCtx, cancel := context.WithTimeout(context.Background(), commitTimeout)
		err = c.reader.CommitMessages(commitCtx, m)
		cancel()
		if err != nil {
			c.logger.Error("commit offset", "partition", m.Partition, "offset", m.Offset, "err", err)
			continue
		}
		c.logger.Info("offset committed", "partition", m.Partition, "offset", m.Offset)
	}
	return nil
}

func (c *Consumer) handle(ctx context.Context, m kafka.Message) error {
	n, err := broker.Decode(m)
	if err == nil {
		err = c.processor.Process(context.WithoutCancel(ctx), n)
	}
	if err == nil {
		return nil
	}

	dlqCtx, cancel := context.WithTimeout(context.Background(), commitTimeout)
	defer cancel()
	if dlqErr := c.dlq.WriteMessages(dlqCtx, broker.DeadLetter(m, err)); dlqErr != nil {
		return fmt.Errorf("send to DLQ (partition %d, offset %d): %w", m.Partition, m.Offset, dlqErr)
	}
	c.logger.Warn("message sent to DLQ", "partition", m.Partition, "offset", m.Offset, "reason", err.Error())
	return nil
}
