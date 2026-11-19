package broker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"notifications/entity"
)

func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
}

func NewReader(brokers []string, topic, groupID string, logger *slog.Logger) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 0,
		Logger:         kafka.LoggerFunc(groupEvents(logger)),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...any) {
			logger.Error("kafka: " + fmt.Sprintf(msg, args...))
		}),
	})
}

func groupEvents(logger *slog.Logger) func(string, ...any) {
	return func(msg string, args ...any) {
		s := fmt.Sprintf(msg, args...)
		if strings.Contains(s, "subscribed to") || strings.Contains(s, "Leaving group") {
			logger.Info("kafka: " + s)
		}
	}
}

type Publisher struct {
	writer *kafka.Writer
}

func NewPublisher(writer *kafka.Writer) *Publisher {
	return &Publisher{writer: writer}
}

func (p *Publisher) Publish(ctx context.Context, n entity.Notification) error {
	m, err := Encode(n)
	if err != nil {
		return fmt.Errorf("encode notification: %w", err)
	}
	return p.writer.WriteMessages(ctx, m)
}
