package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

type Kafka struct {
	Brokers []string
	Topic   string
}

type Producer struct {
	Kafka
	HTTPPort          string
	KafkaWriteTimeout time.Duration
	ShutdownTimeout   time.Duration
	LogLevel          slog.Level
}

type Consumer struct {
	Kafka
	GroupID  string
	DLQTopic string
	LogLevel slog.Level
}

func LoadProducer() (*Producer, error) {
	kafka, err := loadKafka()
	if err != nil {
		return nil, err
	}
	cfg := &Producer{Kafka: kafka, HTTPPort: getEnv("HTTP_PORT", "8080")}

	if cfg.KafkaWriteTimeout, err = getDuration("KAFKA_WRITE_TIMEOUT", 5*time.Second); err != nil {
		return nil, err
	}
	if cfg.ShutdownTimeout, err = getDuration("SHUTDOWN_TIMEOUT", 10*time.Second); err != nil {
		return nil, err
	}
	if cfg.LogLevel, err = getLogLevel(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func LoadConsumer() (*Consumer, error) {
	kafka, err := loadKafka()
	if err != nil {
		return nil, err
	}
	cfg := &Consumer{
		Kafka:    kafka,
		GroupID:  os.Getenv("KAFKA_GROUP_ID"),
		DLQTopic: os.Getenv("KAFKA_DLQ_TOPIC"),
	}
	if cfg.LogLevel, err = getLogLevel(); err != nil {
		return nil, err
	}

	if cfg.GroupID == "" {
		return nil, errors.New("KAFKA_GROUP_ID is required")
	}
	if cfg.DLQTopic == "" {
		return nil, errors.New("KAFKA_DLQ_TOPIC is required")
	}
	return cfg, nil
}

func loadKafka() (Kafka, error) {
	k := Kafka{Topic: os.Getenv("KAFKA_TOPIC")}
	for _, b := range strings.Split(os.Getenv("KAFKA_BROKERS"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			k.Brokers = append(k.Brokers, b)
		}
	}

	if len(k.Brokers) == 0 {
		return Kafka{}, errors.New("KAFKA_BROKERS is required")
	}
	if k.Topic == "" {
		return Kafka{}, errors.New("KAFKA_TOPIC is required")
	}
	return k, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be positive", key)
	}
	return d, nil
}

func getLogLevel() (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(getEnv("LOG_LEVEL", "info"))); err != nil {
		return 0, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	return level, nil
}
