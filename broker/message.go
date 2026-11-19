package broker

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/segmentio/kafka-go"

	"notifications/entity"
)

const (
	headerType              = "type"
	headerError             = "error"
	headerOriginalTopic     = "original-topic"
	headerOriginalPartition = "original-partition"
	headerOriginalOffset    = "original-offset"
)

type payload struct {
	UserID  string `json:"user_id"`
	Content string `json:"content"`
}

func Encode(n entity.Notification) (kafka.Message, error) {
	body, err := json.Marshal(payload{UserID: n.UserID, Content: n.Content})
	if err != nil {
		return kafka.Message{}, err
	}
	return kafka.Message{
		Key:     []byte(n.UserID),
		Value:   body,
		Headers: []kafka.Header{{Key: headerType, Value: []byte(n.Type)}},
	}, nil
}

func Decode(m kafka.Message) (entity.Notification, error) {
	var p payload
	if err := json.Unmarshal(m.Value, &p); err != nil {
		return entity.Notification{}, fmt.Errorf("decode payload: %w", err)
	}
	n := entity.Notification{UserID: p.UserID, Type: header(m, headerType), Content: p.Content}
	if err := n.Validate(); err != nil {
		return entity.Notification{}, err
	}
	return n, nil
}

func DeadLetter(m kafka.Message, cause error) kafka.Message {
	headers := append([]kafka.Header{}, m.Headers...)
	headers = append(headers,
		kafka.Header{Key: headerError, Value: []byte(cause.Error())},
		kafka.Header{Key: headerOriginalTopic, Value: []byte(m.Topic)},
		kafka.Header{Key: headerOriginalPartition, Value: []byte(strconv.Itoa(m.Partition))},
		kafka.Header{Key: headerOriginalOffset, Value: []byte(strconv.FormatInt(m.Offset, 10))},
	)
	return kafka.Message{Key: m.Key, Value: m.Value, Headers: headers}
}

func header(m kafka.Message, key string) string {
	for _, h := range m.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
