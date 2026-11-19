package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"notifications/broker"
	"notifications/entity"
)

type fakeReader struct {
	mu        sync.Mutex
	queue     []kafka.Message
	committed []int64
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	if len(r.queue) > 0 {
		m := r.queue[0]
		r.queue = r.queue[1:]
		r.mu.Unlock()
		return m, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range msgs {
		r.committed = append(r.committed, m.Offset)
	}
	return nil
}

func (r *fakeReader) commits() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.committed...)
}

type fakeWriter struct {
	mu   sync.Mutex
	msgs []kafka.Message
	err  error
}

func (w *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.msgs = append(w.msgs, msgs...)
	return nil
}

type fakeProcessor struct {
	reader          *fakeReader
	commitsAtStart  []int
	processed       []entity.Notification
	cancelOnProcess context.CancelFunc
}

func (p *fakeProcessor) Process(_ context.Context, n entity.Notification) error {
	p.commitsAtStart = append(p.commitsAtStart, len(p.reader.commits()))
	if p.cancelOnProcess != nil {
		p.cancelOnProcess()
	}
	p.processed = append(p.processed, n)
	return nil
}

func message(t *testing.T, offset int64, n entity.Notification) kafka.Message {
	t.Helper()
	m, err := broker.Encode(n)
	require.NoError(t, err)
	m.Offset = offset
	return m
}

func runUntilCommitted(t *testing.T, c *Consumer, r *fakeReader, want int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	require.Eventually(t, func() bool { return len(r.commits()) >= want }, 2*time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestConsumer_CommitsAfterProcessing(t *testing.T) {
	n := entity.Notification{UserID: "u1", Type: "email", Content: "hi"}
	r := &fakeReader{queue: []kafka.Message{message(t, 10, n), message(t, 11, n)}}
	p := &fakeProcessor{reader: r}

	runUntilCommitted(t, NewConsumer(r, &fakeWriter{}, p, discard), r, 2)

	assert.Equal(t, []int64{10, 11}, r.commits())
	assert.Equal(t, []int{0, 1}, p.commitsAtStart)
	assert.Equal(t, n, p.processed[0])
}

func TestConsumer_BrokenMessageGoesToDLQ(t *testing.T) {
	r := &fakeReader{queue: []kafka.Message{{Offset: 5, Value: []byte("{not json")}}}
	dlq := &fakeWriter{}
	p := &fakeProcessor{reader: r}

	runUntilCommitted(t, NewConsumer(r, dlq, p, discard), r, 1)

	assert.Empty(t, p.processed)
	require.Len(t, dlq.msgs, 1)
	assert.Equal(t, []byte("{not json"), dlq.msgs[0].Value)
	assert.Equal(t, []int64{5}, r.commits(), "битое сообщение коммитится, чтобы consumer не застрял на нём")
}

func TestConsumer_DLQUnavailable_NoCommit(t *testing.T) {
	r := &fakeReader{queue: []kafka.Message{{Offset: 5, Value: []byte("{not json")}}}
	dlq := &fakeWriter{err: errors.New("broker is down")}

	err := NewConsumer(r, dlq, &fakeProcessor{reader: r}, discard).Run(context.Background())

	require.ErrorContains(t, err, "broker is down")
	assert.Empty(t, r.commits())
}

func TestConsumer_ShutdownFinishesCurrentMessage(t *testing.T) {
	n := entity.Notification{UserID: "u1", Type: "email", Content: "hi"}
	r := &fakeReader{queue: []kafka.Message{message(t, 1, n), message(t, 2, n)}}
	ctx, cancel := context.WithCancel(context.Background())
	p := &fakeProcessor{reader: r, cancelOnProcess: cancel}

	require.NoError(t, NewConsumer(r, &fakeWriter{}, p, discard).Run(ctx))

	assert.Len(t, p.processed, 1, "после сигнала новые сообщения не берутся")
	assert.Equal(t, []int64{1}, r.commits(), "начатое сообщение доработано и закоммичено")
}
