package http

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"notifications/entity"
)

type fakeSender struct {
	sent []entity.Notification
	err  error
}

func (s *fakeSender) Send(_ context.Context, n entity.Notification) error {
	if err := n.Validate(); err != nil {
		return err
	}
	if s.err != nil {
		return s.err
	}
	s.sent = append(s.sent, n)
	return nil
}

func TestCreateNotification(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		sendErr error
		status  int
	}{
		{"accepted", `{"user_id":"u1","type":"email","content":"hi"}`, nil, http.StatusAccepted},
		{"invalid json", `{oops`, nil, http.StatusBadRequest},
		{"unknown field", `{"user_id":"u1","type":"email","content":"hi","x":1}`, nil, http.StatusBadRequest},
		{"missing user_id", `{"type":"email","content":"hi"}`, nil, http.StatusBadRequest},
		{"kafka unavailable", `{"user_id":"u1","type":"email","content":"hi"}`, errors.New("dial tcp"), http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender := &fakeSender{err: tt.sendErr}
			h := New(sender, slog.New(slog.NewTextHandler(io.Discard, nil)))

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/notifications", strings.NewReader(tt.body))
			h.Routes().ServeHTTP(rec, req)

			assert.Equal(t, tt.status, rec.Code, rec.Body.String())
			if tt.status == http.StatusAccepted {
				assert.Equal(t, []entity.Notification{{UserID: "u1", Type: "email", Content: "hi"}}, sender.sent)
			}
		})
	}
}
