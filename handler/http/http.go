package http

import (
	"context"
	"log/slog"
	"net/http"

	"notifications/entity"
)

type Sender interface {
	Send(ctx context.Context, n entity.Notification) error
}

type Handler struct {
	sender Sender
	logger *slog.Logger
}

func New(sender Sender, logger *slog.Logger) *Handler {
	return &Handler{sender: sender, logger: logger}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/notifications", h.createNotification)

	return mux
}
