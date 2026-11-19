package http

import (
	"errors"
	"net/http"

	"notifications/entity"
)

type notificationRequest struct {
	UserID  string `json:"user_id"`
	Type    string `json:"type"`
	Content string `json:"content"`
}

func (h *Handler) createNotification(w http.ResponseWriter, r *http.Request) {
	var req notificationRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeDecodeError(w, err)
		return
	}

	err := h.sender.Send(r.Context(), entity.Notification{UserID: req.UserID, Type: req.Type, Content: req.Content})
	if err != nil {
		var ve *entity.ValidationError
		if errors.As(err, &ve) {
			writeError(w, http.StatusBadRequest, ve.Message)
			return
		}
		h.logger.ErrorContext(r.Context(), "send notification", "err", err)
		writeError(w, http.StatusServiceUnavailable, "notification service is unavailable")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}
