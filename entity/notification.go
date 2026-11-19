package entity

import "strings"

type Notification struct {
	UserID  string
	Type    string
	Content string
}

func (n Notification) Validate() error {
	switch {
	case strings.TrimSpace(n.UserID) == "":
		return &ValidationError{Message: "user_id is required"}
	case strings.TrimSpace(n.Type) == "":
		return &ValidationError{Message: "type is required"}
	case strings.TrimSpace(n.Content) == "":
		return &ValidationError{Message: "content is required"}
	}
	return nil
}

type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}
