package model

import (
	"errors"
	"fmt"
)

// FieldError is a validation failure on one input field. The API renders it
// as a 400 (or the status its Code maps to).
type FieldError struct {
	Field   string
	Code    string
	Message string
}

func (e *FieldError) Error() string {
	if e.Field == "" {
		return e.Message
	}
	return e.Field + ": " + e.Message
}

func fieldErr(field, code, format string, args ...any) error {
	return &FieldError{Field: field, Code: code, Message: fmt.Sprintf(format, args...)}
}

// AsFieldError unwraps a FieldError.
func AsFieldError(err error) (*FieldError, bool) {
	var fe *FieldError
	ok := errors.As(err, &fe)
	return fe, ok
}

// Validation error codes that map to statuses other than 400.
const (
	CodeForbidden        = "forbidden"          // 403
	CodeTargetNotEnabled = "target_not_enabled" // 400
)
