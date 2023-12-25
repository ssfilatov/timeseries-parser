package record

import (
	"time"
)

// InternalRecord represents record object used internally, encoded to msgpack on disk
type InternalRecord struct {
	Email string
	SessionID string
	Timestamp int64
}

func ConvertInternalRecordToAPI(r *InternalRecord) *APIRecord {
	return &APIRecord{
		Email: r.Email,
		SessionID: r.SessionID,
		EventTime: time.Unix(r.Timestamp, 0).Format(time.RFC3339),
	}
}

// APIRecord represent record object encoded to json and used in http server
type APIRecord struct {
	Email       string  `json:"email"`
	SessionID     string `json:"sessionId"`
	EventTime string  `json:"eventTime"`
}