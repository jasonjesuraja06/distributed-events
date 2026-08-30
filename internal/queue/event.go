package queue

import (
	"encoding/json"
	"time"
)

// Event is the message contract flowing producer -> Kafka -> consumer -> downstream.
//
// IdempotencyKey is the dedup unit. Duplicate publishes (network retry, at-least-once
// producer semantics, upstream replay) share an IdempotencyKey but get distinct
// EventIDs. The idempotent consumer collapses these; the naive baseline does not.
type Event struct {
	EventID        string          `json:"event_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Timestamp      time.Time       `json:"timestamp"`
	AttemptNumber  int             `json:"attempt_number"`
}

func (e *Event) Marshal() ([]byte, error) { return json.Marshal(e) }
func Unmarshal(b []byte) (*Event, error) {
	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, err
	}
	return &e, nil
}
