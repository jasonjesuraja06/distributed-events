package queue

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	in := &Event{
		EventID:        "evt-1",
		IdempotencyKey: "idem-1",
		Type:           "notification.send",
		Payload:        json.RawMessage(`{"user":"u_ab","amount":42}`),
		Timestamp:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		AttemptNumber:  1,
	}
	b, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.EventID != in.EventID || out.IdempotencyKey != in.IdempotencyKey || out.Type != in.Type {
		t.Fatalf("identity fields changed: %+v", out)
	}
	if out.AttemptNumber != in.AttemptNumber {
		t.Fatalf("AttemptNumber = %d, want %d", out.AttemptNumber, in.AttemptNumber)
	}
	if !out.Timestamp.Equal(in.Timestamp) {
		t.Fatalf("Timestamp = %s, want %s; delivery latency is measured from this field", out.Timestamp, in.Timestamp)
	}
	if string(out.Payload) != string(in.Payload) {
		t.Fatalf("Payload = %s, want %s", out.Payload, in.Payload)
	}
}

func TestUnmarshalRejectsMalformedJSON(t *testing.T) {
	// The consumers commit and skip on this error rather than blocking the
	// partition, so it has to be reported rather than swallowed.
	if _, err := Unmarshal([]byte("{not json")); err == nil {
		t.Fatal("Unmarshal accepted malformed JSON")
	}
}

func TestJSONFieldNamesAreStable(t *testing.T) {
	b, err := (&Event{EventID: "e", IdempotencyKey: "k"}).Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var generic map[string]any
	if err := json.Unmarshal(b, &generic); err != nil {
		t.Fatalf("unmarshal to map: %v", err)
	}
	for _, want := range []string{"event_id", "idempotency_key", "type", "payload", "timestamp", "attempt_number"} {
		if _, ok := generic[want]; !ok {
			t.Errorf("wire format is missing field %q", want)
		}
	}
}
