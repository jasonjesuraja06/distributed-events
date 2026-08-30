package main

import (
	"testing"
	"time"
)

func TestSinceEventMeasuresAgeFromTheProducerStamp(t *testing.T) {
	got := sinceEvent(time.Now().Add(-2 * time.Second))
	if got < 1.5 || got > 3.0 {
		t.Fatalf("sinceEvent(2s ago) = %v, want roughly 2", got)
	}
}

func TestSinceEventClampsClockSkewToZero(t *testing.T) {
	// The producer runs on the host and the consumer in a container. A stamp
	// from the future must not be recorded as a negative latency observation.
	if got := sinceEvent(time.Now().Add(5 * time.Second)); got != 0 {
		t.Fatalf("sinceEvent(future stamp) = %v, want 0", got)
	}
}
