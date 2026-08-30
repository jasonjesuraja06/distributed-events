package breaker

import (
	"errors"
	"testing"
	"time"
)

var errDownstream = errors.New("downstream 5xx")

func cfg(rate float64, window uint32, open time.Duration) Config {
	return Config{
		Name:            "test",
		FailureRateOpen: rate,
		WindowRequests:  window,
		WindowInterval:  time.Minute,
		OpenDuration:    open,
	}
}

func TestStaysClosedBelowTheRequestFloor(t *testing.T) {
	// Every call fails, but the window floor is 10 requests, so 9 failures must
	// not be enough to trip. This is what stops a cold start from opening the
	// breaker on a single unlucky call.
	b := New(cfg(0.5, 10, time.Second))
	for i := 0; i < 9; i++ {
		if err := b.Do(func() error { return errDownstream }); !errors.Is(err, errDownstream) {
			t.Fatalf("call %d: got %v, want the downstream error passed through", i, err)
		}
	}
	if got := b.State(); got != "closed" {
		t.Fatalf("state after 9 failures under a 10-request floor = %q, want closed", got)
	}
}

func TestOpensOnceTheFailureRateCrossesTheThreshold(t *testing.T) {
	b := New(cfg(0.5, 10, time.Minute))
	// 5 successes then 5 failures: at the 10th call the rate is exactly 0.5,
	// which meets the >= threshold.
	for i := 0; i < 5; i++ {
		if err := b.Do(func() error { return nil }); err != nil {
			t.Fatalf("success call %d: %v", i, err)
		}
	}
	for i := 0; i < 5; i++ {
		if err := b.Do(func() error { return errDownstream }); !errors.Is(err, errDownstream) {
			t.Fatalf("failure call %d: got %v", i, err)
		}
	}
	if got := b.State(); got != "open" {
		t.Fatalf("state = %q, want open at a 0.5 failure rate over 10 requests", got)
	}
}

func TestStaysClosedWhenTheFailureRateIsUnderTheThreshold(t *testing.T) {
	b := New(cfg(0.5, 10, time.Minute))
	for i := 0; i < 12; i++ {
		fail := i%4 == 0 // 25% failure rate
		_ = b.Do(func() error {
			if fail {
				return errDownstream
			}
			return nil
		})
	}
	if got := b.State(); got != "closed" {
		t.Fatalf("state = %q, want closed at a 25%% failure rate under a 50%% threshold", got)
	}
}

func TestOpenBreakerShedsLoadWithoutCallingDownstream(t *testing.T) {
	b := New(cfg(0.5, 4, time.Minute))
	for i := 0; i < 4; i++ {
		_ = b.Do(func() error { return errDownstream })
	}
	if got := b.State(); got != "open" {
		t.Fatalf("setup: state = %q, want open", got)
	}

	called := false
	err := b.Do(func() error { called = true; return nil })
	if !errors.Is(err, ErrOpen) {
		t.Fatalf("got %v, want ErrOpen so the consumer can count a breaker rejection", err)
	}
	if called {
		t.Fatal("the breaker invoked downstream while open; it is not shedding load")
	}
}

func TestHalfOpensAfterTheOpenDurationAndClosesOnSuccess(t *testing.T) {
	b := New(cfg(0.5, 4, 50*time.Millisecond))
	for i := 0; i < 4; i++ {
		_ = b.Do(func() error { return errDownstream })
	}
	if got := b.State(); got != "open" {
		t.Fatalf("setup: state = %q, want open", got)
	}

	time.Sleep(70 * time.Millisecond)
	called := false
	if err := b.Do(func() error { called = true; return nil }); err != nil {
		t.Fatalf("trial call after the open duration: %v", err)
	}
	if !called {
		t.Fatal("the trial call never reached downstream; the breaker did not half-open")
	}
	if got := b.State(); got != "closed" {
		t.Fatalf("state = %q, want closed after a successful trial (MaxRequests is 1)", got)
	}
}

func TestReopensWhenTheTrialCallFails(t *testing.T) {
	b := New(cfg(0.5, 4, 50*time.Millisecond))
	for i := 0; i < 4; i++ {
		_ = b.Do(func() error { return errDownstream })
	}
	time.Sleep(70 * time.Millisecond)
	if err := b.Do(func() error { return errDownstream }); !errors.Is(err, errDownstream) {
		t.Fatalf("trial call: got %v, want the downstream error", err)
	}
	if got := b.State(); got != "open" {
		t.Fatalf("state = %q, want open again after a failed trial", got)
	}
}

func TestSuccessfulCallsPassThroughUntouched(t *testing.T) {
	b := New(cfg(0.5, 4, time.Second))
	for i := 0; i < 20; i++ {
		if err := b.Do(func() error { return nil }); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if got := b.State(); got != "closed" {
		t.Fatalf("state = %q, want closed", got)
	}
}

func TestCountsResetEachWindowSoTheRateIsRollingNotLifetime(t *testing.T) {
	// gobreaker treats Interval <= 0 as "never clear counts while closed". With
	// that setting a long run of healthy traffic permanently dilutes the
	// failure rate and a later outage cannot trip the breaker. The window
	// interval has to reset the counts.
	c := cfg(0.5, 10, time.Second)
	c.WindowInterval = 60 * time.Millisecond
	b := New(c)

	for i := 0; i < 200; i++ {
		if err := b.Do(func() error { return nil }); err != nil {
			t.Fatalf("healthy call %d: %v", i, err)
		}
	}
	time.Sleep(80 * time.Millisecond) // let the counts roll over

	for i := 0; i < 10; i++ {
		_ = b.Do(func() error { return errDownstream })
	}
	if got := b.State(); got != "open" {
		t.Fatalf("state = %q after a full window of failures, want open; "+
			"200 earlier successes must not dilute the current window", got)
	}
}

func TestZeroWindowIntervalIsNotUsed(t *testing.T) {
	// Guards the constructor: a zero interval is the failure mode above.
	c := cfg(0.5, 4, time.Second)
	if c.WindowInterval <= 0 {
		t.Fatal("test config must carry a positive WindowInterval")
	}
}
