package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestAllowDrainsTheBurstThenRefuses(t *testing.T) {
	// 1 token/sec refill means the burst is the only budget available inside
	// the first second, so the count of successful Allow calls is exact.
	l := New(1, 5)
	granted := 0
	for i := 0; i < 20; i++ {
		if l.Allow() {
			granted++
		}
	}
	if granted != 5 {
		t.Fatalf("granted %d of 20 immediate calls, want exactly the burst size 5", granted)
	}
}

func TestAllowRefillsAtTheConfiguredRate(t *testing.T) {
	l := New(100, 1) // 100/sec => one token every 10ms
	if !l.Allow() {
		t.Fatal("first call should consume the initial burst token")
	}
	if l.Allow() {
		t.Fatal("second immediate call should find the bucket empty")
	}
	time.Sleep(40 * time.Millisecond)
	if !l.Allow() {
		t.Fatal("bucket should have refilled after 40ms at 100/sec")
	}
}

func TestWaitBlocksUntilATokenIsAvailable(t *testing.T) {
	l := New(50, 1) // one token every 20ms
	ctx := context.Background()
	if err := l.Wait(ctx); err != nil {
		t.Fatalf("first Wait: %v", err)
	}
	start := time.Now()
	if err := l.Wait(ctx); err != nil {
		t.Fatalf("second Wait: %v", err)
	}
	// Allow generous slack for scheduler jitter; the point is that it blocked
	// rather than returning immediately.
	if elapsed := time.Since(start); elapsed < 10*time.Millisecond {
		t.Fatalf("second Wait returned after %s, want it to block for roughly the 20ms refill interval", elapsed)
	}
}

func TestWaitReturnsWhenTheContextIsCancelled(t *testing.T) {
	l := New(1, 1)
	if err := l.Wait(context.Background()); err != nil {
		t.Fatalf("priming Wait: %v", err)
	}
	// The consumer passes a signal-cancelled context; on shutdown Wait must
	// return instead of holding the worker for a full second.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.Wait(ctx); err == nil {
		t.Fatal("Wait on a cancelled context returned nil; the worker would keep blocking on shutdown")
	}
}

func TestWaitRespectsADeadline(t *testing.T) {
	l := New(1, 1)
	if err := l.Wait(context.Background()); err != nil {
		t.Fatalf("priming Wait: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := l.Wait(ctx); err == nil {
		t.Fatal("Wait should fail when the next token arrives after the deadline")
	}
}
