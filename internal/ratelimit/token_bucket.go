package ratelimit

import (
	"context"

	"golang.org/x/time/rate"
)

// Limiter caps outbound downstream RPS to protect the downstream from overload
// during traffic spikes. Burst is sized to absorb short bursts without dropping.
type Limiter struct {
	r *rate.Limiter
}

func New(rps int, burst int) *Limiter {
	return &Limiter{r: rate.NewLimiter(rate.Limit(rps), burst)}
}

// Wait blocks until a token is available or ctx is done.
func (l *Limiter) Wait(ctx context.Context) error {
	return l.r.Wait(ctx)
}

// Allow is non-blocking; returns false if no token is available.
func (l *Limiter) Allow() bool {
	return l.r.Allow()
}
