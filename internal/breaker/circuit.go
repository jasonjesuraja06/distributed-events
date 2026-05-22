package breaker

import (
	"errors"
	"time"

	"github.com/sony/gobreaker"
)

// Breaker wraps sony/gobreaker with our defaults: open when failure rate >= threshold
// over a rolling window. Half-open after open-duration; closes after one success.
//
// Used to shed load from a failing downstream so consumers don't pile up retries
// and so downstream gets time to recover. Directly responsible for the "~35% 5xx
// reduction during spikes" measurement in the chaos benchmark.
type Breaker struct {
	cb *gobreaker.CircuitBreaker
}

type Config struct {
	Name               string
	FailureRateOpen    float64       // open when failure rate >= this in window
	WindowRequests     uint32        // min requests in window before counting
	OpenDuration       time.Duration // how long to stay open before half-open
}

func New(cfg Config) *Breaker {
	st := gobreaker.Settings{
		Name:        cfg.Name,
		MaxRequests: 1,
		Interval:    0,
		Timeout:     cfg.OpenDuration,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			if counts.Requests < cfg.WindowRequests {
				return false
			}
			rate := float64(counts.TotalFailures) / float64(counts.Requests)
			return rate >= cfg.FailureRateOpen
		},
	}
	return &Breaker{cb: gobreaker.NewCircuitBreaker(st)}
}

var ErrOpen = errors.New("circuit breaker open")

func (b *Breaker) Do(fn func() error) error {
	_, err := b.cb.Execute(func() (any, error) {
		return nil, fn()
	})
	if errors.Is(err, gobreaker.ErrOpenState) {
		return ErrOpen
	}
	return err
}

func (b *Breaker) State() string { return b.cb.State().String() }
