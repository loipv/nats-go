package nats

import (
	"math/rand/v2"
	"time"
)

// Backoff returns the delay before the given retry attempt (1-based).
type Backoff func(attempt int) time.Duration

// Unlimited as MaxAttempts retries a message until it succeeds.
const Unlimited = -1

// ExponentialBackoff doubles the delay from min on every attempt up to max,
// with +-20% jitter. It panics if min <= 0 or max < min.
func ExponentialBackoff(min, max time.Duration) Backoff {
	if min <= 0 || max < min {
		panic("nats: ExponentialBackoff requires 0 < min <= max")
	}
	return func(attempt int) time.Duration {
		d := min
		for i := 1; i < attempt && d < max; i++ {
			if d > max/2 {
				d = max
			} else {
				d *= 2
			}
		}
		d = time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
		if d > max {
			d = max
		}
		return d
	}
}

func defaultBackoff() Backoff { return ExponentialBackoff(time.Second, time.Minute) }

type retryConfig struct {
	maxAttempts int
	backoff     Backoff
	dlq         string
}

// retry resolves the retry settings of a handler of the given kind. Durable
// consumers retry forever by default, other handlers run once.
func (c handlerConfig) retry(kind handlerKind) retryConfig {
	r := retryConfig{maxAttempts: c.maxAttempts, backoff: c.backoff, dlq: c.dlq}
	if r.maxAttempts == 0 {
		r.maxAttempts = 1
		if kind == kindConsume || kind == kindBatch {
			r.maxAttempts = Unlimited
		}
	}
	if r.backoff == nil {
		r.backoff = defaultBackoff()
	}
	return r
}

// exhausted reports whether attempt was the last one allowed.
func (r retryConfig) exhausted(attempt int) bool {
	return r.maxAttempts != Unlimited && attempt >= r.maxAttempts
}

// exceeded reports whether attempt is past the allowed attempts.
func (r retryConfig) exceeded(attempt int) bool {
	return r.maxAttempts != Unlimited && attempt > r.maxAttempts
}
