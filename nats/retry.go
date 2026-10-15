package nats

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Backoff returns the delay before the given retry attempt (1-based).
type Backoff func(attempt int) time.Duration

// Unlimited as WithMaxAttempts retries a message until it succeeds.
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

// dispose acks, naks or terms a consumed message according to the handler
// result, publishing it to the DLQ when giving up on it. A duplicate skipped
// by Idempotent is acked. A term carries the first line of err as its
// reason, which the server reports in the MSG_TERMINATED advisory. Ack failures are only logged: the server
// redelivers the message after AckWait.
func (a *App) dispose(ctx context.Context, m jetstream.Msg, raw *RawMsg, err error, rc retryConfig, src dlqSource, log *sink) {
	attempt := src.deliveries
	var ackErr error
	switch {
	case err == nil:
		ackErr = m.Ack()
	case errors.Is(err, ErrDuplicate):
		log.Debug("nats: duplicate message acked", "subject", raw.Subject)
		ackErr = m.Ack()
	case !IsTerminal(err) && !rc.exhausted(attempt):
		log.Error("nats: handler failed, will retry", "subject", raw.Subject, "attempt", attempt, "error", err)
		log.retry(raw, attempt, err)
		ackErr = m.NakWithDelay(rc.backoff(attempt))
	case rc.dlq == "":
		log.Error("nats: handler failed, message terminated", "subject", raw.Subject, "attempt", attempt, "error", err)
		log.failed(raw, attempt, err)
		ackErr = m.TermWithReason(firstLine(err))
	default:
		if perr := a.publishDLQ(ctx, rc.dlq, raw, err, src); perr != nil {
			log.Error("nats: handler failed, DLQ publish failed, will retry", "subject", raw.Subject, "attempt", attempt, "error", err, "dlq_error", perr)
			log.deadLetterFailed(raw, attempt, err, perr)
			ackErr = m.NakWithDelay(rc.backoff(attempt))
		} else {
			log.Error("nats: handler failed, message sent to DLQ", "subject", raw.Subject, "attempt", attempt, "error", err, "dlq", rc.dlq)
			log.deadLettered(raw, attempt, err)
			ackErr = m.TermWithReason(firstLine(err))
		}
	}
	if ackErr != nil {
		log.Error("nats: ack failed", "subject", raw.Subject, "error", ackErr)
	}
}

// retryLocal runs h on m until it succeeds, fails with a Terminal error or
// uses up rc's attempts, waiting rc.backoff between attempts. It returns the
// last error and the number of attempts made. A wait cut short by stopping
// abandons the message.
func (a *App) retryLocal(ctx context.Context, h Handler, m *RawMsg, rc retryConfig, stopping <-chan struct{}, log *sink) (attempts int, abandoned bool, err error) {
	for attempt := 1; ; attempt++ {
		err = safeCall(withAttempt(ctx, attempt), h, m, log.Logger)
		if err == nil || IsTerminal(err) || rc.exhausted(attempt) {
			return attempt, false, err
		}
		log.Error("nats: handler failed, will retry", "subject", m.Subject, "attempt", attempt, "error", err)
		log.retry(m, attempt, err)
		select {
		case <-time.After(rc.backoff(attempt)):
		case <-stopping:
			log.Warn("nats: retry abandoned, shutting down", "subject", m.Subject)
			return attempt, true, err
		}
	}
}

// handleLocal runs h with in-process retries and, when the message finally
// fails, logs it and publishes it to the DLQ if one is set. A duplicate
// skipped by Idempotent is neither. It returns the final error and whether
// the message was abandoned.
func (a *App) handleLocal(ctx context.Context, h Handler, m *RawMsg, rc retryConfig, stopping <-chan struct{}, src dlqSource, log *sink) (abandoned bool, err error) {
	attempts, abandoned, err := a.retryLocal(ctx, h, m, rc, stopping, log)
	if err == nil || abandoned {
		return abandoned, err
	}
	if errors.Is(err, ErrDuplicate) {
		log.Debug("nats: duplicate message skipped", "subject", m.Subject)
		return false, err
	}
	if rc.dlq == "" {
		log.Error("nats: handler failed", "subject", m.Subject, "attempt", attempts, "error", err)
		log.failed(m, attempts, err)
		return false, err
	}
	src.deliveries = attempts
	if perr := a.publishDLQ(ctx, rc.dlq, m, err, src); perr != nil {
		log.Error("nats: handler failed, DLQ publish failed, message dropped", "subject", m.Subject, "attempt", attempts, "error", err, "dlq_error", perr)
		log.deadLetterFailed(m, attempts, err, perr)
	} else {
		log.Error("nats: handler failed, message sent to DLQ", "subject", m.Subject, "attempt", attempts, "error", err, "dlq", rc.dlq)
		log.deadLettered(m, attempts, err)
	}
	return false, err
}
