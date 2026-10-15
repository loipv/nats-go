package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// IdempotencyStore records which message keys were processed. Begin claims a
// key for ttl and returns ErrDuplicate if the key is already completed or
// ErrInProgress if it is claimed by another delivery; any other error is a
// store failure. Complete marks the key done for ttl; Abort releases a claim.
type IdempotencyStore interface {
	Begin(ctx context.Context, key string, ttl time.Duration) error
	Complete(ctx context.Context, key string, ttl time.Duration) error
	Abort(ctx context.Context, key string) error
}

type idemConfig struct {
	key      func(*RawMsg) string
	claimTTL time.Duration
	doneTTL  time.Duration
}

// IdempotencyOption configures Idempotent.
type IdempotencyOption func(*idemConfig)

// WithIdempotencyKey derives the dedup key from a message. The default is the
// Nats-Msg-Id header. An empty key disables dedup for that message.
func WithIdempotencyKey(fn func(*RawMsg) string) IdempotencyOption {
	return func(c *idemConfig) { c.key = fn }
}

// WithClaimTTL sets how long an in-flight claim lasts (default 1 minute). It must
// exceed the worst-case handler duration: claims are released by key, without
// an ownership token, so a handler that outlives its claim can have its
// duplicate admitted and its claim released by the other delivery.
func WithClaimTTL(d time.Duration) IdempotencyOption { return func(c *idemConfig) { c.claimTTL = d } }

// WithDoneTTL sets how long a completed key is remembered (default 24 hours).
func WithDoneTTL(d time.Duration) IdempotencyOption { return func(c *idemConfig) { c.doneTTL = d } }

// Idempotent skips messages whose key was already processed successfully.
// It returns Terminal(ErrDuplicate) for a duplicate without calling the
// handler. Workers recognise it: a duplicate JetStream message is acked (with
// WithManualAck too), a duplicate request gets a 400 response, and a duplicate is
// never retried, dead-lettered or logged as a failure.
// It panics if store is nil or a TTL is not positive.
func Idempotent(store IdempotencyStore, opts ...IdempotencyOption) Middleware {
	c := idemConfig{
		key:      func(m *RawMsg) string { return m.Header.Get(jetstream.MsgIDHeader) },
		claimTTL: time.Minute,
		doneTTL:  24 * time.Hour,
	}
	for _, o := range opts {
		o(&c)
	}
	if store == nil || c.key == nil || c.claimTTL <= 0 || c.doneTTL <= 0 {
		panic("nats: Idempotent needs a store, a key function and positive TTLs")
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, m *RawMsg) error {
			key := c.key(m)
			if key == "" {
				return next(ctx, m)
			}
			if err := store.Begin(ctx, key, c.claimTTL); err != nil {
				if errors.Is(err, ErrDuplicate) {
					return Terminal(ErrDuplicate)
				}
				return err
			}
			// abort releases the claim even if ctx is already cancelled, or the retry sees ErrInProgress.
			abort := func() {
				if aerr := store.Abort(context.WithoutCancel(ctx), key); aerr != nil {
					log := sinkFrom(ctx)
					log.ErrorContext(ctx, "nats: idempotency abort failed", "key", key, "error", aerr)
					log.asyncErr(m.Subject, fmt.Errorf("idempotency abort: %w", aerr))
				}
			}
			defer func() {
				// A panic is a failure too: release the claim, then let it propagate to Recover.
				if r := recover(); r != nil {
					abort()
					panic(r)
				}
			}()
			err := next(ctx, m)
			if err != nil {
				abort()
				return err
			}
			if err := store.Complete(context.WithoutCancel(ctx), key, c.doneTTL); err != nil {
				log := sinkFrom(ctx)
				log.ErrorContext(ctx, "nats: idempotency complete failed", "key", key, "error", err)
				log.asyncErr(m.Subject, fmt.Errorf("idempotency complete: %w", err))
			}
			return nil
		}
	}
}
