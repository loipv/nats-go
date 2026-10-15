package nats

import (
	"context"
	"errors"
	"log/slog"
	"time"
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

// IdempotencyKey derives the dedup key from a message. The default is the
// Nats-Msg-Id header. An empty key disables dedup for that message.
func IdempotencyKey(fn func(*RawMsg) string) IdempotencyOption {
	return func(c *idemConfig) { c.key = fn }
}

// ClaimTTL sets how long an in-flight claim lasts (default 1 minute).
func ClaimTTL(d time.Duration) IdempotencyOption { return func(c *idemConfig) { c.claimTTL = d } }

// DoneTTL sets how long a completed key is remembered (default 24 hours).
func DoneTTL(d time.Duration) IdempotencyOption { return func(c *idemConfig) { c.doneTTL = d } }

// Idempotent skips messages whose key was already processed successfully.
// A duplicate is acked (nil) for JetStream and subscribe deliveries, but a
// duplicate core request fails with Terminal(ErrDuplicate) so the requester
// gets a 400 instead of waiting for a response that never comes.
// It panics if store is nil or a TTL is not positive.
func Idempotent(store IdempotencyStore, opts ...IdempotencyOption) Middleware {
	c := idemConfig{
		key:      func(m *RawMsg) string { return m.Header.Get("Nats-Msg-Id") },
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
					if m.Reply != "" && jsMsgFrom(ctx) == nil {
						return Terminal(ErrDuplicate)
					}
					return nil
				}
				return err
			}
			if err := next(ctx, m); err != nil {
				// The claim must go even if ctx is already cancelled, or the retry sees ErrInProgress.
				if aerr := store.Abort(context.WithoutCancel(ctx), key); aerr != nil {
					slog.ErrorContext(ctx, "nats: idempotency abort failed", "key", key, "error", aerr)
				}
				return err
			}
			if err := store.Complete(context.WithoutCancel(ctx), key, c.doneTTL); err != nil {
				slog.ErrorContext(ctx, "nats: idempotency complete failed", "key", key, "error", err)
			}
			return nil
		}
	}
}
