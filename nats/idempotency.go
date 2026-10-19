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
//
// A key is the handler's scope, a NUL byte and the message key, so it can hold
// any character, including "\x00", "/", "@" and subject wildcards. A store
// whose backend restricts keys must hash or escape them, as KVStore does.
type IdempotencyStore interface {
	Begin(ctx context.Context, key string, ttl time.Duration) error
	Complete(ctx context.Context, key string, ttl time.Duration) error
	Abort(ctx context.Context, key string) error
}

// idemConfig is a set of idempotency settings. A zero field is unset: over
// fills it from a lower level.
type idemConfig struct {
	store    IdempotencyStore
	key      func(*RawMsg) string
	claimTTL time.Duration
	doneTTL  time.Duration
	// err is the first invalid option value.
	err error
}

// idemDefaults are the library defaults, below App.Idempotency and
// WithIdempotency.
var idemDefaults = idemConfig{
	key:      func(m *RawMsg) string { return m.Header.Get(jetstream.MsgIDHeader) },
	claimTTL: time.Minute,
	doneTTL:  24 * time.Hour,
}

// over returns c with its unset fields taken from base.
func (c idemConfig) over(base idemConfig) idemConfig {
	if c.store == nil {
		c.store = base.store
	}
	if c.key == nil {
		c.key = base.key
	}
	if c.claimTTL == 0 {
		c.claimTTL = base.claimTTL
	}
	if c.doneTTL == 0 {
		c.doneTTL = base.doneTTL
	}
	return c
}

func (c *idemConfig) fail(err error) {
	if c.err == nil {
		c.err = err
	}
}

func (c *idemConfig) setTTL(dst *time.Duration, d time.Duration) {
	if d <= 0 {
		c.fail(fmt.Errorf("non-positive TTL %s", d))
		return
	}
	*dst = d
}

// IdempotencyOption configures App.Idempotency and WithIdempotency.
type IdempotencyOption func(*idemConfig)

// WithIdempotencyStore sets the store that records processed keys. It
// overrides the store given to App.Idempotency.
func WithIdempotencyStore(s IdempotencyStore) IdempotencyOption {
	return func(c *idemConfig) {
		if s == nil {
			c.fail(errors.New("nil store"))
			return
		}
		c.store = s
	}
}

// WithIdempotencyKey derives the dedup key from a message. The default is the
// Nats-Msg-Id header. An empty key disables dedup for that message.
func WithIdempotencyKey(fn func(*RawMsg) string) IdempotencyOption {
	return func(c *idemConfig) {
		if fn == nil {
			c.fail(errors.New("nil key function"))
			return
		}
		c.key = fn
	}
}

// WithClaimTTL sets how long an in-flight claim lasts (default 1 minute). It must
// exceed the worst-case handler duration: claims are released by key, without
// an ownership token, so a handler that outlives its claim can have its
// duplicate admitted and its claim released by the other delivery.
func WithClaimTTL(d time.Duration) IdempotencyOption {
	return func(c *idemConfig) { c.setTTL(&c.claimTTL, d) }
}

// WithDoneTTL sets how long a completed key is remembered (default 24 hours).
func WithDoneTTL(d time.Duration) IdempotencyOption {
	return func(c *idemConfig) { c.setTTL(&c.doneTTL, d) }
}

// Idempotency sets the App's idempotency defaults: store, and opts applied
// after it. It does not enable idempotency on any handler; WithIdempotency
// does, and its options override these field by field. It may be called
// before or after the handlers are registered, but only once.
func (a *App) Idempotency(store IdempotencyStore, opts ...IdempotencyOption) {
	c := &idemConfig{store: store}
	for _, o := range opts {
		o(c)
	}
	if c.store == nil {
		c.fail(errors.New("nil store"))
	}
	if c.err != nil {
		a.addErr(fmt.Errorf("nats: App.Idempotency: %w", c.err))
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lateLocked() {
		return
	}
	if a.idem != nil {
		a.regErrs = append(a.regErrs, errors.New("nats: App.Idempotency called twice"))
		return
	}
	a.idem = c
}

// WithIdempotency makes the handler skip messages whose key it already
// processed successfully. opts override the settings of App.Idempotency
// field by field, which override the defaults (key Nats-Msg-Id, claim TTL 1
// minute, done TTL 24 hours). Start fails if no store is set at either level.
// Not allowed on ConsumeBatch.
//
// Keys are scoped per handler, so handlers that receive the same message each
// process it once, and the replicas of one handler share their keys:
//
//	Consume                                STREAM/consumer
//	Endpoint                               service.group.endpoint, none with
//	                                       micro.Config.QueueGroupDisabled
//	Subscribe, SubscribeReply + WithQueue  subject@queue
//	Subscribe, SubscribeReply, no queue    none: WithIdempotencyScope required
//	ConsumeOrdered                         none: WithIdempotencyScope required
//
// The key is claimed before the handler runs, for WithClaimTTL, and
// remembered for WithDoneTTL once the handler succeeds. A failing or
// panicking handler releases the claim so the retry can run. A duplicate
// returns Terminal(ErrDuplicate) without calling the handler, inside every
// middleware: a duplicate JetStream message is acked (with WithManualAck too),
// a duplicate request gets a 400 response, not the original response, and a
// duplicate is never retried, dead-lettered or logged as a failure.
//
// A key claimed by another delivery fails with ErrInProgress, which is
// retried like any other error. On JetStream every such nak is a delivery
// that counts toward WithMaxAttempts: with a limit and a DLQ, a copy whose
// twin is still being handled after all its retries is dead-lettered, even
// though the twin then succeeds. Keep handlers within AckWait (see
// WithAutoInProgress) and leave enough attempts and backoff for the
// slowest handler. The other handlers make one attempt by default, so a copy
// that arrives while its twin runs (a fan-out Subscribe or ConsumeOrdered on
// several replicas sharing a scope) is reported as failed, and dead-lettered
// if WithDLQ is set, unless WithMaxAttempts and WithBackoff leave its twin
// time to finish.
//
// This is not exactly-once: a retry after a failure runs the side effects of
// the failed attempt again, a key whose Complete fails is only logged and
// reported (a later duplicate is admitted), and a duplicate arriving after
// WithDoneTTL is processed again.
func WithIdempotency(opts ...IdempotencyOption) HandlerOption {
	return func(c *handlerConfig) {
		ic := &idemConfig{}
		for _, o := range opts {
			o(ic)
		}
		c.idem = ic
		c.used = append(c.used, "WithIdempotency")
	}
}

// WithIdempotencyScope sets the scope that keeps this handler's keys apart
// from other handlers' keys, replacing the default scope (see
// WithIdempotency). Handlers with the same scope share their keys, whether
// the scope is explicit or a default.
func WithIdempotencyScope(scope string) HandlerOption {
	return func(c *handlerConfig) { c.idemScope = scope; c.used = append(c.used, "WithIdempotencyScope") }
}

// idemScope resolves the scope of handler name, using def unless
// WithIdempotencyScope set one, and records a handler that relies on the
// App's store.
func (a *App) idemScope(c *handlerConfig, kind handlerKind, name, def string) error {
	if c.idem == nil {
		return nil
	}
	if c.idemScope == "" {
		c.idemScope = def
	}
	if c.idemScope == "" {
		return fmt.Errorf("nats: %s %q: WithIdempotency needs WithIdempotencyScope: no default scope", kind, name)
	}
	if c.idem.store == nil {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.idemNoStore = append(a.idemNoStore, fmt.Sprintf("%s %q", kind, name))
	}
	return nil
}

// idempotent claims scope + "\x00" + the message key in c.store around
// next. c is resolved and valid (see WithIdempotency).
func idempotent(c idemConfig, scope string) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, m *RawMsg) error {
			key := c.key(m)
			if key == "" {
				return next(ctx, m)
			}
			key = scope + "\x00" + key
			if err := c.store.Begin(ctx, key, c.claimTTL); err != nil {
				if errors.Is(err, ErrDuplicate) {
					return Terminal(ErrDuplicate)
				}
				return err
			}
			// abort releases the claim even if ctx is already cancelled, or the
			// retry sees ErrInProgress.
			abort := func() {
				if aerr := c.store.Abort(context.WithoutCancel(ctx), key); aerr != nil {
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
			if err := c.store.Complete(context.WithoutCancel(ctx), key, c.doneTTL); err != nil {
				log := sinkFrom(ctx)
				log.ErrorContext(ctx, "nats: idempotency complete failed", "key", key, "error", err)
				log.asyncErr(m.Subject, fmt.Errorf("idempotency complete: %w", err))
			}
			return nil
		}
	}
}
