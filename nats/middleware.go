package nats

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"
)

// Handler processes one raw message. Middleware wraps Handlers.
type Handler func(ctx context.Context, m *RawMsg) error

// Middleware wraps a Handler on the inbound path.
type Middleware func(Handler) Handler

// PublishFunc sends one message. PublishMiddleware wraps PublishFuncs.
type PublishFunc func(ctx context.Context, m *RawMsg) error

// PublishMiddleware wraps a PublishFunc on the outbound path.
type PublishMiddleware func(PublishFunc) PublishFunc

// Recover turns a panic in the handler into an error carrying the stack.
func Recover() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, m *RawMsg) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("nats: panic: %v\n%s", r, debug.Stack())
				}
			}()
			return next(ctx, m)
		}
	}
}

// Logger logs each handled message at Debug and each failure at Error.
func Logger(l *slog.Logger) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, m *RawMsg) error {
			begin := time.Now()
			err := next(ctx, m)
			if err != nil {
				l.ErrorContext(ctx, "nats: handler failed", "subject", m.Subject, "error", err)
			} else {
				l.DebugContext(ctx, "nats: handled", "subject", m.Subject, "duration", time.Since(begin))
			}
			return err
		}
	}
}
