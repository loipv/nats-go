package nats

import (
	"context"
	"errors"
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

// Recover turns a panic in the handler into an error carrying the stack. The
// stack stays in the error for logs; a requester of Reply or an endpoint only
// receives "internal error".
func Recover() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, m *RawMsg) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = &panicError{v: r, stack: string(debug.Stack())}
				}
			}()
			return next(ctx, m)
		}
	}
}

// Logger logs each handled message at Debug and each failure at Error, except
// a duplicate (ErrDuplicate), which is logged at Debug.
func Logger(l *slog.Logger) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, m *RawMsg) error {
			begin := time.Now()
			err := next(ctx, m)
			if errors.Is(err, ErrDuplicate) {
				l.DebugContext(ctx, "nats: duplicate skipped", "subject", m.Subject)
			} else if err != nil {
				l.ErrorContext(ctx, "nats: handler failed", "subject", m.Subject, "error", err)
			} else {
				l.DebugContext(ctx, "nats: handled", "subject", m.Subject, "duration", time.Since(begin))
			}
			return err
		}
	}
}
