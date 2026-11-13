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
// stack stays in the error for logs; a requester of SubscribeReply or an endpoint only
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

// Logger is an access log: one Debug line per message, "nats: handled", with
// the subject, the duration and, when the handler failed, the error. It logs
// through the handler's context, so a slog.Handler can add a trace ID. The App
// already logs every failure at Error, so Logger does not. A skipped duplicate
// (ErrDuplicate) gets "nats: duplicate skipped" instead.
func Logger(l *slog.Logger) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, m *RawMsg) error {
			begin := time.Now()
			err := next(ctx, m)
			switch {
			case errors.Is(err, ErrDuplicate):
				l.DebugContext(ctx, "nats: duplicate skipped", "subject", m.Subject)
			case err != nil:
				l.DebugContext(ctx, "nats: handled", "subject", m.Subject, "duration", time.Since(begin), "error", err)
			default:
				l.DebugContext(ctx, "nats: handled", "subject", m.Subject, "duration", time.Since(begin))
			}
			return err
		}
	}
}
