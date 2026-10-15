package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"

	natsio "github.com/nats-io/nats.go"
)

// MsgEvent describes a message the library retried, gave up on or dead-lettered.
type MsgEvent struct {
	Handler string // as HandlerStatus.Name
	Subject string
	// Attempt is the attempt that just failed: the JetStream delivery count,
	// or the in-process attempt (1-based). It is 0 when the failure is not
	// tied to an attempt (response encoding).
	Attempt int
	Msg     *RawMsg
	Err     error // the handler error (see WithOnDeadLetterFailed)
}

// HandlerEvent describes a handler state change.
type HandlerEvent struct {
	Handler string // as HandlerStatus.Name
	Kind    string // as HandlerStatus.Kind
	Err     error
}

// AsyncErrorEvent describes an error the library could only log.
type AsyncErrorEvent struct {
	Handler string // empty for connection errors
	Subject string
	Err     error // prefixed with the failed operation, e.g. "ack: ..."
}

// events holds the callbacks registered through the WithOn* options.
type events struct {
	retry, failed, deadLettered, deadLetterFailed []func(MsgEvent)
	restarting, restarted, stopped                []func(HandlerEvent)
	disconnected                                  []func(error)
	reconnected, closed                           []func(struct{}) // wrap the user's func()
	asyncError                                    []func(AsyncErrorEvent)
}

// fire calls each fn with e in order. A panic is recovered and logged, and
// the next fn still runs.
func fire[E any](log *slog.Logger, fns []func(E), e E) {
	for _, fn := range fns {
		func() {
			defer func() {
				if p := recover(); p != nil {
					log.Error("nats: event callback panicked", "panic", p, "stack", string(debug.Stack()))
				}
			}()
			fn(e)
		}()
	}
}

// sink is a worker's logger plus the App's event callbacks, bound to one handler.
// Call sites log first and then call the matching method, which only dispatches.
type sink struct {
	*slog.Logger
	ev      *events // nil: no callbacks (an App{} in unit tests)
	handler string
	kind    handlerKind
}

func (a *App) sink(handler string, kind handlerKind) *sink {
	return &sink{Logger: a.opts.logger.With("handler", handler, "kind", kind), ev: &a.opts.events, handler: handler, kind: kind}
}

func (s *sink) msg(m *RawMsg, attempt int, err error) MsgEvent {
	return MsgEvent{Handler: s.handler, Subject: m.Subject, Attempt: attempt, Msg: m, Err: err}
}

func (s *sink) retry(m *RawMsg, attempt int, err error) {
	if s.ev != nil {
		fire(s.Logger, s.ev.retry, s.msg(m, attempt, err))
	}
}

func (s *sink) failed(m *RawMsg, attempt int, err error) {
	if s.ev != nil {
		fire(s.Logger, s.ev.failed, s.msg(m, attempt, err))
	}
}

func (s *sink) deadLettered(m *RawMsg, attempt int, err error) {
	if s.ev != nil {
		fire(s.Logger, s.ev.deadLettered, s.msg(m, attempt, err))
	}
}

func (s *sink) deadLetterFailed(m *RawMsg, attempt int, err, dlqErr error) {
	if s.ev != nil {
		fire(s.Logger, s.ev.deadLetterFailed, s.msg(m, attempt, errors.Join(err, dlqErr)))
	}
}

func (s *sink) handlerEvent(err error) HandlerEvent {
	return HandlerEvent{Handler: s.handler, Kind: string(s.kind), Err: err}
}

func (s *sink) restarting(err error) {
	if s.ev != nil {
		fire(s.Logger, s.ev.restarting, s.handlerEvent(err))
	}
}

func (s *sink) restarted() {
	if s.ev != nil {
		fire(s.Logger, s.ev.restarted, s.handlerEvent(nil))
	}
}

// connCallbacks returns the nats.go options that emit the connection events
// and close(closed). They replace the user's callbacks from WithNATSOptions,
// so each wrapper calls the user's callback first. closed is closed after the
// closed events fired, so Shutdown returns after them.
func (a *App) connCallbacks(closed chan struct{}) []natsio.Option {
	var user natsio.Options
	for _, opt := range a.opts.natsOpts {
		_ = opt(&user) // a failing option makes Connect fail with the same error
	}
	log, ev := a.opts.logger, &a.opts.events
	return []natsio.Option{
		natsio.DisconnectErrHandler(func(nc *natsio.Conn, err error) {
			// nats.go skips DisconnectedCB when DisconnectedErrCB is set.
			if user.DisconnectedErrCB != nil {
				user.DisconnectedErrCB(nc, err)
			} else if user.DisconnectedCB != nil {
				user.DisconnectedCB(nc)
			}
			fire(log, ev.disconnected, err)
		}),
		natsio.ReconnectHandler(func(nc *natsio.Conn) {
			if user.ReconnectedCB != nil {
				user.ReconnectedCB(nc)
			}
			fire(log, ev.reconnected, struct{}{})
		}),
		natsio.ClosedHandler(func(nc *natsio.Conn) {
			defer close(closed)
			if user.ClosedCB != nil {
				user.ClosedCB(nc)
			}
			fire(log, ev.closed, struct{}{})
		}),
		natsio.ErrorHandler(func(nc *natsio.Conn, sub *natsio.Subscription, err error) {
			e := AsyncErrorEvent{Err: fmt.Errorf("conn: %w", err)}
			if sub != nil {
				e.Subject = sub.Subject
			}
			if user.AsyncErrorCB != nil {
				user.AsyncErrorCB(nc, sub, err)
			} else {
				// Our handler replaces nats.go's default, which prints to stderr.
				log.Error("nats: connection error", "subject", e.Subject, "error", err)
			}
			fire(log, ev.asyncError, e)
		}),
	}
}

type sinkKey struct{}

// withSink stores the worker's sink in ctx for middleware to use.
func withSink(ctx context.Context, s *sink) context.Context {
	return context.WithValue(ctx, sinkKey{}, s)
}

// sinkFrom returns the sink stored in ctx, else one logging to slog.Default().
func sinkFrom(ctx context.Context) *sink {
	if s, ok := ctx.Value(sinkKey{}).(*sink); ok {
		return s
	}
	return &sink{Logger: slog.Default()}
}

func addEvent[E any](name string, dst func(*options) *[]func(E), fn func(E)) Option {
	return func(o *options) error {
		if fn == nil {
			return fmt.Errorf("nats: %s: nil callback", name)
		}
		d := dst(o)
		*d = append(*d, fn)
		return nil
	}
}

// WithOnRetry sets a callback run when a failed message will be tried again:
// when a JetStream handler error is nakked for redelivery (it is not terminal
// and attempts are left), or when a core handler is about to retry in process.
// Err is the handler error.
//
// fn runs synchronously on the goroutine where the event happens and must not
// block; a panic in fn is recovered and logged.
func WithOnRetry(fn func(MsgEvent)) Option {
	return addEvent("WithOnRetry", func(o *options) *[]func(MsgEvent) { return &o.events.retry }, fn)
}

// WithOnFailed sets a callback run when the library gives up on a message
// without a dead-letter queue: a JetStream message is terminated, a core
// handler gives up (this includes every failed core handler with the default
// single attempt), or a request handler fails to encode its response. Err is
// the handler or encode error. Messages handled with WithManualAck emit no
// message event.
//
// fn runs synchronously on the goroutine where the event happens and must not
// block; a panic in fn is recovered and logged.
func WithOnFailed(fn func(MsgEvent)) Option {
	return addEvent("WithOnFailed", func(o *options) *[]func(MsgEvent) { return &o.events.failed }, fn)
}

// WithOnDeadLettered sets a callback run after a failed message was published
// to its dead-letter queue. Err is the handler error.
//
// fn runs synchronously on the goroutine where the event happens and must not
// block; a panic in fn is recovered and logged.
func WithOnDeadLettered(fn func(MsgEvent)) Option {
	return addEvent("WithOnDeadLettered", func(o *options) *[]func(MsgEvent) { return &o.events.deadLettered }, fn)
}

// WithOnDeadLetterFailed sets a callback run when publishing a message to its
// dead-letter queue failed (a JetStream message is nakked for retry, a core
// message is dropped). Err joins the handler error and the dead-letter error.
//
// fn runs synchronously on the goroutine where the event happens and must not
// block; a panic in fn is recovered and logged.
func WithOnDeadLetterFailed(fn func(MsgEvent)) Option {
	return addEvent("WithOnDeadLetterFailed", func(o *options) *[]func(MsgEvent) { return &o.events.deadLetterFailed }, fn)
}

// WithOnHandlerRestarting sets a callback run when the consumer of a Consume
// or ConsumeBatch handler is gone and the supervisor starts restarting it.
// Err is why.
//
// fn runs synchronously on the goroutine where the event happens and must not
// block; a panic in fn is recovered and logged.
func WithOnHandlerRestarting(fn func(HandlerEvent)) Option {
	return addEvent("WithOnHandlerRestarting", func(o *options) *[]func(HandlerEvent) { return &o.events.restarting }, fn)
}

// WithOnHandlerRestarted sets a callback run when a restarted handler is
// running again. Err is nil.
//
// fn runs synchronously on the goroutine where the event happens and must not
// block; a panic in fn is recovered and logged.
func WithOnHandlerRestarted(fn func(HandlerEvent)) Option {
	return addEvent("WithOnHandlerRestarted", func(o *options) *[]func(HandlerEvent) { return &o.events.restarted }, fn)
}

// WithOnHandlerStopped sets a callback run after each handler of any kind has
// stopped, on Shutdown and on a failed Start. Err is the error stopping
// returned, for example the shutdown deadline.
//
// fn runs synchronously on the goroutine where the event happens and must not
// block; a panic in fn is recovered and logged.
func WithOnHandlerStopped(fn func(HandlerEvent)) Option {
	return addEvent("WithOnHandlerStopped", func(o *options) *[]func(HandlerEvent) { return &o.events.stopped }, fn)
}

// WithOnDisconnected sets a callback run when the connection is lost,
// including the disconnect caused by Shutdown. err may be nil.
//
// fn runs synchronously on the goroutine where the event happens and must not
// block; a panic in fn is recovered and logged.
func WithOnDisconnected(fn func(err error)) Option {
	return addEvent("WithOnDisconnected", func(o *options) *[]func(error) { return &o.events.disconnected }, fn)
}

// WithOnReconnected sets a callback run when the connection is re-established.
//
// fn runs synchronously on the goroutine where the event happens and must not
// block; a panic in fn is recovered and logged.
func WithOnReconnected(fn func()) Option {
	return func(o *options) error {
		if fn == nil {
			return errors.New("nats: WithOnReconnected: nil callback")
		}
		o.events.reconnected = append(o.events.reconnected, func(struct{}) { fn() })
		return nil
	}
}

// WithOnClosed sets a callback run when the connection is closed for good,
// including on Shutdown and on a failed Start after the connection was made.
//
// fn runs synchronously on the goroutine where the event happens and must not
// block; a panic in fn is recovered and logged.
func WithOnClosed(fn func()) Option {
	return func(o *options) error {
		if fn == nil {
			return errors.New("nats: WithOnClosed: nil callback")
		}
		o.events.closed = append(o.events.closed, func(struct{}) { fn() })
		return nil
	}
}

// WithOnAsyncError sets a callback run for an error the library could only
// log: a failed ack, nak, term, in-progress or response, a consume, fetch or
// restart failure, an idempotency claim that could not be completed or
// aborted, or a connection error reported by nats.go. Err is prefixed with
// the failed operation, for example "ack: ...", and wraps the original error.
//
// fn runs synchronously on the goroutine where the event happens and must not
// block; a panic in fn is recovered and logged.
func WithOnAsyncError(fn func(AsyncErrorEvent)) Option {
	return addEvent("WithOnAsyncError", func(o *options) *[]func(AsyncErrorEvent) { return &o.events.asyncError }, fn)
}
