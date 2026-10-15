package nats

import (
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
)

// Event callbacks observe what the library otherwise only logs. Each WithOn*
// option may be passed several times; callbacks of the same kind run in the
// order given, and a nil function makes New return an error. Callbacks run
// synchronously on the goroutine where the event happens (a handler worker,
// the supervisor, a nats.go callback goroutine), after the library has logged
// the event, so they must return quickly and must not block. A panic in a
// callback is recovered and logged, and the remaining callbacks still run.

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
func WithOnRetry(fn func(MsgEvent)) Option {
	return addEvent("WithOnRetry", func(o *options) *[]func(MsgEvent) { return &o.events.retry }, fn)
}

// WithOnFailed sets a callback run when the library gives up on a message
// without a dead-letter queue: a JetStream message is terminated, a core
// handler gives up (this includes every failed core handler with the default
// single attempt), or a request handler fails to encode its response. Err is
// the handler or encode error. Messages handled with WithManualAck emit no
// message event.
func WithOnFailed(fn func(MsgEvent)) Option {
	return addEvent("WithOnFailed", func(o *options) *[]func(MsgEvent) { return &o.events.failed }, fn)
}

// WithOnDeadLettered sets a callback run after a failed message was published
// to its dead-letter queue. Err is the handler error.
func WithOnDeadLettered(fn func(MsgEvent)) Option {
	return addEvent("WithOnDeadLettered", func(o *options) *[]func(MsgEvent) { return &o.events.deadLettered }, fn)
}

// WithOnDeadLetterFailed sets a callback run when publishing a message to its
// dead-letter queue failed (a JetStream message is nakked for retry, a core
// message is dropped). Err joins the handler error and the dead-letter error.
func WithOnDeadLetterFailed(fn func(MsgEvent)) Option {
	return addEvent("WithOnDeadLetterFailed", func(o *options) *[]func(MsgEvent) { return &o.events.deadLetterFailed }, fn)
}

// WithOnHandlerRestarting sets a callback run when the consumer of a Consume
// or ConsumeBatch handler is gone and the supervisor starts restarting it.
// Err is why.
func WithOnHandlerRestarting(fn func(HandlerEvent)) Option {
	return addEvent("WithOnHandlerRestarting", func(o *options) *[]func(HandlerEvent) { return &o.events.restarting }, fn)
}

// WithOnHandlerRestarted sets a callback run when a restarted handler is
// running again. Err is nil.
func WithOnHandlerRestarted(fn func(HandlerEvent)) Option {
	return addEvent("WithOnHandlerRestarted", func(o *options) *[]func(HandlerEvent) { return &o.events.restarted }, fn)
}

// WithOnHandlerStopped sets a callback run after each handler of any kind has
// stopped, on Shutdown and on a failed Start. Err is the error stopping
// returned, for example the shutdown deadline.
func WithOnHandlerStopped(fn func(HandlerEvent)) Option {
	return addEvent("WithOnHandlerStopped", func(o *options) *[]func(HandlerEvent) { return &o.events.stopped }, fn)
}

// WithOnDisconnected sets a callback run when the connection is lost,
// including the disconnect caused by Shutdown. err may be nil.
func WithOnDisconnected(fn func(err error)) Option {
	return addEvent("WithOnDisconnected", func(o *options) *[]func(error) { return &o.events.disconnected }, fn)
}

// WithOnReconnected sets a callback run when the connection is re-established.
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
// including on Shutdown and on a failed Start.
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
func WithOnAsyncError(fn func(AsyncErrorEvent)) Option {
	return addEvent("WithOnAsyncError", func(o *options) *[]func(AsyncErrorEvent) { return &o.events.asyncError }, fn)
}
