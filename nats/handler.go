package nats

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

// MsgHandler is a typed message handler for App.Subscribe, App.Consume and
// App.ConsumeOrdered. Create it with Typed; the zero value is rejected by
// Start.
type MsgHandler struct{ bind func(Codec) Handler }

// Typed wraps fn for App.Subscribe, App.Consume and App.ConsumeOrdered. The
// payload is decoded into T with the app codec (T = []byte skips decoding)
// and validated when T has a Validate() error method.
func Typed[T any](fn func(context.Context, *Msg[T]) error) MsgHandler {
	if fn == nil {
		return MsgHandler{}
	}
	return MsgHandler{bind: func(c Codec) Handler { return typed(c, fn) }}
}

// ReplyHandler is a typed responder for App.SubscribeReply and service endpoints.
// Create it with TypedReply; the zero value is rejected by Start.
type ReplyHandler struct{ bind func(Codec) Handler }

// TypedReply wraps fn for App.SubscribeReply and service endpoints. The request is
// decoded into Req and the returned Resp is encoded with the app codec.
func TypedReply[Req, Resp any](fn func(context.Context, *Msg[Req]) (Resp, error)) ReplyHandler {
	if fn == nil {
		return ReplyHandler{}
	}
	return ReplyHandler{bind: func(c Codec) Handler { return replyHandler(c, fn) }}
}

// BatchHandler is a typed batch handler for App.ConsumeBatch. Create it with
// TypedBatch; the zero value is rejected by Start.
type BatchHandler struct {
	reg func(jsReg, Codec) registration
}

// TypedBatch wraps fn for App.ConsumeBatch. Every message is decoded into T
// with the app codec before fn is called.
func TypedBatch[T any](fn func(context.Context, []*Msg[T]) error) BatchHandler {
	if fn == nil {
		return BatchHandler{}
	}
	return BatchHandler{reg: func(j jsReg, c Codec) registration {
		return &batchReg[T]{jsReg: j, codec: c, fn: fn}
	}}
}

// typed adapts a typed handler to a raw Handler: decode, validate, call.
func typed[T any](c Codec, fn func(context.Context, *Msg[T]) error) Handler {
	return func(ctx context.Context, raw *RawMsg) error {
		m, err := decodeMsg[T](c, ctx, raw)
		if err != nil {
			return err
		}
		return fn(ctx, m)
	}
}

// chain wraps h with handler middleware, then app middleware, then the
// handler timeout as the outermost layer.
func (a *App) chain(c handlerConfig, h Handler) Handler {
	for _, mw := range slices.Backward(c.mws) {
		h = mw(h)
	}
	for _, mw := range slices.Backward(a.mws) {
		h = mw(h)
	}
	if c.timeout > 0 {
		inner := h
		h = func(ctx context.Context, m *RawMsg) error {
			ctx, cancel := context.WithTimeout(ctx, c.timeout)
			defer cancel()
			return inner(ctx, m)
		}
	}
	return h
}

type jsMsgKey struct{}

func withJSMsg(ctx context.Context, m jetstream.Msg) context.Context {
	return context.WithValue(ctx, jsMsgKey{}, m)
}

func jsMsgFrom(ctx context.Context) jetstream.Msg {
	m, _ := ctx.Value(jsMsgKey{}).(jetstream.Msg)
	return m
}

type attemptKey struct{}

func withAttempt(ctx context.Context, n int) context.Context {
	return context.WithValue(ctx, attemptKey{}, n)
}

// Attempt returns the attempt of the message being handled, counting from 1:
// the retry attempt stored in ctx, else the JetStream delivery count. It is 1
// outside handlers.
func Attempt(ctx context.Context) int { return attemptFrom(ctx) }

// attemptFrom returns the attempt stored in ctx, else the JetStream delivery
// count, else 1.
func attemptFrom(ctx context.Context) int {
	if n, ok := ctx.Value(attemptKey{}).(int); ok {
		return n
	}
	if m := jsMsgFrom(ctx); m != nil {
		if md, err := m.Metadata(); err == nil {
			return int(md.NumDelivered)
		}
	}
	return 1
}

type loggerKey struct{}

// withLogger stores the worker's logger in ctx for middleware to use.
func withLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// loggerFrom returns the logger stored in ctx, else slog.Default().
func loggerFrom(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}

// panicError is the error a recovered handler panic becomes. SubscribeReply and
// endpoint requesters see "internal error" instead of its text, which may
// leak internals.
type panicError struct {
	v     any
	stack string
}

func (e *panicError) Error() string {
	if e.stack == "" {
		return fmt.Sprintf("nats: panic: %v", e.v)
	}
	return fmt.Sprintf("nats: panic: %v\n%s", e.v, e.stack)
}

// safeCall runs h and turns a panic into a *panicError, so a faulty handler
// cannot crash the worker goroutine and the process with it.
func safeCall(ctx context.Context, h Handler, m *RawMsg, log *slog.Logger) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("nats: handler panicked", "subject", m.Subject, "panic", r, "stack", string(debug.Stack()))
			err = &panicError{v: r}
		}
	}()
	return h(ctx, m)
}

func toRaw(m jetstream.Msg) *RawMsg {
	return &RawMsg{Subject: m.Subject(), Reply: m.Reply(), Header: m.Headers(), Data: m.Data()}
}

type job struct {
	ctx context.Context
	msg *RawMsg
}

// pool runs a handler on n worker goroutines. dispatch blocks while all
// workers are busy, which pushes back on the subscription.
type pool struct {
	jobs chan job
	quit chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

func newPool(n int, h func(context.Context, *RawMsg)) *pool {
	p := &pool{jobs: make(chan job), quit: make(chan struct{})}
	for range n {
		p.wg.Go(func() {
			for {
				select {
				case j := <-p.jobs:
					h(j.ctx, j.msg)
				case <-p.quit:
					return
				}
			}
		})
	}
	return p
}

// dispatch hands m to a worker. It returns without delivering once close
// has been called; JetStream redelivers such messages.
func (p *pool) dispatch(ctx context.Context, m *RawMsg) {
	select {
	case p.jobs <- job{ctx: ctx, msg: m}:
	case <-p.quit:
	}
}

// close stops accepting messages and waits for in-flight handlers or ctx expiry.
func (p *pool) close(ctx context.Context) error {
	p.once.Do(func() { close(p.quit) })
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
