package nats

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Msg is a decoded message passed to typed handlers.
type Msg[T any] struct {
	Subject string
	Header  Header
	Data    T
	// Raw is the undecoded message.
	Raw *RawMsg

	js jetstream.Msg
}

func (m *Msg[T]) jsMsg() (jetstream.Msg, error) {
	if m.js == nil {
		return nil, ErrNotJetStream
	}
	return m.js, nil
}

// Ack acknowledges a JetStream message. Use it with ManualAck.
func (m *Msg[T]) Ack() error {
	js, err := m.jsMsg()
	if err != nil {
		return err
	}
	return js.Ack()
}

// Nak asks the server to redeliver a JetStream message.
func (m *Msg[T]) Nak() error {
	js, err := m.jsMsg()
	if err != nil {
		return err
	}
	return js.Nak()
}

// NakWithDelay asks the server to redeliver a JetStream message after d.
func (m *Msg[T]) NakWithDelay(d time.Duration) error {
	js, err := m.jsMsg()
	if err != nil {
		return err
	}
	return js.NakWithDelay(d)
}

// Term tells the server never to redeliver a JetStream message.
func (m *Msg[T]) Term() error {
	js, err := m.jsMsg()
	if err != nil {
		return err
	}
	return js.Term()
}

// InProgress resets the ack wait timer of a JetStream message.
func (m *Msg[T]) InProgress() error {
	js, err := m.jsMsg()
	if err != nil {
		return err
	}
	return js.InProgress()
}

type handlerKind string

const (
	kindSubscribe handlerKind = "subscribe"
	kindReply     handlerKind = "reply"
	kindConsume   handlerKind = "consume"
	kindOrdered   handlerKind = "ordered"
)

// HandlerOption configures a handler registered with Subscribe, Reply,
// Consume or ConsumeOrdered. Using an option on a handler kind it does not
// apply to is a registration error returned by Start.
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	queue       string
	concurrency int
	timeout     time.Duration
	mws         []Middleware
	manualAck   bool
	pullMax     int
	filters     []string
	deliver     *jetstream.DeliverPolicy
	used        []string
}

// allowedKinds returns the handler kinds a restricted option applies to,
// or nil when the option applies to every kind.
func allowedKinds(option string) []handlerKind {
	switch option {
	case "Queue":
		return []handlerKind{kindSubscribe, kindReply}
	case "ManualAck":
		return []handlerKind{kindConsume}
	case "PullMaxMessages":
		return []handlerKind{kindConsume, kindOrdered}
	case "FilterSubjects", "DeliverPolicy":
		return []handlerKind{kindOrdered}
	}
	return nil
}

func newHandlerConfig(kind handlerKind, opts []HandlerOption) (handlerConfig, error) {
	c := handlerConfig{concurrency: 1}
	for _, opt := range opts {
		opt(&c)
	}
	for _, name := range c.used {
		if kinds := allowedKinds(name); kinds != nil && !slices.Contains(kinds, kind) {
			return c, fmt.Errorf("nats: option %s not allowed for %s", name, kind)
		}
	}
	if kind == kindOrdered && c.concurrency > 1 {
		return c, fmt.Errorf("nats: concurrency must be 1 for ordered consumers to keep stream order, got %d", c.concurrency)
	}
	if c.concurrency < 1 {
		return c, fmt.Errorf("nats: concurrency must be at least 1, got %d", c.concurrency)
	}
	return c, nil
}

// Queue joins the subscription to a queue group, so each message is
// delivered to one member of the group.
func Queue(group string) HandlerOption {
	return func(c *handlerConfig) { c.queue = group; c.used = append(c.used, "Queue") }
}

// Concurrency runs up to n handler calls in parallel. Defaults to 1, which
// preserves message order.
func Concurrency(n int) HandlerOption {
	return func(c *handlerConfig) { c.concurrency = n; c.used = append(c.used, "Concurrency") }
}

// Timeout sets a deadline on the context passed to each handler call.
func Timeout(d time.Duration) HandlerOption {
	return func(c *handlerConfig) { c.timeout = d; c.used = append(c.used, "Timeout") }
}

// With adds middleware to this handler only, inside the app middleware.
func With(mw ...Middleware) HandlerOption {
	return func(c *handlerConfig) { c.mws = append(c.mws, mw...); c.used = append(c.used, "With") }
}

// ManualAck disables automatic ack: the handler acks with Msg.Ack and friends.
func ManualAck() HandlerOption {
	return func(c *handlerConfig) { c.manualAck = true; c.used = append(c.used, "ManualAck") }
}

// PullMaxMessages sets how many messages a pull consumer buffers.
func PullMaxMessages(n int) HandlerOption {
	return func(c *handlerConfig) { c.pullMax = n; c.used = append(c.used, "PullMaxMessages") }
}

// FilterSubjects limits an ordered consumer to the given subjects.
func FilterSubjects(subjects ...string) HandlerOption {
	return func(c *handlerConfig) { c.filters = subjects; c.used = append(c.used, "FilterSubjects") }
}

// DeliverPolicy sets where an ordered consumer starts in the stream.
func DeliverPolicy(p jetstream.DeliverPolicy) HandlerOption {
	return func(c *handlerConfig) { c.deliver = &p; c.used = append(c.used, "DeliverPolicy") }
}

// typed adapts a typed handler to a raw Handler: decode, validate, call.
// Decode and validation failures are terminal.
func typed[T any](c Codec, fn func(context.Context, *Msg[T]) error) Handler {
	return func(ctx context.Context, raw *RawMsg) error {
		data, err := decode[T](c, raw.Data)
		if err != nil {
			return Terminal(fmt.Errorf("nats: decode: %w", err))
		}
		if v, ok := any(&data).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return Terminal(fmt.Errorf("nats: validate: %w", err))
			}
		}
		return fn(ctx, &Msg[T]{Subject: raw.Subject, Header: raw.Header, Data: data, Raw: raw, js: jsMsgFrom(ctx)})
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

func toRaw(m jetstream.Msg) *RawMsg {
	return &RawMsg{Subject: m.Subject(), Reply: m.Reply(), Header: m.Headers(), Data: m.Data()}
}

// settle acks a JetStream message according to the handler result.
func settle(m jetstream.Msg, err error, manual bool) error {
	switch {
	case manual:
		return nil
	case err == nil:
		return m.Ack()
	case IsTerminal(err):
		return m.Term()
	default:
		return m.Nak()
	}
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
