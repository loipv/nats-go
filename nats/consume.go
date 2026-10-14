package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

// Consume registers fn on an existing durable consumer (or one declared with
// App.Consumer). Pull and push consumers are detected at Start. With
// automatic ack (the default) a nil error acks, a Terminal error terms and
// any other error naks the message.
func Consume[T any](app *App, stream, consumer string, fn func(context.Context, *Msg[T]) error, opts ...HandlerOption) {
	cfg, err := newHandlerConfig(kindConsume, opts)
	if err == nil && (stream == "" || consumer == "") {
		err = errors.New("nats: consume: stream and consumer are required")
	}
	if err == nil && !app.claimConsumer(stream+"/"+consumer) {
		err = fmt.Errorf("nats: duplicate consumer %s/%s", stream, consumer)
	}
	if err != nil {
		app.addErr(err)
		return
	}
	app.register(&jsReg{kind: kindConsume, stream: stream, consumer: consumer, cfg: cfg, h: typed(app.opts.codec, fn)})
}

// ConsumeOrdered registers fn on an ephemeral ordered consumer of stream.
// Messages arrive in stream order and are never acked.
func ConsumeOrdered[T any](app *App, stream string, fn func(context.Context, *Msg[T]) error, opts ...HandlerOption) {
	cfg, err := newHandlerConfig(kindOrdered, opts)
	if err == nil && stream == "" {
		err = errors.New("nats: consume ordered: stream is required")
	}
	if err != nil {
		app.addErr(err)
		return
	}
	app.register(&jsReg{kind: kindOrdered, stream: stream, cfg: cfg, h: typed(app.opts.codec, fn)})
}

func (a *App) claimConsumer(key string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.consumeKeys == nil {
		a.consumeKeys = map[string]bool{}
	}
	if a.consumeKeys[key] {
		return false
	}
	a.consumeKeys[key] = true
	return true
}

type jsReg struct {
	kind     handlerKind
	stream   string
	consumer string
	cfg      handlerConfig
	h        Handler
}

func (r *jsReg) name() string {
	if r.kind == kindOrdered {
		return r.stream + " (ordered)"
	}
	return r.stream + "/" + r.consumer
}

func (r *jsReg) start(ctx context.Context, a *App) (runner, error) {
	rn := &jsRunner{name: r.name(), kind: r.kind}
	h := a.chain(r.cfg, r.h)
	log := a.opts.logger.With("handler", rn.name, "kind", r.kind)
	manual := r.cfg.manualAck || r.kind == kindOrdered
	rn.pool = newPool(r.cfg.concurrency, func(ctx context.Context, raw *RawMsg) {
		err := h(ctx, raw)
		if err != nil {
			log.Error("nats: handler failed", "subject", raw.Subject, "error", err)
		}
		if err := settle(jsMsgFrom(ctx), err, manual); err != nil {
			log.Error("nats: ack failed", "subject", raw.Subject, "error", err)
		}
	})

	cb := func(m jetstream.Msg) { rn.pool.dispatch(withJSMsg(a.baseCtx, m), toRaw(m)) }
	onErr := jetstream.ConsumeErrHandler(func(cc jetstream.ConsumeContext, err error) {
		if errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound) {
			rn.fail(err)
			cc.Stop()
			return
		}
		log.Warn("nats: consume", "error", err)
	})
	var pullOpts []jetstream.PullConsumeOpt
	if r.cfg.pullMax > 0 {
		pullOpts = append(pullOpts, jetstream.PullMaxMessages(r.cfg.pullMax))
	}
	pullOpts = append(pullOpts, onErr)

	cc, err := r.consume(ctx, a.js, cb, pullOpts, onErr)
	if err != nil {
		_ = rn.pool.close(context.Background())
		return nil, err
	}
	rn.cc = cc
	return rn, nil
}

func (r *jsReg) consume(ctx context.Context, js jetstream.JetStream, cb jetstream.MessageHandler, pullOpts []jetstream.PullConsumeOpt, onErr jetstream.ConsumeErrHandler) (jetstream.ConsumeContext, error) {
	wrap := func(err error) error { return fmt.Errorf("nats: consume %s: %w", r.name(), err) }
	if r.kind == kindOrdered {
		cfg := jetstream.OrderedConsumerConfig{FilterSubjects: r.cfg.filters}
		if r.cfg.deliver != nil {
			cfg.DeliverPolicy = *r.cfg.deliver
		}
		c, err := js.OrderedConsumer(ctx, r.stream, cfg)
		if err != nil {
			return nil, wrap(err)
		}
		cc, err := c.Consume(cb, pullOpts...)
		if err != nil {
			return nil, wrap(err)
		}
		return cc, nil
	}
	c, err := js.Consumer(ctx, r.stream, r.consumer)
	if errors.Is(err, jetstream.ErrNotPullConsumer) {
		pc, err := js.PushConsumer(ctx, r.stream, r.consumer)
		if err != nil {
			return nil, wrap(err)
		}
		cc, err := pc.Consume(cb, onErr)
		if err != nil {
			return nil, wrap(err)
		}
		return cc, nil
	}
	if err != nil {
		return nil, wrap(err)
	}
	cc, err := c.Consume(cb, pullOpts...)
	if err != nil {
		return nil, wrap(err)
	}
	return cc, nil
}

type jsRunner struct {
	name string
	kind handlerKind
	cc   jetstream.ConsumeContext
	pool *pool

	mu      sync.Mutex
	stopped bool
	lastErr error
}

func (r *jsRunner) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped, r.lastErr = true, err
}

// stop stops fetching; buffered, unacked messages are redelivered by the server.
func (r *jsRunner) stop(ctx context.Context) error {
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()
	r.cc.Stop()
	select {
	case <-r.cc.Closed():
	case <-ctx.Done():
	}
	return r.pool.close(ctx)
}

func (r *jsRunner) status() HandlerStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := HandlerStatus{Name: r.name, Kind: string(r.kind), State: "running"}
	if r.stopped {
		st.State = "stopped"
	}
	if r.lastErr != nil {
		st.LastError = r.lastErr.Error()
	}
	return st
}
