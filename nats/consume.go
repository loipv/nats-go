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
// automatic ack (the default) a nil error acks; any other error naks the
// message with a backoff delay until MaxAttempts is used up. A Terminal
// error, or the last failed attempt, terms the message after publishing it
// to the DLQ if one is set.
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

	// Set by prepare: exactly one of pull or push, and info for durable
	// consumers.
	pull jetstream.Consumer
	push jetstream.PushConsumer
	info *jetstream.ConsumerInfo
}

// prepare resolves the consumer, detecting pull or push, without consuming.
func (r *jsReg) prepare(ctx context.Context, a *App) error {
	wrap := func(err error) error { return fmt.Errorf("nats: consume %s: %w", r.name(), err) }
	r.pull, r.push, r.info = nil, nil, nil
	if r.kind == kindOrdered {
		cfg := jetstream.OrderedConsumerConfig{FilterSubjects: r.cfg.filters}
		if r.cfg.deliver != nil {
			cfg.DeliverPolicy = *r.cfg.deliver
		}
		c, err := a.js.OrderedConsumer(ctx, r.stream, cfg)
		if err != nil {
			return wrap(err)
		}
		r.pull = c
		return r.check(ctx, a)
	}
	c, err := a.js.Consumer(ctx, r.stream, r.consumer)
	if errors.Is(err, jetstream.ErrNotPullConsumer) {
		pc, err := a.js.PushConsumer(ctx, r.stream, r.consumer)
		if err != nil {
			return wrap(err)
		}
		r.push, r.info = pc, pc.CachedInfo()
	} else if err != nil {
		return wrap(err)
	} else {
		r.pull, r.info = c, c.CachedInfo()
	}
	return r.check(ctx, a)
}

// check validates the retry settings against the server-side consumer.
func (r *jsReg) check(ctx context.Context, a *App) error {
	rc := r.cfg.retry(r.kind)
	if rc.dlq != "" {
		if err := a.checkDLQ(ctx, rc.dlq, r.stream); err != nil {
			return fmt.Errorf("nats: consume %s: %w", r.name(), err)
		}
	}
	if r.kind == kindOrdered || r.cfg.manualAck {
		return nil
	}
	// The server stops redelivering after MaxDeliver, before the last
	// attempt could reach the DLQ.
	maxDeliver := r.info.Config.MaxDeliver
	if rc.maxAttempts != Unlimited && maxDeliver != -1 && maxDeliver <= rc.maxAttempts {
		return fmt.Errorf("nats: consume %s: MaxDeliver %d must exceed MaxAttempts %d", r.name(), maxDeliver, rc.maxAttempts)
	}
	if rc.maxAttempts == Unlimited && maxDeliver > 0 {
		a.opts.logger.Warn("nats: consumer MaxDeliver drops messages the handler retries forever", "handler", r.name(), "max_deliver", maxDeliver)
	}
	return nil
}

func (r *jsReg) name() string {
	if r.kind == kindOrdered {
		return r.stream + " (ordered)"
	}
	return r.stream + "/" + r.consumer
}

func (r *jsReg) start(_ context.Context, a *App) (runner, error) {
	rn := &jsRunner{name: r.name(), kind: r.kind}
	h := a.chain(r.cfg, r.h)
	log := a.opts.logger.With("handler", rn.name, "kind", r.kind)
	manual := r.cfg.manualAck || r.kind == kindOrdered
	rc := r.cfg.retry(r.kind)
	rn.pool = newPool(r.cfg.concurrency, func(ctx context.Context, raw *RawMsg) {
		if manual {
			if err := safeCall(ctx, h, raw, log); err != nil {
				log.Error("nats: handler failed", "subject", raw.Subject, "error", err)
			}
			return
		}
		m := jsMsgFrom(ctx)
		src := dlqSource{stream: r.stream, consumer: r.consumer, deliveries: 1}
		if md, err := m.Metadata(); err == nil {
			src.seq, src.deliveries, src.msgID = md.Sequence.Stream, int(md.NumDelivered), true
		}
		// A message redelivered past MaxAttempts (say, after AckWait expired
		// on a slow handler) is given up on without running the handler.
		err := errMaxAttempts
		if !rc.exceeded(src.deliveries) {
			err = safeCall(ctx, h, raw, log)
		}
		a.dispose(ctx, m, raw, err, rc, src, log)
	})

	cb := func(m jetstream.Msg) { rn.pool.dispatch(withJSMsg(a.baseCtx, m), toRaw(m)) }
	onErr := jetstream.ConsumeErrHandler(func(cc jetstream.ConsumeContext, err error) {
		// nats.go recreates ordered consumers itself, so only durable
		// consumers stop for good when deleted.
		if r.kind != kindOrdered && (errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound)) {
			rn.fail(err)
			cc.Stop()
			return
		}
		log.Warn("nats: consume", "error", err)
	})

	var cc jetstream.ConsumeContext
	var err error
	if r.push != nil {
		cc, err = r.push.Consume(cb, onErr)
	} else {
		opts := []jetstream.PullConsumeOpt{onErr}
		if r.cfg.pullMax > 0 {
			opts = append(opts, jetstream.PullMaxMessages(r.cfg.pullMax))
		}
		cc, err = r.pull.Consume(cb, opts...)
	}
	if err != nil {
		_ = rn.pool.close(context.Background())
		return nil, fmt.Errorf("nats: consume %s: %w", r.name(), err)
	}
	rn.cc = cc
	return rn, nil
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
