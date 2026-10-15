package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Consume registers fn on an existing durable consumer (or one declared with
// App.Consumer). Pull and push consumers are detected at Start. With
// automatic ack (the default) a nil error acks; any other error naks the
// message with a backoff delay until MaxAttempts is used up. A Terminal
// error, or the last failed attempt, terms the message after publishing it
// to the DLQ if one is set.
//
// Push consumers should set IdleHeartbeat in their ConsumerConfig: without
// it a deleted push consumer cannot be detected, so the handler is not
// restarted.
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
//
// A failed message is not retried by default. MaxAttempts, WithBackoff and
// DLQ retry it in process; the whole stream waits while a message is retried,
// which keeps the order. Unlimited can stall the stream while failures last.
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

	// ackWait is the consumer's AckWait, set by prepare for durable consumers.
	ackWait time.Duration
}

// prepare resolves the consumer, detecting pull or push, without consuming.
func (r *jsReg) prepare(ctx context.Context, a *App) error {
	wrap := func(err error) error { return fmt.Errorf("nats: consume %s: %w", r.name(), err) }
	r.pull, r.push, r.info, r.ackWait = nil, nil, nil, defaultAckWait
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
	if w := r.info.Config.AckWait; w > 0 {
		r.ackWait = w
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

// source describes a message of a durable consumer for dispose.
func (r *jsReg) source(m jetstream.Msg) dlqSource {
	src := dlqSource{stream: r.stream, consumer: r.consumer, deliveries: 1}
	if md, err := m.Metadata(); err == nil {
		src.seq, src.deliveries, src.msgID = md.Sequence.Stream, int(md.NumDelivered), true
	}
	return src
}

func (r *jsReg) start(_ context.Context, a *App) (runner, error) {
	rn := &jsRunner{runnerState: newRunnerState(r.name(), r.kind)}
	rn.ctx, rn.cancel = context.WithCancel(context.Background())
	h := a.chain(r.cfg, r.h)
	log := a.opts.logger.With("handler", rn.name, "kind", r.kind)
	rc := r.cfg.retry(r.kind)
	rn.pool = newPool(r.cfg.concurrency, func(ctx context.Context, raw *RawMsg) {
		ctx = withLogger(ctx, log)
		if r.kind == kindOrdered {
			// Retries hold the worker, so the stream waits and stays in order.
			src := dlqSource{stream: r.stream}
			if md, err := jsMsgFrom(ctx).Metadata(); err == nil {
				src.seq, src.msgID = md.Sequence.Stream, true
			}
			a.handleLocal(ctx, h, raw, rc, rn.ctx.Done(), src, log)
			return
		}
		// call runs the handler, extending AckWait meanwhile if asked to.
		call := func() error {
			if r.cfg.autoInProgress {
				defer keepAlive([]jetstream.Msg{jsMsgFrom(ctx)}, r.ackWait/2, log)()
			}
			return safeCall(ctx, h, raw, log)
		}
		if r.cfg.manualAck {
			if err := call(); err != nil {
				log.Error("nats: handler failed", "subject", raw.Subject, "error", err)
			}
			return
		}
		m := jsMsgFrom(ctx)
		src := r.source(m)
		// A message redelivered past MaxAttempts (say, after AckWait expired
		// on a slow handler) is given up on without running the handler.
		err := errMaxAttempts
		if !rc.exceeded(src.deliveries) {
			err = call()
		}
		a.dispose(ctx, m, raw, err, rc, src, log)
	})

	cb := func(m jetstream.Msg) { rn.pool.dispatch(withJSMsg(a.baseCtx, m), toRaw(m)) }
	var onErr jetstream.ConsumeErrHandler
	onErr = func(cc jetstream.ConsumeContext, err error) {
		// nats.go recreates ordered consumers itself.
		if r.kind == kindOrdered || !mayBeGone(err) {
			log.Warn("nats: consume", "error", err)
			return
		}
		// Off the nats.go goroutine: the lookup in gone blocks, and push
		// consumers call onErr holding the lock that cc.Stop takes.
		rn.supervise(cc, err, func(ctx context.Context, cc jetstream.ConsumeContext, err error) {
			rn.mu.Lock()
			replaced := cc != rn.cc
			rn.mu.Unlock()
			if replaced {
				return
			}
			if !r.gone(ctx, a, err) {
				log.Warn("nats: consume", "error", err)
				return
			}
			rn.setRestarting(err)
			cc.Stop()
			reconnect := func(ctx context.Context) error {
				if err := a.recoverConsumer(ctx, r.stream, r.consumer); err != nil {
					return err
				}
				// Prepare a copy: workers read r.ackWait concurrently, so
				// they keep the AckWait seen at Start.
				nr := *r
				if err := nr.prepare(ctx, a); err != nil {
					return err
				}
				return rn.consume(&nr, cb, onErr)
			}
			if a.superviseLoop(ctx, ctx.Done(), reconnect, log) {
				rn.setRestarted()
			}
		})
	}

	if err := rn.consume(r, cb, onErr); err != nil {
		_ = rn.pool.close(context.Background())
		return nil, err
	}
	return rn, nil
}

// consume starts consuming the consumer r resolved in prepare. It holds mu
// until cc is set, so onErr never sees a consume context it does not know.
func (rn *jsRunner) consume(r *jsReg, cb jetstream.MessageHandler, onErr jetstream.ConsumeErrHandler) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
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
		return fmt.Errorf("nats: consume %s: %w", r.name(), err)
	}
	rn.cc = cc
	return nil
}

type jsRunner struct {
	runnerState
	pool *pool

	// ctx is cancelled when stop begins, cutting retry and restart waits
	// short.
	ctx    context.Context
	cancel context.CancelFunc

	// Guarded by mu. supervisor is closed when the running supervisor
	// exits; nil when none runs. pending is the error it runs for next.
	cc         jetstream.ConsumeContext
	supervisor chan struct{}
	pendingCC  jetstream.ConsumeContext
	pendingErr error
}

// supervise runs fn for the error err of cc in a goroutine, unless the
// runner is stopping. While one runs, the latest error is kept and run
// next: it may come from a consume context the running one just opened.
func (r *jsRunner) supervise(cc jetstream.ConsumeContext, err error, fn func(ctx context.Context, cc jetstream.ConsumeContext, err error)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ctx.Err() != nil {
		return
	}
	if r.supervisor != nil {
		r.pendingCC, r.pendingErr = cc, err
		return
	}
	done := make(chan struct{})
	r.supervisor = done
	go func() {
		defer close(done)
		for err != nil {
			fn(r.ctx, cc, err)
			r.mu.Lock()
			cc, err, r.pendingCC, r.pendingErr = r.pendingCC, r.pendingErr, nil, nil
			if err == nil {
				r.supervisor = nil
			}
			r.mu.Unlock()
		}
	}()
}

// stop stops the supervisor, then fetching; buffered, unacked messages are
// redelivered by the server.
func (r *jsRunner) stop(ctx context.Context) error {
	r.cancel()
	r.setStopped()
	r.mu.Lock()
	sup := r.supervisor
	r.mu.Unlock()
	if sup != nil {
		select {
		case <-sup:
		case <-ctx.Done():
		}
	}
	r.mu.Lock()
	cc := r.cc
	r.mu.Unlock()
	cc.Stop()
	select {
	case <-cc.Closed():
	case <-ctx.Done():
	}
	return r.pool.close(ctx)
}
