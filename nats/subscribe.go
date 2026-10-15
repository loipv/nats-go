package nats

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	natsio "github.com/nats-io/nats.go"
)

// Subscribe registers fn for messages on subject. Wildcards (* and >) and
// Queue are supported. Errors returned by fn are logged.
//
// Core NATS never redelivers, so a failed message is not retried by default.
// MaxAttempts, WithBackoff and DLQ retry it in the worker, waiting for the
// backoff between attempts and keeping the worker busy meanwhile. Unlimited
// can stall the subscription while failures last: with every worker retrying,
// pending messages pile up and nats.go may report a slow consumer.
func Subscribe[T any](app *App, subject string, fn func(context.Context, *Msg[T]) error, opts ...HandlerOption) {
	registerCore(app, kindSubscribe, subject, typed(app.opts.codec, fn), opts)
}

// Reply registers fn as the responder for requests on subject. The returned
// value is encoded and sent back. A returned error is sent back in the
// Nats-Service-Error and Nats-Service-Error-Code headers: code 400 for
// Terminal errors (including decode and validation failures), 500 otherwise.
//
// A failed request is not retried by default. MaxAttempts, WithBackoff and
// DLQ retry it in the worker, and the response is sent after the retries
// finish, so the total retry time must fit within the requester's timeout.
// Unlimited can stall the subscription while failures last.
func Reply[Req, Resp any](app *App, subject string, fn func(context.Context, *Msg[Req]) (Resp, error), opts ...HandlerOption) {
	h := typed(app.opts.codec, func(ctx context.Context, m *Msg[Req]) error {
		resp, err := fn(ctx, m)
		if err != nil {
			return err
		}
		if out, ok := ctx.Value(replyKey{}).(*reply); ok {
			out.v, out.set = resp, true
		}
		return nil
	})
	registerCore(app, kindReply, subject, h, opts)
}

// reply carries Reply's response out of the handler chain. The worker sends
// it once the chain has succeeded, so a failure to encode or send the
// response never re-runs the handler.
type reply struct {
	v   any
	set bool
}

type replyKey struct{}

func registerCore(app *App, kind handlerKind, subject string, h Handler, opts []HandlerOption) {
	cfg, err := newHandlerConfig(kind, opts)
	if err == nil && subject == "" {
		err = fmt.Errorf("nats: %s: empty subject", kind)
	}
	if err != nil {
		app.addErr(err)
		return
	}
	app.register(&coreReg{kind: kind, subject: subject, cfg: cfg, h: h})
}

type coreReg struct {
	kind    handlerKind
	subject string
	cfg     handlerConfig
	h       Handler
}

// prepare checks the DLQ stream, if any.
func (r *coreReg) prepare(ctx context.Context, a *App) error {
	if r.cfg.dlq == "" {
		return nil
	}
	if err := a.checkDLQ(ctx, r.cfg.dlq, ""); err != nil {
		return fmt.Errorf("nats: %s %q: %w", r.kind, r.subject, err)
	}
	return nil
}

func (r *coreReg) start(_ context.Context, a *App) (runner, error) {
	h := a.chain(r.cfg, r.h)
	log := a.opts.logger.With("handler", r.subject, "kind", r.kind)
	rc := r.cfg.retry(r.kind)
	stopping := make(chan struct{})
	p := newPool(r.cfg.concurrency, func(ctx context.Context, m *RawMsg) {
		ctx = withLogger(ctx, log)
		if r.kind == kindReply && m.Reply == "" {
			log.Warn("nats: request without reply subject dropped", "subject", m.Subject)
			return
		}
		if r.kind != kindReply {
			a.handleLocal(ctx, h, m, rc, stopping, dlqSource{}, log)
			return
		}
		out := &reply{}
		abandoned, err := a.handleLocal(context.WithValue(ctx, replyKey{}, out), h, m, rc, stopping, dlqSource{}, log)
		if abandoned {
			return
		}
		if err == nil && out.set {
			var data []byte
			if data, err = encode(a.opts.codec, out.v); err != nil {
				err = fmt.Errorf("nats: encode response: %w", err)
				log.Error("nats: handler failed", "subject", m.Subject, "error", err)
			} else if err := a.nc.Publish(m.Reply, data); err != nil {
				log.Error("nats: send response", "subject", m.Subject, "error", err)
			}
		}
		// The requester gets the error even when the message went to the DLQ.
		if err != nil {
			if err := respondError(a.nc, m.Reply, err); err != nil {
				log.Error("nats: send error response", "subject", m.Subject, "error", err)
			}
		}
	})
	cb := func(m *natsio.Msg) { p.dispatch(a.baseCtx, m) }
	var sub *natsio.Subscription
	var err error
	if r.cfg.queue != "" {
		sub, err = a.nc.QueueSubscribe(r.subject, r.cfg.queue, cb)
	} else {
		sub, err = a.nc.Subscribe(r.subject, cb)
	}
	if err != nil {
		_ = p.close(context.Background())
		return nil, fmt.Errorf("nats: subscribe %q: %w", r.subject, err)
	}
	return &coreRunner{name: r.subject, kind: r.kind, sub: sub, pool: p, stopping: stopping}, nil
}

func respondError(nc *natsio.Conn, reply string, err error) error {
	code := 500
	if IsTerminal(err) {
		code = 400
	}
	m := natsio.NewMsg(reply)
	m.Header.Set("Nats-Service-Error", firstLine(err))
	m.Header.Set("Nats-Service-Error-Code", strconv.Itoa(code))
	return nc.PublishMsg(m)
}

type coreRunner struct {
	name    string
	kind    handlerKind
	sub     *natsio.Subscription
	pool    *pool
	stopped atomic.Bool

	// stopping is closed when stop begins, cutting retry waits short.
	stopping chan struct{}
	stopOnce sync.Once
}

func (r *coreRunner) stop(ctx context.Context) error {
	r.stopOnce.Do(func() { close(r.stopping) })
	r.stopped.Store(true)
	closed := r.sub.StatusChanged(natsio.SubscriptionClosed)
	if err := r.sub.Drain(); err == nil {
		select {
		case <-closed:
		case <-ctx.Done():
		}
	}
	return r.pool.close(ctx)
}

func (r *coreRunner) status() HandlerStatus {
	state := "running"
	if r.stopped.Load() {
		state = "stopped"
	}
	return HandlerStatus{Name: r.name, Kind: string(r.kind), State: state}
}
