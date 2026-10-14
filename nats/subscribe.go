package nats

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	natsio "github.com/nats-io/nats.go"
)

// Subscribe registers fn for messages on subject. Wildcards (* and >) and
// Queue are supported. Errors returned by fn are logged.
func Subscribe[T any](app *App, subject string, fn func(context.Context, *Msg[T]) error, opts ...HandlerOption) {
	registerCore(app, kindSubscribe, subject, typed(app.opts.codec, fn), opts)
}

// Reply registers fn as the responder for requests on subject. The returned
// value is encoded and sent back. A returned error is sent back in the
// Nats-Service-Error and Nats-Service-Error-Code headers: code 400 for
// Terminal errors (including decode and validation failures), 500 otherwise.
func Reply[Req, Resp any](app *App, subject string, fn func(context.Context, *Msg[Req]) (Resp, error), opts ...HandlerOption) {
	codec := app.opts.codec
	h := typed(codec, func(ctx context.Context, m *Msg[Req]) error {
		resp, err := fn(ctx, m)
		if err != nil {
			return err
		}
		data, err := encode(codec, resp)
		if err != nil {
			return fmt.Errorf("nats: encode response: %w", err)
		}
		return app.nc.Publish(m.Raw.Reply, data)
	})
	registerCore(app, kindReply, subject, h, opts)
}

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

func (r *coreReg) start(_ context.Context, a *App) (runner, error) {
	h := a.chain(r.cfg, r.h)
	log := a.opts.logger.With("handler", r.subject, "kind", r.kind)
	p := newPool(r.cfg.concurrency, func(ctx context.Context, m *RawMsg) {
		if r.kind == kindReply && m.Reply == "" {
			log.Warn("nats: request without reply subject dropped", "subject", m.Subject)
			return
		}
		err := h(ctx, m)
		if err == nil {
			return
		}
		log.Error("nats: handler failed", "subject", m.Subject, "error", err)
		if r.kind == kindReply {
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
	return &coreRunner{name: r.subject, kind: r.kind, sub: sub, pool: p}, nil
}

func respondError(nc *natsio.Conn, reply string, err error) error {
	code := 500
	if IsTerminal(err) {
		code = 400
	}
	// Only the first line goes on the wire: headers cannot carry newlines, and
	// multi-line errors (such as Recover's stack trace) must not leak to callers.
	desc, _, _ := strings.Cut(err.Error(), "\n")
	m := natsio.NewMsg(reply)
	m.Header.Set("Nats-Service-Error", desc)
	m.Header.Set("Nats-Service-Error-Code", strconv.Itoa(code))
	return nc.PublishMsg(m)
}

type coreRunner struct {
	name    string
	kind    handlerKind
	sub     *natsio.Subscription
	pool    *pool
	stopped atomic.Bool
}

func (r *coreRunner) stop(ctx context.Context) error {
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
