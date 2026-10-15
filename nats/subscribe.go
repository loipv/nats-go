package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// Subscribe registers h for messages on subject. Wildcards (* and >) and
// WithQueue are supported. Errors returned by the handler are logged.
//
// Core NATS never redelivers, so a failed message is not retried by default.
// WithMaxAttempts, WithBackoff and WithDLQ retry it in the worker, waiting for the
// backoff between attempts and keeping the worker busy meanwhile. Unlimited
// can stall the subscription while failures last: with every worker retrying,
// pending messages pile up and nats.go may report a slow consumer.
func (a *App) Subscribe(subject string, h MsgHandler, opts ...HandlerOption) {
	registerCore(a, kindSubscribe, subject, h.bind, opts)
}

// SubscribeReply registers h as the responder for requests on subject. The returned
// value is encoded and sent back. A returned error is sent back in the
// Nats-Service-Error and Nats-Service-Error-Code headers: code 400 for
// Terminal errors (including decode and validation failures), 500 otherwise.
// A panic in h is sent as "internal error" with code 500; the panic and its
// stack are only logged.
//
// A failed request is not retried by default. WithMaxAttempts, WithBackoff and
// WithDLQ retry it in the worker, and the response is sent after the retries
// finish, so the total retry time must fit within the requester's timeout.
// Unlimited can stall the subscription while failures last.
func (a *App) SubscribeReply(subject string, h ReplyHandler, opts ...HandlerOption) {
	registerCore(a, kindReply, subject, h.bind, opts)
}

// replyHandler adapts a typed responder to a raw Handler that stores the
// response in the reply slot of ctx.
func replyHandler[Req, Resp any](c Codec, fn func(context.Context, *Msg[Req]) (Resp, error)) Handler {
	return typed(c, func(ctx context.Context, m *Msg[Req]) error {
		resp, err := fn(ctx, m)
		if err != nil {
			return err
		}
		if out, ok := ctx.Value(replyKey{}).(*reply); ok {
			out.v, out.set = resp, true
		}
		return nil
	})
}

// reply carries SubscribeReply's response out of the handler chain. The worker sends
// it once the chain has succeeded, so a failure to encode or send the
// response never re-runs the handler.
type reply struct {
	v   any
	set bool
}

type replyKey struct{}

func registerCore(app *App, kind handlerKind, subject string, bind func(Codec) Handler, opts []HandlerOption) {
	cfg, err := newHandlerConfig(kind, opts)
	if err == nil && subject == "" {
		err = fmt.Errorf("nats: %s: empty subject", kind)
	}
	if err == nil && bind == nil {
		err = fmt.Errorf("nats: %s %q: nil handler", kind, subject)
	}
	if err != nil {
		app.addErr(err)
		return
	}
	app.register(&coreReg{kind: kind, subject: subject, cfg: cfg, h: bind(app.opts.codec)})
}

type coreReg struct {
	kind    handlerKind
	subject string
	cfg     handlerConfig
	h       Handler
}

// prepare checks the DLQ, if any: a DLQ subject the handler subscribes to
// would feed every dead-lettered message back to it.
func (r *coreReg) prepare(ctx context.Context, a *App) error {
	if r.cfg.dlq == "" {
		return nil
	}
	if subjectMatches(r.subject, r.cfg.dlq) {
		return fmt.Errorf("nats: %s %q: DLQ subject %q is matched by the handler's own subject", r.kind, r.subject, r.cfg.dlq)
	}
	if err := a.checkDLQ(ctx, r.cfg.dlq, ""); err != nil {
		return fmt.Errorf("nats: %s %q: %w", r.kind, r.subject, err)
	}
	return nil
}

// subjectMatches reports whether subject is matched by pattern, which may
// contain the * and > wildcards.
func subjectMatches(pattern, subject string) bool {
	p, s := strings.Split(pattern, "."), strings.Split(subject, ".")
	for i, tok := range p {
		if tok == ">" {
			return len(s) > i
		}
		if i >= len(s) || (tok != "*" && tok != s[i]) {
			return false
		}
	}
	return len(p) == len(s)
}

func (r *coreReg) start(_ context.Context, a *App) (runner, error) {
	h := a.chain(r.cfg, r.h)
	log := a.sink(r.subject, r.kind)
	rc := r.cfg.retry(r.kind)
	stopping := make(chan struct{})
	p := newPool(r.cfg.concurrency, func(ctx context.Context, m *RawMsg) {
		ctx = withSink(ctx, log)
		if r.kind == kindReply && m.Reply == "" {
			log.Warn("nats: request without reply subject dropped", "subject", m.Subject)
			return
		}
		if r.kind != kindReply {
			a.handleLocal(ctx, h, m, rc, stopping, dlqSource{}, log)
			return
		}
		resp, abandoned, err := a.handleRequest(ctx, h, m, rc, stopping, log)
		switch {
		case abandoned:
		case err != nil:
			// The requester gets the error even when the message went to the DLQ.
			if err := respondError(m, err); err != nil {
				log.Error("nats: send error response", "subject", m.Subject, "error", err)
				log.asyncErr(m.Subject, fmt.Errorf("respond: %w", err))
			}
		case resp != nil:
			if err := m.Respond(resp); err != nil {
				log.Error("nats: send response", "subject", m.Subject, "error", err)
				log.asyncErr(m.Subject, fmt.Errorf("respond: %w", err))
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

// handleRequest runs a responder handler with in-process retries and the
// DLQ, like handleLocal, and encodes the response it stored in the reply
// slot. resp is nil when the chain succeeded without setting a response (a
// middleware did not call next). An encode failure is returned, not retried.
func (a *App) handleRequest(ctx context.Context, h Handler, m *RawMsg, rc retryConfig, stopping <-chan struct{}, log *sink) (resp []byte, abandoned bool, err error) {
	out := &reply{}
	abandoned, err = a.handleLocal(context.WithValue(ctx, replyKey{}, out), h, m, rc, stopping, dlqSource{}, log)
	if abandoned || err != nil || !out.set {
		return nil, abandoned, err
	}
	if resp, err = encode(a.opts.codec, out.v); err != nil {
		err = fmt.Errorf("nats: encode response: %w", err)
		log.Error("nats: handler failed", "subject", m.Subject, "error", err)
		log.failed(m, 0, err)
		return nil, false, err
	}
	if resp == nil {
		resp = []byte{} // a nil []byte response is still a response
	}
	return resp, false, nil
}

// errorCode is the service error code for a handler error: 400 for Terminal
// errors (including decode and validation failures), 500 otherwise.
func errorCode(err error) string {
	if IsTerminal(err) {
		return "400"
	}
	return "500"
}

// errorDescription is the first line of err, or "internal error" for a panic,
// whose text stays in the server log.
func errorDescription(err error) string {
	if errors.As(err, new(*panicError)) {
		return "internal error"
	}
	return firstLine(err)
}

// respondError answers req with err in the micro error headers.
func respondError(req *RawMsg, err error) error {
	return req.RespondMsg(&natsio.Msg{Header: Header{
		micro.ErrorHeader:     {errorDescription(err)},
		micro.ErrorCodeHeader: {errorCode(err)},
	}})
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
