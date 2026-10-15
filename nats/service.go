package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/nats-io/nats.go/micro"
)

// ServiceRef is a NATS micro service registered with Service. Add endpoints
// with Endpoint and groups with Group.
type ServiceRef struct {
	app *App
	reg *serviceReg
}

// GroupRef is a group of endpoints sharing a subject prefix, created with
// ServiceRef.Group or GroupRef.Group.
type GroupRef struct {
	svc    *ServiceRef
	groups []groupSpec
}

// EndpointParent is a *ServiceRef or *GroupRef, the parent of an Endpoint.
type EndpointParent interface {
	endpointParent() (*ServiceRef, []groupSpec)
}

type groupSpec struct {
	prefix string
	opts   []micro.GroupOpt
}

// Service registers a NATS micro service, discoverable through the $SRV
// PING, INFO and STATS subjects once the app starts. cfg is passed to
// micro.AddService as is; Name and Version are required, and Name must be
// unique within the app.
func Service(app *App, cfg micro.Config) *ServiceRef {
	s := &ServiceRef{app: app, reg: &serviceReg{cfg: cfg}}
	switch {
	case cfg.Name == "":
		app.addErr(errors.New("nats: service: empty name"))
		return s
	case cfg.Version == "":
		app.addErr(fmt.Errorf("nats: service %q: empty version", cfg.Name))
		return s
	}
	app.mu.Lock()
	defer app.mu.Unlock()
	if app.lateLocked() {
		return s
	}
	for _, r := range app.regs {
		if o, ok := r.(*serviceReg); ok && o.cfg.Name == cfg.Name {
			app.regErrs = append(app.regErrs, fmt.Errorf("nats: service %q: registered twice", cfg.Name))
			return s
		}
	}
	app.regs = append(app.regs, s.reg)
	return s
}

// Group returns a group whose endpoints' subjects are prefixed with prefix.
func (s *ServiceRef) Group(prefix string, opts ...micro.GroupOpt) *GroupRef {
	return (&GroupRef{svc: s}).Group(prefix, opts...)
}

// Group returns a nested group whose prefix is appended to g's.
func (g *GroupRef) Group(prefix string, opts ...micro.GroupOpt) *GroupRef {
	if prefix == "" {
		g.svc.app.addErr(fmt.Errorf("nats: service %q: empty group prefix", g.svc.reg.cfg.Name))
	}
	return &GroupRef{svc: g.svc, groups: append(append([]groupSpec(nil), g.groups...), groupSpec{prefix, opts})}
}

func (s *ServiceRef) endpointParent() (*ServiceRef, []groupSpec) { return s, nil }
func (g *GroupRef) endpointParent() (*ServiceRef, []groupSpec)   { return g.svc, g.groups }

// Endpoint registers fn as a micro endpoint of parent. The subject is the
// group prefixes followed by EndpointSubject, or name if unset. Requests are
// handled like Reply: the returned value is encoded and sent back, and an
// error is sent as a micro service error (code 400 for Terminal errors, 500
// otherwise), counted in the service STATS.
//
// Options allowed: Timeout, With, MaxAttempts, WithBackoff, DLQ,
// EndpointSubject and EndpointMetadata. MaxAttempts defaults to 1.
func Endpoint[Req, Resp any](parent EndpointParent, name string, fn func(context.Context, *Msg[Req]) (Resp, error), opts ...HandlerOption) {
	s, groups := parent.endpointParent()
	cfg, err := newHandlerConfig(kindEndpoint, opts)
	if err == nil && name == "" {
		err = errors.New("nats: endpoint: empty name")
	}
	if err != nil {
		s.app.addErr(fmt.Errorf("nats: service %q: %w", s.reg.cfg.Name, err))
		return
	}
	e := endpointReg{name: name, groups: groups, cfg: cfg, h: replyHandler(s.app.opts.codec, fn)}
	s.app.mu.Lock()
	defer s.app.mu.Unlock()
	if s.app.lateLocked() {
		return
	}
	s.reg.endpoints = append(s.reg.endpoints, e)
}

// EndpointSubject sets the subject of an endpoint, relative to its group.
// Defaults to the endpoint name.
func EndpointSubject(subject string) HandlerOption {
	return func(c *handlerConfig) { c.endpointSubject = subject; c.used = append(c.used, "EndpointSubject") }
}

// EndpointMetadata sets the metadata an endpoint reports in the service INFO.
func EndpointMetadata(md map[string]string) HandlerOption {
	return func(c *handlerConfig) { c.endpointMetadata = md; c.used = append(c.used, "EndpointMetadata") }
}

type serviceReg struct {
	cfg micro.Config
	// endpoints are in registration order; a.mu guards appends.
	endpoints []endpointReg
}

type endpointReg struct {
	name   string
	groups []groupSpec
	cfg    handlerConfig
	h      Handler
}

// subject is the endpoint's full subject, as micro builds it.
func (e endpointReg) subject() string {
	parts := make([]string, 0, len(e.groups)+1)
	for _, g := range e.groups {
		parts = append(parts, g.prefix)
	}
	if e.cfg.endpointSubject != "" {
		return strings.Join(append(parts, e.cfg.endpointSubject), ".")
	}
	return strings.Join(append(parts, e.name), ".")
}

// prepare checks the DLQ of every endpoint, as coreReg.prepare does.
func (r *serviceReg) prepare(ctx context.Context, a *App) error {
	for _, e := range r.endpoints {
		if e.cfg.dlq == "" {
			continue
		}
		if subjectMatches(e.subject(), e.cfg.dlq) {
			return fmt.Errorf("nats: service %q endpoint %q: DLQ subject %q is matched by the endpoint's own subject", r.cfg.Name, e.name, e.cfg.dlq)
		}
		if err := a.checkDLQ(ctx, e.cfg.dlq, ""); err != nil {
			return fmt.Errorf("nats: service %q endpoint %q: %w", r.cfg.Name, e.name, err)
		}
	}
	return nil
}

func (r *serviceReg) start(_ context.Context, a *App) (runner, error) {
	svc, err := micro.AddService(a.nc, r.cfg)
	if err != nil {
		return nil, fmt.Errorf("nats: service %q: %w", r.cfg.Name, err)
	}
	rn := &serviceRunner{name: r.cfg.Name, svc: svc, stopping: make(chan struct{})}
	for _, e := range r.endpoints {
		var parent micro.Group = svc
		for _, g := range e.groups {
			parent = parent.AddGroup(g.prefix, g.opts...)
		}
		var opts []micro.EndpointOpt
		if e.cfg.endpointSubject != "" {
			opts = append(opts, micro.WithEndpointSubject(e.cfg.endpointSubject))
		}
		if e.cfg.endpointMetadata != nil {
			opts = append(opts, micro.WithEndpointMetadata(e.cfg.endpointMetadata))
		}
		name := r.cfg.Name + "." + e.name
		if err := parent.AddEndpoint(e.name, rn.handler(a, name, e), opts...); err != nil {
			_ = svc.Stop()
			return nil, fmt.Errorf("nats: service %q: %w", r.cfg.Name, err)
		}
		rn.endpoints = append(rn.endpoints, name)
	}
	return rn, nil
}

type serviceRunner struct {
	name      string
	svc       micro.Service
	endpoints []string

	// stopping is closed when stop begins, cutting retry waits short.
	stopping chan struct{}
	// mu guards closed, so no inflight.Add follows inflight.Wait.
	mu       sync.Mutex
	closed   bool
	inflight sync.WaitGroup
}

// handler runs one request of endpoint e synchronously in the micro
// callback, so micro counts errors in its STATS.
func (r *serviceRunner) handler(a *App, name string, e endpointReg) micro.Handler {
	h := a.chain(e.cfg, e.h)
	rc := e.cfg.retry(kindEndpoint)
	log := a.opts.logger.With("handler", name, "kind", kindEndpoint)
	return micro.HandlerFunc(func(req micro.Request) {
		if !r.begin() {
			log.Warn("nats: request dropped, shutting down", "subject", req.Subject())
			return
		}
		defer r.inflight.Done()
		m := &RawMsg{Subject: req.Subject(), Reply: req.Reply(), Header: Header(req.Headers()), Data: req.Data()}
		resp, abandoned, err := a.handleRequest(withLogger(a.baseCtx, log), h, m, rc, r.stopping, log)
		switch {
		case abandoned:
		case err != nil:
			desc := firstLine(err)
			if desc == "" {
				desc = "error"
			}
			if err := req.Error(errorCode(err), desc, nil); err != nil {
				log.Error("nats: send error response", "subject", m.Subject, "error", err)
			}
		default:
			if err := req.Respond(resp); err != nil {
				log.Error("nats: send response", "subject", m.Subject, "error", err)
			}
		}
	})
}

// begin counts a request in flight, unless stop has begun.
func (r *serviceRunner) begin() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.inflight.Add(1)
	return true
}

// stop stops the service and waits for in-flight requests or ctx expiry.
// Requests still pending in the drained subscriptions are dropped.
func (r *serviceRunner) stop(ctx context.Context) error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		close(r.stopping)
	}
	r.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		err := r.svc.Stop()
		r.inflight.Wait()
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("nats: service %q: stop: %w", r.name, err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *serviceRunner) state() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return "stopped"
	}
	return "running"
}

// status summarises the service; Health reports statuses instead.
func (r *serviceRunner) status() HandlerStatus {
	return HandlerStatus{Name: r.name, Kind: string(kindEndpoint), State: r.state()}
}

// statuses reports one status per endpoint.
func (r *serviceRunner) statuses() []HandlerStatus {
	state := r.state()
	out := make([]HandlerStatus, len(r.endpoints))
	for i, name := range r.endpoints {
		out[i] = HandlerStatus{Name: name, Kind: string(kindEndpoint), State: state}
	}
	return out
}
