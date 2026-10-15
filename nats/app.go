package nats

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// App owns a NATS connection and the handlers, streams and middleware
// registered on it. Register everything, then call Run (or Start and Shutdown).
type App struct {
	opts options

	mu       sync.Mutex
	starting bool
	// startDone is closed when the Start that set starting returns.
	startDone chan struct{}
	// abortStart is set by a Shutdown that gave up waiting for Start; that
	// Start then tears itself down instead of starting.
	abortStart  bool
	started     bool
	regErrs     []error
	regs        []registration
	streams     []jetstream.StreamConfig
	consumers   []consumerDecl
	consumeKeys map[string]bool
	mws         []Middleware
	hooks       []Hook
	pubMws      []PublishMiddleware
	runners     []runner
	// shutdownDone is set by the first Shutdown and closed when it returns
	// shutdownErr.
	shutdownDone chan struct{}
	shutdownErr  error

	isRunning atomic.Bool
	// pubOpen outlives isRunning during Shutdown so in-flight handlers can
	// still publish; it closes once every handler has stopped.
	pubOpen atomic.Bool
	nc      *natsio.Conn
	js      jetstream.JetStream
	// publishAsyncTimeout fails an async publish whose ack never arrives so it
	// does not count against the in-flight limit forever; tests shorten it.
	publishAsyncTimeout time.Duration
	closed              chan struct{}
	baseCtx             context.Context
	cancelBase          context.CancelFunc
}

// Hook is a pair of lifecycle callbacks registered with App.Hook. Either
// function may be nil.
//
// Hooks start in registration order and stop in reverse. OnStart runs after
// publishing is open and before any handler starts, so it can publish but no
// handler has seen a message yet. If a later hook or a handler fails to start,
// the hooks that already started are stopped in reverse and Start returns the
// error, with their OnStop errors joined into it (each wrapped
// "nats: stop hook i: <err>"). OnStop gets Shutdown's ctx, even when it has
// already expired, and must honor it. A panic in either function is not
// recovered.
type Hook struct {
	OnStart func(ctx context.Context) error
	OnStop  func(ctx context.Context) error
}

// HandlerStatus describes one registered handler.
type HandlerStatus struct {
	// Name is the subject, "stream/consumer", "stream (ordered)", or
	// for an endpoint the service name, group prefixes and endpoint name joined with "."
	// (orders.v2.get).
	Name string `json:"name"`
	// Kind is subscribe, reply, consume, ordered, batch or endpoint.
	Kind string `json:"kind"`
	// State is running, restarting (its consumer was deleted) or stopped
	// (after Shutdown).
	State     string `json:"state"`
	LastError string `json:"last_error,omitempty"`
	// Restarts counts how often the handler was restarted after its
	// consumer was deleted.
	Restarts int `json:"restarts"`
}

// registration is a handler recorded before Start.
type registration interface {
	start(ctx context.Context, a *App) (runner, error)
}

// preparer is a registration that resolves or checks server-side resources
// before any registration starts receiving messages.
type preparer interface {
	prepare(ctx context.Context, a *App) error
}

// runner is a started handler.
type runner interface {
	// stop stops intake and waits for in-flight handlers or ctx expiry.
	stop(ctx context.Context) error
	status() HandlerStatus
}

// New creates an App. It does not connect; WithNKeyFromSeed reads its seed
// file here.
func New(opts ...Option) (*App, error) {
	o := defaultOptions()
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	return &App{opts: o, publishAsyncTimeout: 30 * time.Second}, nil
}

// Conn returns the underlying connection. It is nil before Start and after
// a failed Start.
func (a *App) Conn() *natsio.Conn {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.started && !a.starting {
		return nil
	}
	return a.nc
}

// JetStream returns the JetStream context. It is nil before Start and after
// a failed Start.
func (a *App) JetStream() jetstream.JetStream {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.started && !a.starting {
		return nil
	}
	return a.js
}

// Use appends inbound middleware applied to every handler.
func (a *App) Use(mw ...Middleware) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lateLocked() {
		return
	}
	a.mws = append(a.mws, mw...)
}

// UsePublish appends outbound middleware applied to every publish.
func (a *App) UsePublish(mw ...PublishMiddleware) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lateLocked() {
		return
	}
	a.pubMws = append(a.pubMws, mw...)
}

// Hook registers lifecycle callbacks. Hooks start in registration order, after
// publishing opens and before any handler starts, and stop in reverse order
// once handlers have stopped (or Shutdown's deadline passed), while publishing
// is still open. A failed Start stops the hooks it started and joins their
// OnStop errors into its error, each wrapped "nats: stop hook i: <err>".
// Shutdown runs every OnStop once, joins their errors and gives them its ctx,
// even when expired. A Hook with both functions nil is a registration error.
// Panics are not recovered.
func (a *App) Hook(h Hook) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lateLocked() {
		return
	}
	if h.OnStart == nil && h.OnStop == nil {
		a.regErrs = append(a.regErrs, errors.New("nats: hook: OnStart and OnStop are both nil"))
		return
	}
	a.hooks = append(a.hooks, h)
}

func (a *App) register(r registration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lateLocked() {
		return
	}
	a.regs = append(a.regs, r)
}

func (a *App) addErr(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lateLocked() {
		return
	}
	a.regErrs = append(a.regErrs, err)
}

// lateLocked logs and reports registrations made after Start began. a.mu
// must be held.
func (a *App) lateLocked() bool {
	late := a.started || a.starting
	if late {
		a.opts.logger.Error("nats: registration ignored", "error", ErrAlreadyStarted)
	}
	return late
}

func (a *App) running() bool { return a.isRunning.Load() }

// Start connects, syncs declared streams and consumers, and starts every
// handler. It does not block. Any failure stops what was started and is
// returned, and Start may then be called again.
func (a *App) Start(ctx context.Context) error {
	a.mu.Lock()
	if a.started || a.starting {
		a.mu.Unlock()
		return ErrAlreadyStarted
	}
	if err := errors.Join(a.regErrs...); err != nil {
		a.mu.Unlock()
		return err
	}
	// The lock is not held while connecting so Health answers meanwhile;
	// starting rejects registrations and a second Start.
	a.starting, a.startDone = true, make(chan struct{})
	a.mu.Unlock()

	runners, err := a.connectAndStart(ctx)
	a.mu.Lock()
	defer a.mu.Unlock()
	if err == nil && a.abortStart {
		nc := a.nc
		a.mu.Unlock()
		hookErr := a.abandon(ctx, nc, runners, len(a.hooks))
		err = joinHookErr(errors.New("nats: start: cancelled by Shutdown"), hookErr)
		a.mu.Lock()
	}
	a.starting, a.abortStart = false, false
	close(a.startDone)
	// a.nc and a.js stay set after a failure: publishers read them without
	// the lock once past the pubOpen check. Conn and JetStream hide them.
	if err != nil {
		return err
	}
	a.runners, a.started = runners, true
	a.isRunning.Store(true)
	return nil
}

// connectAndStart does the work of Start without holding a.mu. On failure
// it closes the connection and stops whatever it started.
func (a *App) connectAndStart(ctx context.Context) ([]runner, error) {
	closed := make(chan struct{})
	natsOpts := append(slices.Clone(a.opts.natsOpts), a.connCallbacks(closed)...)
	nc, err := natsio.Connect(a.opts.url, natsOpts...)
	if err != nil {
		return nil, fmt.Errorf("nats: connect: %w", err)
	}
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncTimeout(a.publishAsyncTimeout))
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats: jetstream: %w", err)
	}
	a.mu.Lock()
	a.nc, a.js, a.closed = nc, js, closed
	a.mu.Unlock()
	if err := a.provision(ctx); err != nil {
		nc.Close()
		return nil, err
	}
	a.baseCtx, a.cancelBase = context.WithCancel(context.Background())
	var runners []runner
	var started int
	fail := func(err error) ([]runner, error) {
		return nil, joinHookErr(err, a.abandon(ctx, nc, runners, started))
	}

	// Resolve every consumer before any handler sees a message, so a failed
	// Start has no side effects.
	var core, jsRegs []registration
	for _, r := range a.regs {
		if p, ok := r.(preparer); ok {
			if err := p.prepare(ctx, a); err != nil {
				return fail(err)
			}
		}
		switch r.(type) {
		case *coreReg, *serviceReg:
			core = append(core, r)
		default:
			jsRegs = append(jsRegs, r)
		}
	}

	// Handlers may publish as soon as they receive messages. Core
	// subscriptions start first: they fail synchronously on a bad subject
	// before any JetStream backlog is delivered.
	a.pubOpen.Store(true)
	if started, err = a.startHooks(ctx); err != nil {
		return fail(err)
	}
	for _, r := range slices.Concat(core, jsRegs) {
		rn, err := r.start(ctx, a)
		if err != nil {
			return fail(err)
		}
		runners = append(runners, rn)
	}
	return runners, nil
}

// startHooks runs OnStart in order and returns how many hooks started before
// the first error.
func (a *App) startHooks(ctx context.Context) (started int, err error) {
	for i, h := range a.hooks {
		if h.OnStart != nil {
			if err := h.OnStart(ctx); err != nil {
				return i, fmt.Errorf("nats: start hook %d: %w", i, err)
			}
		}
	}
	return len(a.hooks), nil
}

// stopHooks runs OnStop of the first n hooks in reverse order and joins the
// errors.
func (a *App) stopHooks(ctx context.Context, n int) error {
	var errs []error
	for i := n - 1; i >= 0; i-- {
		if stop := a.hooks[i].OnStop; stop != nil {
			if err := stop(ctx); err != nil {
				errs = append(errs, fmt.Errorf("nats: stop hook %d: %w", i, err))
			}
		}
	}
	return join(errs)
}

// join is errors.Join, except that a lone error is returned as is, keeping
// its type.
func join(errs []error) error {
	var lone error
	n := 0
	for _, err := range errs {
		if err != nil {
			lone, n = err, n+1
		}
	}
	if n == 1 {
		return lone
	}
	return errors.Join(errs...)
}

// joinHookErr joins hookErr to err only when there is one, so a plain error
// keeps its type and errors.Unwrap result. A nil err yields hookErr.
func joinHookErr(err, hookErr error) error {
	switch {
	case hookErr == nil:
		return err
	case err == nil:
		return hookErr
	}
	return errors.Join(err, hookErr)
}

// abandon undoes a Start that failed or was cancelled after its handlers
// began starting, and returns the errors of the hooks it stopped.
func (a *App) abandon(ctx context.Context, nc *natsio.Conn, runners []runner, hooks int) error {
	a.cancelBase()
	_ = a.stopRunners(ctx, runners)
	err := a.stopHooks(ctx, hooks)
	a.pubOpen.Store(false)
	nc.Close()
	return err
}

// stopRunners stops runners in parallel, firing the stopped event as each
// one returns.
func (a *App) stopRunners(ctx context.Context, runners []runner) error {
	errs := make([]error, len(runners))
	var wg sync.WaitGroup
	for i, r := range runners {
		wg.Go(func() {
			errs[i] = r.stop(ctx)
			// A service reports one event per endpoint, as Health does.
			sts := []HandlerStatus{r.status()}
			if m, ok := r.(interface{ statuses() []HandlerStatus }); ok {
				sts = m.statuses()
			}
			for _, st := range sts {
				fire(a.opts.logger, a.opts.events.stopped, HandlerEvent{Handler: st.Name, Kind: st.Kind, Err: errs[i]})
			}
		})
	}
	wg.Wait()
	return join(errs)
}

// Shutdown stops intake on every handler, waits for in-flight handlers and
// drains the connection, bounded by ctx. It is a no-op before Start or after
// a completed Shutdown. A Shutdown called while another is in progress waits
// for it, bounded by its own ctx, and returns the same error. A Shutdown
// called during Start waits for Start, bounded by ctx, then shuts down what
// it started; if ctx ends first, that Start is cancelled and returns an error.
func (a *App) Shutdown(ctx context.Context) error {
	a.mu.Lock()
	if a.starting {
		done := a.startDone
		a.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
		}
		a.mu.Lock()
		// A retried Start may have begun since done closed; only abort the
		// one waited on, which can only still be running if ctx ended.
		if a.starting && a.startDone == done {
			a.abortStart = true
			a.mu.Unlock()
			return fmt.Errorf("nats: shutdown: %w", ctx.Err())
		}
		// Start finished; if ctx has also ended, the shutdown below closes
		// the connection at once and reports ctx.Err().
	}
	if !a.started {
		a.mu.Unlock()
		return nil
	}
	if done := a.shutdownDone; done != nil {
		a.mu.Unlock()
		select {
		case <-done:
			return nil
		default:
		}
		select {
		case <-done:
			return a.shutdownErr
		case <-ctx.Done():
			return fmt.Errorf("nats: shutdown: %w", ctx.Err())
		}
	}
	a.shutdownDone = make(chan struct{})
	a.isRunning.Store(false)
	a.mu.Unlock()
	a.shutdownErr = a.shutdown(ctx)
	close(a.shutdownDone)
	return a.shutdownErr
}

func (a *App) shutdown(ctx context.Context) error {
	// Handler contexts live until the deadline, then are cancelled so
	// ctx-aware handlers return and stopRunners can finish.
	stopCancel := context.AfterFunc(ctx, a.cancelBase)
	defer stopCancel()
	defer a.cancelBase()

	stopErr := a.stopRunners(ctx, a.runners)
	hookErr := a.stopHooks(ctx, len(a.hooks))
	a.pubOpen.Store(false)
	if ctx.Err() == nil {
		if err := a.nc.Drain(); err == nil {
			select {
			case <-a.closed:
				return joinHookErr(stopErr, hookErr)
			case <-ctx.Done():
			}
		}
	}
	a.nc.Close()
	if err := ctx.Err(); err != nil {
		return joinHookErr(fmt.Errorf("nats: shutdown: %w", err), hookErr)
	}
	return joinHookErr(stopErr, hookErr)
}

// Run calls Start, blocks until ctx is cancelled or the connection is closed
// for good, then shuts down within the WithShutdownTimeout limit.
func (a *App) Run(ctx context.Context) error {
	if err := a.Start(ctx); err != nil {
		return err
	}
	var closedErr error
	select {
	case <-ctx.Done():
	case <-a.closed:
		closedErr = fmt.Errorf("nats: %w", natsio.ErrConnectionClosed)
	}
	sctx, cancel := context.WithTimeout(context.Background(), a.opts.shutdownTimeout)
	defer cancel()
	if err := a.Shutdown(sctx); err != nil {
		return err
	}
	return closedErr
}
