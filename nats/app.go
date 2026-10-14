package nats

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// App owns a NATS connection and the handlers, streams and middleware
// registered on it. Register everything, then call Run (or Start and Shutdown).
type App struct {
	opts options

	mu      sync.Mutex
	started bool
	stopped bool
	regErrs []error
	regs    []registration
	mws     []Middleware
	pubMws  []PublishMiddleware
	runners []runner

	isRunning  atomic.Bool
	nc         *natsio.Conn
	js         jetstream.JetStream
	closed     chan struct{}
	baseCtx    context.Context
	cancelBase context.CancelFunc
}

// HandlerStatus describes one registered handler.
type HandlerStatus struct {
	Name      string
	Kind      string
	State     string
	LastError string
}

// registration is a handler recorded before Start.
type registration interface {
	start(ctx context.Context, a *App) (runner, error)
}

// runner is a started handler.
type runner interface {
	// stop stops intake and waits for in-flight handlers or ctx expiry.
	stop(ctx context.Context) error
	status() HandlerStatus
}

// New creates an App. It performs no I/O.
func New(opts ...Option) (*App, error) {
	o := defaultOptions()
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	return &App{opts: o}, nil
}

// Conn returns the underlying connection. It is nil before Start.
func (a *App) Conn() *natsio.Conn { return a.nc }

// JetStream returns the JetStream context. It is nil before Start.
func (a *App) JetStream() jetstream.JetStream { return a.js }

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

// lateLocked logs and reports registrations made after Start. a.mu must be held.
func (a *App) lateLocked() bool {
	if a.started {
		a.opts.logger.Error("nats: registration ignored", "error", ErrAlreadyStarted)
	}
	return a.started
}

func (a *App) running() bool { return a.isRunning.Load() }

// Start connects, syncs declared streams and consumers, and starts every
// handler. It does not block. Any failure stops what was started and is
// returned.
func (a *App) Start(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started {
		return ErrAlreadyStarted
	}
	if err := errors.Join(a.regErrs...); err != nil {
		return err
	}

	// ponytail: overrides a ClosedHandler passed via WithNATSOptions; chain it if someone needs both.
	closed := make(chan struct{})
	natsOpts := append(slices.Clone(a.opts.natsOpts), natsio.ClosedHandler(func(*natsio.Conn) { close(closed) }))
	nc, err := natsio.Connect(a.opts.url, natsOpts...)
	if err != nil {
		return fmt.Errorf("nats: connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return fmt.Errorf("nats: jetstream: %w", err)
	}
	a.nc, a.js, a.closed = nc, js, closed
	a.baseCtx, a.cancelBase = context.WithCancel(context.Background())

	for _, r := range a.regs {
		rn, err := r.start(ctx, a)
		if err != nil {
			a.stopRunners(ctx)
			a.runners = nil
			a.cancelBase()
			nc.Close()
			return err
		}
		a.runners = append(a.runners, rn)
	}

	a.started = true
	a.isRunning.Store(true)
	return nil
}

func (a *App) stopRunners(ctx context.Context) error {
	errs := make([]error, len(a.runners))
	var wg sync.WaitGroup
	for i, r := range a.runners {
		wg.Go(func() { errs[i] = r.stop(ctx) })
	}
	wg.Wait()
	return errors.Join(errs...)
}

// Shutdown stops intake on every handler, waits for in-flight handlers and
// drains the connection, bounded by ctx. It is a no-op before Start or after
// a previous Shutdown.
func (a *App) Shutdown(ctx context.Context) error {
	a.mu.Lock()
	if !a.started || a.stopped {
		a.mu.Unlock()
		return nil
	}
	a.stopped = true
	a.isRunning.Store(false)
	a.mu.Unlock()
	defer a.cancelBase()

	stopErr := a.stopRunners(ctx)
	if ctx.Err() == nil {
		if err := a.nc.Drain(); err == nil {
			select {
			case <-a.closed:
				return stopErr
			case <-ctx.Done():
			}
		}
	}
	a.nc.Close()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("nats: shutdown: %w", err)
	}
	return stopErr
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
