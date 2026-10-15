package nats

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestNewValidatesOptions(t *testing.T) {
	if _, err := New(WithURL("")); err == nil {
		t.Fatal("empty url must fail")
	}
	if _, err := New(WithShutdownTimeout(0)); err == nil {
		t.Fatal("zero timeout must fail")
	}
}

func TestStartFailsFastWhenServerDown(t *testing.T) {
	a, _ := New(WithURL("nats://127.0.0.1:1"))
	begin := time.Now()
	if err := a.Start(context.Background()); err == nil {
		t.Fatal("want connect error")
	}
	if time.Since(begin) > 5*time.Second {
		t.Fatal("start must not retry")
	}
}

func TestStartTwice(t *testing.T) {
	a := newApp(t, runServer(t))
	start(t, a)
	if err := a.Start(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatal(err)
	}
}

func TestShutdownBeforeStartIsNoop(t *testing.T) {
	a, _ := New()
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRunReturnsNilOnCancel(t *testing.T) {
	a := newApp(t, runServer(t))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	waitFor(t, a.running)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !a.Conn().IsClosed() {
		t.Fatal("conn must be closed after Run")
	}
}

func TestStartReturnsJoinedRegistrationErrors(t *testing.T) {
	a := newApp(t, runServer(t))
	e1, e2 := errors.New("e1"), errors.New("e2")
	a.addErr(e1)
	a.addErr(e2)
	err := a.Start(context.Background())
	if !errors.Is(err, e1) || !errors.Is(err, e2) {
		t.Fatal(err)
	}
}

func TestTokenAuth(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.Authorization = "s3cret"
	s := natstest.RunServer(&opts)
	t.Cleanup(s.Shutdown)
	if err := newApp(t, s, WithToken("wrong")).Start(context.Background()); err == nil {
		t.Fatal("wrong token must fail")
	}
	a := newApp(t, s, WithToken("s3cret"))
	start(t, a)
}

// C1: a ctx-aware handler plus a queued message must not make Shutdown ignore its deadline.
func TestShutdownDeadlineWithQueuedMessageCore(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	started := make(chan struct{}, 2)
	a.Subscribe("block", Typed(func(ctx context.Context, _ *Msg[[]byte]) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}))
	start(t, a)
	_ = a.Conn().Publish("block", nil)
	_ = a.Conn().Publish("block", nil)
	<-started
	time.Sleep(50 * time.Millisecond) // second message is now blocked in dispatch
	assertShutdownBounded(t, a)
}

func TestShutdownDeadlineWithQueuedMessageJS(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	started := make(chan struct{}, 2)
	a.Consume("ORDERS", "billing", Typed(func(ctx context.Context, _ *Msg[order]) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "2"})
	<-started
	time.Sleep(50 * time.Millisecond)
	assertShutdownBounded(t, a)
}

func assertShutdownBounded(t *testing.T, a *App) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Shutdown(ctx) }()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown hung past its deadline")
	}
}

// I3: a failing Start must not have handled any message on earlier registrations.
func TestFailedStartHandlesNothing(t *testing.T) {
	cases := map[string]func(a *App){
		"missing consumer": func(a *App) {
			a.Consume("ORDERS", "nope", Typed(func(context.Context, *Msg[order]) error { return nil }))
		},
		"bad subject": func(a *App) {
			a.Subscribe("foo..bar", Typed(func(context.Context, *Msg[[]byte]) error { return nil }))
		},
	}
	for name, addFailing := range cases {
		t.Run(name, func(t *testing.T) {
			s := runServer(t)
			seed := newApp(t, s)
			setupStream(t, seed)
			start(t, seed)
			_, _ = seed.PublishJS(context.Background(), "orders.x", order{ID: "backlog"})

			a := newApp(t, s)
			var calls atomic.Int32
			a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error { calls.Add(1); return nil }))
			addFailing(a)
			if err := a.Start(context.Background()); err == nil {
				t.Fatal("Start must fail")
			}
			time.Sleep(200 * time.Millisecond)
			if calls.Load() != 0 {
				t.Fatal("handler ran during a failed Start")
			}
		})
	}
}

// I4: the default must keep reconnecting; a user option still wins.
func TestDefaultReconnectsForever(t *testing.T) {
	maxReconnect := func(a *App) int {
		o := natsio.GetDefaultOptions()
		for _, opt := range a.opts.natsOpts {
			_ = opt(&o)
		}
		return o.MaxReconnect
	}
	a, _ := New()
	if got := maxReconnect(a); got != -1 {
		t.Fatal("default MaxReconnect", got)
	}
	b, _ := New(WithNATSOptions(natsio.MaxReconnects(5)))
	if got := maxReconnect(b); got != 5 {
		t.Fatal("user MaxReconnect", got)
	}
}

// blockedApp starts an app whose "stuck" handler has received one message and
// blocks until release is closed.
func blockedApp(t *testing.T) (a *App, release chan struct{}) {
	t.Helper()
	a = newApp(t, runServer(t))
	started, release := make(chan struct{}), make(chan struct{})
	a.Subscribe("stuck", Typed(func(context.Context, *Msg[[]byte]) error {
		close(started)
		<-release
		return nil
	}))
	start(t, a)
	_ = a.Conn().Publish("stuck", nil)
	<-started
	return a, release
}

func shutdownAsync(a *App, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- a.Shutdown(ctx) }()
	return done
}

func TestConcurrentShutdownWaits(t *testing.T) {
	a, release := blockedApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first := shutdownAsync(a, ctx)
	time.Sleep(50 * time.Millisecond)
	second := shutdownAsync(a, ctx)
	expectNone(t, second, 150*time.Millisecond)
	close(release)
	if err := recv(t, second); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, first); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentShutdownOwnDeadline(t *testing.T) {
	a, release := blockedApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first := shutdownAsync(a, ctx)
	time.Sleep(50 * time.Millisecond)
	sctx, scancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer scancel()
	if err := a.Shutdown(sctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	close(release)
	if err := recv(t, first); err != nil {
		t.Fatal(err)
	}
}

type slowDialer struct{ delay time.Duration }

func (d slowDialer) Dial(network, address string) (net.Conn, error) {
	time.Sleep(d.delay)
	return net.Dial(network, address)
}

func TestHealthDuringStart(t *testing.T) {
	a := newApp(t, runServer(t), WithNATSOptions(natsio.SetCustomDialer(slowDialer{500 * time.Millisecond})))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan error, 1)
	go func() { started <- a.Start(ctx) }()
	time.Sleep(100 * time.Millisecond)
	health := make(chan Report, 1)
	go func() { health <- a.Health() }()
	select {
	case r := <-health:
		if r.Ready || r.Status != "down" {
			t.Fatalf("got %+v", r)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Health blocked during Start")
	}
	if err := recv(t, started); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Shutdown(context.Background()) })
}

// failingApp returns an app whose Start fails: consumer "missing" does not exist.
func failingApp(t *testing.T) *App {
	t.Helper()
	a := newApp(t, runServer(t))
	a.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	a.Consume("ORDERS", "missing", Typed(func(context.Context, *Msg[[]byte]) error { return nil }))
	if err := a.Start(context.Background()); err == nil {
		t.Fatal("want start error")
	}
	return a
}

func TestConnNilAfterFailedStart(t *testing.T) {
	a := failingApp(t)
	if a.Conn() != nil || a.JetStream() != nil {
		t.Fatal("Conn and JetStream must be nil after a failed Start")
	}
}

func TestStartRetryAfterFailure(t *testing.T) {
	a := failingApp(t)
	b, err := New(WithURL(a.opts.url))
	if err != nil {
		t.Fatal(err)
	}
	start(t, b)
	if _, err := b.JetStream().CreateOrUpdateConsumer(context.Background(), "ORDERS", jetstream.ConsumerConfig{Durable: "missing", AckPolicy: jetstream.AckExplicitPolicy}); err != nil {
		t.Fatal(err)
	}
	start(t, a)
}

func TestConcurrentStart(t *testing.T) {
	a := newApp(t, runServer(t), WithNATSOptions(natsio.SetCustomDialer(slowDialer{300 * time.Millisecond})))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first := make(chan error, 1)
	go func() { first <- a.Start(ctx) }()
	time.Sleep(100 * time.Millisecond)
	if err := a.Start(ctx); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatal(err)
	}
	if err := recv(t, first); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Shutdown(context.Background()) })
}

func TestShutdownDuringStart(t *testing.T) {
	a := newApp(t, runServer(t), WithNATSOptions(natsio.SetCustomDialer(slowDialer{500 * time.Millisecond})))
	started := make(chan error, 1)
	go func() { started <- a.Start(context.Background()) }()
	time.Sleep(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	// A Shutdown that did not wait for Start finds no connection, or one
	// that is still open.
	if nc := a.Conn(); nc == nil || !nc.IsClosed() || a.Health().Ready {
		t.Fatal("Shutdown must wait for Start and shut the started app down")
	}
	if err := recv(t, started); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDuringStartDeadline(t *testing.T) {
	a := newApp(t, runServer(t), WithNATSOptions(natsio.SetCustomDialer(slowDialer{500 * time.Millisecond})))
	started := make(chan error, 1)
	go func() { started <- a.Start(context.Background()) }()
	time.Sleep(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := a.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := recv(t, started); err == nil {
		t.Fatal("Start must fail after a cancelling Shutdown")
	}
	if a.Health().Ready || a.Conn() != nil {
		t.Fatal("app must not be running")
	}
}

func TestShutdownWaitsForInflight(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	started, finished := make(chan struct{}), make(chan struct{})
	a.Subscribe("slow", Typed(func(context.Context, *Msg[[]byte]) error {
		close(started)
		time.Sleep(200 * time.Millisecond)
		close(finished)
		return nil
	}))
	start(t, a)
	_ = a.Conn().Publish("slow", nil)
	<-started
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("shutdown returned before handler finished")
	}
}

func TestShutdownDeadlineExceeded(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	a.Subscribe("stuck", Typed(func(context.Context, *Msg[[]byte]) error {
		close(started)
		<-release
		return nil
	}))
	start(t, a)
	_ = a.Conn().Publish("stuck", nil)
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	begin := time.Now()
	if err := a.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if time.Since(begin) > time.Second || !a.Conn().IsClosed() {
		t.Fatal("must return promptly and close conn")
	}
}

// hookLog records hook calls in order.
type hookLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *hookLog) add(c string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, c)
}

func (l *hookLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.calls)
}

func (l *hookLog) hook(name string, startErr, stopErr error) Hook {
	return Hook{
		OnStart: func(context.Context) error { l.add("start " + name); return startErr },
		OnStop:  func(context.Context) error { l.add("stop " + name); return stopErr },
	}
}

func TestHookOrder(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	var l hookLog
	early := make(chan *Msg[[]byte], 1)
	a.Subscribe("early", Typed(func(_ context.Context, m *Msg[[]byte]) error { early <- m; return nil }))
	ha := l.hook("a", nil, nil)
	ha.OnStart = func(ctx context.Context) error {
		l.add("start a")
		return a.Publish(ctx, "early", nil)
	}
	ha.OnStop = func(ctx context.Context) error {
		l.add("stop a")
		return a.Publish(ctx, "late", nil)
	}
	a.Hook(ha)
	a.Hook(l.hook("b", nil, nil))
	a.Hook(l.hook("c", nil, nil))
	start(t, a)
	expectNone(t, early, 200*time.Millisecond)
	if got := l.get(); !slices.Equal(got, []string{"start a", "start b", "start c"}) {
		t.Fatalf("calls after Start: %v", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	want := []string{"start a", "start b", "start c", "stop c", "stop b", "stop a"}
	if got := l.get(); !slices.Equal(got, want) {
		t.Fatalf("calls: %v, want %v", got, want)
	}
}

func TestHookStartFailureRollsBack(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	var l hookLog
	var fail atomic.Bool
	fail.Store(true)
	b := l.hook("b", nil, nil)
	b.OnStart = func(context.Context) error {
		l.add("start b")
		if fail.Load() {
			return errors.New("boom")
		}
		return nil
	}
	a.Hook(l.hook("a", nil, nil))
	a.Hook(b)
	a.Hook(l.hook("c", nil, nil))
	err := a.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "nats: start hook 1: boom") {
		t.Fatalf("Start: %v", err)
	}
	if got := l.get(); !slices.Equal(got, []string{"start a", "start b", "stop a"}) {
		t.Fatalf("calls: %v", got)
	}
	fail.Store(false)
	start(t, a)
	want := []string{"start a", "start b", "stop a", "start a", "start b", "start c"}
	if got := l.get(); !slices.Equal(got, want) {
		t.Fatalf("calls: %v, want %v", got, want)
	}
}

func TestHookHandlerStartFailureStopsHooks(t *testing.T) {
	a := newApp(t, runServer(t))
	var l hookLog
	a.Hook(l.hook("a", nil, nil))
	a.Subscribe("foo..bar", Typed(func(context.Context, *Msg[[]byte]) error { return nil }))
	if err := a.Start(context.Background()); err == nil {
		t.Fatal("Start must fail")
	}
	if got := l.get(); !slices.Equal(got, []string{"start a", "stop a"}) {
		t.Fatalf("calls: %v", got)
	}
}

func TestHookStopErrorsJoined(t *testing.T) {
	a := newApp(t, runServer(t))
	var l hookLog
	errStop := errors.New("flush")
	a.Hook(l.hook("a", nil, errStop))
	start(t, a)
	err := a.Shutdown(context.Background())
	if !errors.Is(err, errStop) || !strings.Contains(err.Error(), "nats: stop hook 0") {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestHookStopWithExpiredContext(t *testing.T) {
	a := newApp(t, runServer(t))
	var expired atomic.Bool
	called := make(chan struct{})
	a.Hook(Hook{OnStop: func(ctx context.Context) error {
		expired.Store(ctx.Err() != nil)
		close(called)
		return nil
	}})
	start(t, a)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = a.Shutdown(ctx)
	recv(t, called)
	if !expired.Load() {
		t.Fatal("OnStop must see the expired Shutdown ctx")
	}
}

func TestHookRollbackStopErrorsJoined(t *testing.T) {
	a := newApp(t, runServer(t))
	var l hookLog
	errA := errors.New("flush")
	a.Hook(l.hook("a", nil, errA))
	a.Hook(l.hook("b", errors.New("boom"), nil))
	err := a.Start(context.Background())
	if !errors.Is(err, errA) || !strings.Contains(err.Error(), "nats: stop hook 0") || !strings.Contains(err.Error(), "nats: start hook 1: boom") {
		t.Fatalf("Start: %v", err)
	}
}

func TestHookWithoutOnStartCountsAsStarted(t *testing.T) {
	var l hookLog
	stopOnly := Hook{OnStop: func(context.Context) error { l.add("stop only"); return nil }}

	rollback := newApp(t, runServer(t))
	rollback.Hook(stopOnly)
	rollback.Hook(l.hook("b", errors.New("boom"), nil))
	if err := rollback.Start(context.Background()); err == nil {
		t.Fatal("Start must fail")
	}
	if got := l.get(); !slices.Equal(got, []string{"start b", "stop only"}) {
		t.Fatalf("rollback calls: %v", got)
	}

	shutdown := newApp(t, runServer(t))
	shutdown.Hook(stopOnly)
	start(t, shutdown)
	if err := shutdown.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := l.get(); !slices.Equal(got, []string{"start b", "stop only", "stop only"}) {
		t.Fatalf("shutdown calls: %v", got)
	}
}

func TestHookWithoutOnStopIsSkipped(t *testing.T) {
	a := newApp(t, runServer(t))
	var started atomic.Int32
	a.Hook(Hook{OnStart: func(context.Context) error { started.Add(1); return nil }})
	a.Hook(Hook{OnStart: func(context.Context) error { return errors.New("boom") }})
	if err := a.Start(context.Background()); err == nil {
		t.Fatal("Start must fail")
	}

	b := newApp(t, runServer(t))
	b.Hook(Hook{OnStart: func(context.Context) error { started.Add(1); return nil }})
	start(t, b)
	if err := b.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := started.Load(); n != 2 {
		t.Fatalf("started = %d, want 2", n)
	}
}

func TestHookStopOncePerStart(t *testing.T) {
	a := newApp(t, runServer(t))
	var l hookLog
	a.Hook(l.hook("a", nil, nil))
	start(t, a)
	for range 2 {
		if err := a.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := l.get(); !slices.Equal(got, []string{"start a", "stop a"}) {
		t.Fatalf("calls: %v", got)
	}
}

func TestHookShutdownDuringStart(t *testing.T) {
	a := newApp(t, runServer(t))
	var l hookLog
	release, entered := make(chan struct{}), make(chan struct{})
	a.Hook(Hook{
		OnStart: func(context.Context) error {
			l.add("start a")
			close(entered)
			<-release
			return nil
		},
		OnStop: func(context.Context) error { l.add("stop a"); return nil },
	})
	startErr := make(chan error, 1)
	go func() { startErr <- a.Start(context.Background()) }()
	recv(t, entered)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := a.Shutdown(ctx); err == nil {
		t.Fatal("Shutdown must fail while Start is blocked")
	}
	close(release)
	if err := recv(t, startErr); err == nil || !strings.Contains(err.Error(), "cancelled by Shutdown") {
		t.Fatalf("Start = %v, want cancelled by Shutdown", err)
	}
	if got := l.get(); !slices.Contains(got, "stop a") {
		t.Fatalf("calls: %v", got)
	}
}

func TestHookRegistration(t *testing.T) {
	a := newApp(t, runServer(t))
	a.Hook(Hook{})
	err := a.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "nats: hook: OnStart and OnStop are both nil") {
		t.Fatalf("Start: %v", err)
	}

	var buf bytes.Buffer
	b := newApp(t, runServer(t), WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
	start(t, b)
	var l hookLog
	b.Hook(l.hook("late", nil, nil))
	if !strings.Contains(buf.String(), "registration ignored") {
		t.Fatalf("late Hook not logged: %q", buf.String())
	}
	if err := b.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := l.get(); len(got) != 0 {
		t.Fatalf("late hook was called: %v", got)
	}
}

func TestStartFailureWithoutHooksKeepsCause(t *testing.T) {
	a := newApp(t, runServer(t))
	a.Subscribe("foo..bar", Typed(func(context.Context, *Msg[[]byte]) error { return nil }))
	err := a.Start(context.Background())
	if err == nil {
		t.Fatal("Start must fail")
	}
	if _, ok := err.(interface{ Unwrap() []error }); ok {
		t.Fatalf("error is wrapped in a join: %v", err)
	}
}
