package nats

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

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
	select {
	case err := <-started:
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("Shutdown returned before Start")
	}
	if a.Health().Ready || !a.Conn().IsClosed() {
		t.Fatal("app must be shut down")
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
