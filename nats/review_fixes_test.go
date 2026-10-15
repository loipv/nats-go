package nats

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

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

// I1: ordered consumers must not accept Concurrency > 1.
func TestOrderedRejectsConcurrency(t *testing.T) {
	if _, err := newHandlerConfig(kindOrdered, []HandlerOption{WithConcurrency(4)}); err == nil {
		t.Fatal("WithConcurrency(4) must be rejected for ordered")
	}
	if _, err := newHandlerConfig(kindOrdered, []HandlerOption{WithConcurrency(1)}); err != nil {
		t.Fatal(err)
	}
}

// I2: nats.go recreates a deleted ordered consumer; health must not stay degraded.
func TestOrderedConsumerRecoversAfterDelete(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	got := make(chan string, 4)
	a.ConsumeOrdered("ORDERS", Typed(func(_ context.Context, m *Msg[order]) error { got <- m.Data.ID; return nil }))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "before"})
	recv(t, got)
	stream, _ := a.JetStream().Stream(context.Background(), "ORDERS")
	names := stream.ConsumerNames(context.Background())
	for name := range names.Name() {
		if name != "billing" {
			_ = stream.DeleteConsumer(context.Background(), name)
		}
	}
	time.Sleep(500 * time.Millisecond)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "after"})
	if recv(t, got) != "after" {
		t.Fatal("ordered consumer did not recover")
	}
	if st := a.Health(); st.Status != "ok" {
		t.Fatalf("health %+v", st)
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

// I3: handlers that run as soon as Start opens intake must be able to publish.
func TestHandlerCanPublishDuringStartup(t *testing.T) {
	s := runServer(t)
	seed := newApp(t, s)
	setupStream(t, seed)
	start(t, seed)
	for range 20 {
		_, _ = seed.PublishJS(context.Background(), "orders.x", order{ID: "backlog"})
	}
	a := newApp(t, s)
	var failed atomic.Int32
	var handled atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(ctx context.Context, _ *Msg[order]) error {
		if err := a.Publish(ctx, "audit", nil); err != nil {
			failed.Add(1)
		}
		handled.Add(1)
		return nil
	}))
	// More registrations keep Start busy after billing has begun receiving.
	for i := range 10 {
		name := fmt.Sprintf("extra%d", i)
		a.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: name, AckPolicy: jetstream.AckExplicitPolicy})
		a.Consume("ORDERS", name, Typed(func(context.Context, *Msg[order]) error { return nil }))
	}
	start(t, a)
	waitFor(t, func() bool { return handled.Load() == 20 })
	if failed.Load() != 0 {
		t.Fatalf("%d publishes failed during startup", failed.Load())
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

// A publish middleware that short-circuits must not panic Request.
func TestRequestShortCircuitMiddlewareDoesNotPanic(t *testing.T) {
	a := newApp(t, runServer(t))
	a.UsePublish(func(PublishFunc) PublishFunc {
		return func(context.Context, *RawMsg) error { return nil }
	})
	start(t, a)
	if _, err := a.Request(context.Background(), "x", 1, new(int)); err == nil {
		t.Fatal("want error when no reply was produced")
	}
}
