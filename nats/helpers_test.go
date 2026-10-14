package nats

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
)

func runServer(t *testing.T) *server.Server {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := natstest.RunServer(&opts)
	t.Cleanup(s.Shutdown)
	return s
}

func newApp(t *testing.T, s *server.Server, opts ...Option) *App {
	t.Helper()
	a, err := New(append([]Option{WithURL(s.ClientURL())}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func start(t *testing.T, a *App) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = a.Shutdown(ctx)
	})
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("nothing received within 2s")
		var zero T
		return zero
	}
}

func expectNone[T any](t *testing.T, ch <-chan T, d time.Duration) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected value %v", v)
	case <-time.After(d):
	}
}
