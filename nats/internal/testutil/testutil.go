// Package testutil holds test helpers shared by the integration subpackages.
// Import it from _test.go files only.
package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"

	"github.com/loipv/nats-go/nats"
)

// RunServer starts an embedded JetStream server on a random port.
func RunServer(t *testing.T) *server.Server {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := natstest.RunServer(&opts)
	t.Cleanup(s.Shutdown)
	return s
}

// NewApp creates an App for url and applies setup before returning it.
func NewApp(t *testing.T, url string, setup ...func(*nats.App)) *nats.App {
	t.Helper()
	a, err := nats.New(nats.WithURL(url))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range setup {
		f(a)
	}
	return a
}

// Start starts a and shuts it down when the test ends.
func Start(t *testing.T, a *nats.App) {
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

// Recv returns the next value of ch or fails after 2s.
func Recv[T any](t *testing.T, ch <-chan T) T {
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

// WaitFor polls cond until it holds or fails after 5s.
func WaitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
