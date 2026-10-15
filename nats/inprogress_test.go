package nats

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestAutoInProgressPreventsRedelivery(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a) // AckWait 500ms
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error {
		calls.Add(1)
		time.Sleep(1500 * time.Millisecond)
		return nil
	}), WithAutoInProgress(), WithConcurrency(2))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	waitFor(t, func() bool { return calls.Load() >= 1 })
	time.Sleep(2 * time.Second)
	if calls.Load() != 1 || delivered(t, a, "billing") != 1 {
		t.Fatal(calls.Load(), delivered(t, a, "billing"))
	}
}

// inProgressFails is a message whose InProgress always fails.
type inProgressFails struct{ jetstream.Msg }

func (inProgressFails) Subject() string   { return "s" }
func (inProgressFails) InProgress() error { return errors.New("gone") }

func TestKeepAliveFailureEmits(t *testing.T) {
	got := make(chan AsyncErrorEvent, 1)
	log := &sink{Logger: slog.New(slog.DiscardHandler), handler: "h", ev: &events{asyncError: []func(AsyncErrorEvent){func(e AsyncErrorEvent) {
		select {
		case got <- e:
		default:
		}
	}}}}
	stop := keepAlive([]jetstream.Msg{inProgressFails{}}, time.Millisecond, log)
	defer stop()
	e := recv(t, got)
	if e.Handler != "h" || e.Subject != "s" || e.Err.Error() != "in-progress: gone" {
		t.Fatalf("event = %+v", e)
	}
}
