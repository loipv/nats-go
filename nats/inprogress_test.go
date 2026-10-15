package nats

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
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
