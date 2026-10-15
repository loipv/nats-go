package nats

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestSubscribePanicWithoutRecoverDoesNotCrash(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	got := make(chan string, 2)
	a.Subscribe("p", Typed(func(_ context.Context, m *Msg[string]) error {
		if m.Data == "boom" {
			panic("boom")
		}
		got <- m.Data
		return nil
	}))
	start(t, a)
	_ = a.Publish(context.Background(), "p", "boom")
	_ = a.Publish(context.Background(), "p", "ok")
	if recv(t, got) != "ok" {
		t.Fatal()
	}
}

func TestConsumePanicWithoutRecoverIsRetried(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(_ context.Context, m *Msg[order]) error {
		if calls.Add(1) == 1 {
			panic("first try")
		}
		return nil
	}), WithBackoff(func(int) time.Duration { return 50 * time.Millisecond }))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	waitFor(t, func() bool { return calls.Load() == 2 && pendingAcks(t, a, "billing") == 0 })
}

func TestAttemptCore(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	got := make(chan int, 1)
	a.Subscribe("a", Typed(func(_ context.Context, m *Msg[[]byte]) error { got <- m.Attempt(); return nil }))
	start(t, a)
	_ = a.Conn().Publish("a", nil)
	if recv(t, got) != 1 {
		t.Fatal()
	}
}
