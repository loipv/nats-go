package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestConsumeBatchAcksAll(t *testing.T) {
	s := runServer(t)
	seed := newApp(t, s)
	setupStream(t, seed)
	start(t, seed)
	for i := range 12 {
		_, _ = seed.PublishJS(context.Background(), "orders.x", order{ID: fmt.Sprint(i)})
	} // backlog
	a := newApp(t, s)
	sizes := make(chan int, 10)
	ConsumeBatch(a, "ORDERS", "billing", func(_ context.Context, ms []*Msg[order]) error { sizes <- len(ms); return nil }, BatchSize(5), BatchWait(200*time.Millisecond))
	start(t, a)
	total := 0
	for total < 12 {
		n := recv(t, sizes)
		if n > 5 {
			t.Fatal("batch too big", n)
		}
		total += n
	}
	waitFor(t, func() bool { return pendingAcks(t, a, "billing") == 0 })
}
func TestConsumeBatchPartialFailure(t *testing.T) {
	s := runServer(t)
	seed := newApp(t, s)
	setupStream(t, seed)
	setupDLQ(t, seed)
	start(t, seed)
	for _, id := range []string{"ok1", "bad", "ok2"} {
		_, _ = seed.PublishJS(context.Background(), "orders.x", order{ID: id})
	}
	a := newApp(t, s)
	var seen sync.Map
	ConsumeBatch(a, "ORDERS", "billing", func(_ context.Context, ms []*Msg[order]) error {
		be := &BatchError{Failed: map[int]error{}}
		for i, m := range ms {
			seen.Store(m.Data.ID, true)
			if m.Data.ID == "bad" {
				be.Failed[i] = Terminal(errors.New("bad row"))
			}
		}
		return be
	}, BatchSize(10), BatchWait(200*time.Millisecond), DLQ("dlq.orders"))
	start(t, a)
	waitFor(t, func() bool { return len(dlqMsgs(t, a)) == 1 && pendingAcks(t, a, "billing") == 0 })
	if dlqMsgs(t, a)[0].Header.Get("Nats-DLQ-Error") != "bad row" {
		t.Fatal()
	}
}
func TestConsumeBatchFiltersPoison(t *testing.T) {
	s := runServer(t)
	seed := newApp(t, s)
	setupStream(t, seed)
	start(t, seed)
	_, _ = seed.PublishJS(context.Background(), "orders.x", []byte("{not json"))
	_, _ = seed.PublishJS(context.Background(), "orders.x", order{ID: "good"})
	a := newApp(t, s)
	got := make(chan []string, 2)
	ConsumeBatch(a, "ORDERS", "billing", func(_ context.Context, ms []*Msg[order]) error {
		var ids []string
		for _, m := range ms {
			ids = append(ids, m.Data.ID)
		}
		got <- ids
		return nil
	}, BatchWait(200*time.Millisecond))
	start(t, a)
	if ids := recv(t, got); len(ids) != 1 || ids[0] != "good" {
		t.Fatal(ids)
	}
	waitFor(t, func() bool { return pendingAcks(t, a, "billing") == 0 })
}
func TestConsumeBatchWholeBatchRetry(t *testing.T) {
	s := runServer(t)
	seed := newApp(t, s)
	setupStream(t, seed)
	start(t, seed)
	for i := range 3 {
		_, _ = seed.PublishJS(context.Background(), "orders.x", order{ID: fmt.Sprint(i)})
	}
	a := newApp(t, s)
	var calls atomic.Int32
	ConsumeBatch(a, "ORDERS", "billing", func(context.Context, []*Msg[order]) error {
		if calls.Add(1) == 1 {
			return errors.New("db down")
		}
		return nil
	}, BatchWait(200*time.Millisecond), fast)
	start(t, a)
	waitFor(t, func() bool { return calls.Load() >= 2 && pendingAcks(t, a, "billing") == 0 })
}
func TestConsumeBatchRejectsPush(t *testing.T) {
	a := newApp(t, runServer(t))
	a.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	a.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "audit", DeliverSubject: "deliver.audit", AckPolicy: jetstream.AckExplicitPolicy})
	ConsumeBatch(a, "ORDERS", "audit", func(context.Context, []*Msg[order]) error { return nil })
	if err := a.Start(context.Background()); err == nil {
		t.Fatal("push must be rejected")
	}
}
func TestConsumeBatchShutdownDeadline(t *testing.T) { // Review Focus #4
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	started, release := make(chan struct{}, 1), make(chan struct{})
	defer close(release)
	ConsumeBatch(a, "ORDERS", "billing", func(context.Context, []*Msg[order]) error { started <- struct{}{}; <-release; return nil }, BatchWait(100*time.Millisecond))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	begin := time.Now()
	if err := a.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) || time.Since(begin) > time.Second {
		t.Fatal(err, time.Since(begin))
	}
}
