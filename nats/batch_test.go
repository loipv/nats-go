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
	a.ConsumeBatch("ORDERS", "billing", TypedBatch(func(_ context.Context, ms []*Msg[order]) error { sizes <- len(ms); return nil }), WithBatchSize(5), WithBatchWait(200*time.Millisecond))
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
	a.ConsumeBatch("ORDERS", "billing", TypedBatch(func(_ context.Context, ms []*Msg[order]) error {
		be := &BatchError{Failed: map[int]error{}}
		for i, m := range ms {
			seen.Store(m.Data.ID, true)
			if m.Data.ID == "bad" {
				be.Failed[i] = Terminal(errors.New("bad row"))
			}
		}
		return be
	}), WithBatchSize(10), WithBatchWait(200*time.Millisecond), WithDLQ("dlq.orders"))
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
	a.ConsumeBatch("ORDERS", "billing", TypedBatch(func(_ context.Context, ms []*Msg[order]) error {
		var ids []string
		for _, m := range ms {
			ids = append(ids, m.Data.ID)
		}
		got <- ids
		return nil
	}), WithBatchWait(200*time.Millisecond))
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
	a.ConsumeBatch("ORDERS", "billing", TypedBatch(func(context.Context, []*Msg[order]) error {
		if calls.Add(1) == 1 {
			return errors.New("db down")
		}
		return nil
	}), WithBatchWait(200*time.Millisecond), fast)
	start(t, a)
	waitFor(t, func() bool { return calls.Load() >= 2 && pendingAcks(t, a, "billing") == 0 })
}
func TestConsumeBatchRejectsPush(t *testing.T) {
	a := newApp(t, runServer(t))
	a.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	a.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "audit", DeliverSubject: "deliver.audit", AckPolicy: jetstream.AckExplicitPolicy})
	a.ConsumeBatch("ORDERS", "audit", TypedBatch(func(context.Context, []*Msg[order]) error { return nil }))
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
	a.ConsumeBatch("ORDERS", "billing", TypedBatch(func(context.Context, []*Msg[order]) error { started <- struct{}{}; <-release; return nil }), WithBatchWait(100*time.Millisecond))
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
func TestConsumeBatchTypedNilBatchError(t *testing.T) {
	if (*BatchError)(nil).Error() == "" {
		t.Fatal("empty message")
	}
	s := runServer(t)
	seed := newApp(t, s)
	setupStream(t, seed)
	start(t, seed)
	for i := range 3 {
		_, _ = seed.PublishJS(context.Background(), "orders.x", order{ID: fmt.Sprint(i)})
	}
	a := newApp(t, s)
	var calls atomic.Int32
	a.ConsumeBatch("ORDERS", "billing", TypedBatch(func(context.Context, []*Msg[order]) error {
		calls.Add(1)
		var be *BatchError
		return be // a typed nil reports no failures
	}), WithBatchWait(200*time.Millisecond), fast)
	start(t, a)
	waitFor(t, func() bool {
		return calls.Load() >= 1 && pendingAcks(t, a, "billing") == 0 && delivered(t, a, "billing") == 3
	})
}

func TestBatchJoinedBatchErrorFailsListedOnly(t *testing.T) {
	s := runServer(t)
	seed := newApp(t, s)
	setupStream(t, seed)
	start(t, seed)
	for i := range 3 {
		_, _ = seed.PublishJS(context.Background(), "orders.x", order{ID: fmt.Sprint(i)})
	}
	a := newApp(t, s)
	sizes := make(chan int, 10)
	a.ConsumeBatch("ORDERS", "billing", TypedBatch(func(_ context.Context, ms []*Msg[order]) error {
		sizes <- len(ms)
		if len(ms) == 3 {
			return errors.Join(errors.New("boom"), &BatchError{Failed: map[int]error{0: errors.New("x")}})
		}
		return nil
	}), WithBatchSize(10), WithBatchWait(200*time.Millisecond), WithBackoff(func(int) time.Duration { return 10 * time.Millisecond }))
	start(t, a)
	if n := recv(t, sizes); n != 3 {
		t.Fatal("first batch", n)
	}
	if n := recv(t, sizes); n != 1 { // only index 0 is redelivered
		t.Fatal("second batch", n)
	}
	waitFor(t, func() bool { return pendingAcks(t, a, "billing") == 0 })
}

func TestBatchJoinedNilBatchErrorFails(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	var calls atomic.Int32
	a.ConsumeBatch("ORDERS", "billing", TypedBatch(func(context.Context, []*Msg[order]) error {
		if calls.Add(1) == 1 {
			return errors.Join(errors.New("boom"), (*BatchError)(nil))
		}
		return nil
	}), WithBatchWait(200*time.Millisecond), WithBackoff(func(int) time.Duration { return 10 * time.Millisecond }))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	waitFor(t, func() bool { return calls.Load() == 2 && pendingAcks(t, a, "billing") == 0 })
}
