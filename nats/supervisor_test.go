package nats

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var restartFast = WithRestartBackoff(func(int) time.Duration { return 100 * time.Millisecond })

func TestSupervisorRecreatesDeclaredConsumer(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s, restartFast)
	setupStream(t, a)
	got := make(chan string, 2)
	a.Consume("ORDERS", "billing", Typed(func(_ context.Context, m *Msg[order]) error { got <- m.Data.ID; return nil }))
	start(t, a)
	_ = a.JetStream().DeleteConsumer(context.Background(), "ORDERS", "billing")
	waitFor(t, func() bool { h := a.Health(); return h.Status == "ok" && h.Handlers[0].Restarts == 1 })
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "after"})
	if recv(t, got) != "after" {
		t.Fatal()
	}
}

func TestSupervisorWaitsForUndeclaredConsumer(t *testing.T) {
	s := runServer(t)
	seed := newApp(t, s)
	setupStream(t, seed)
	start(t, seed)
	a := newApp(t, s, restartFast)
	a.Consume("ORDERS", "billing", Typed(nopOrder))
	start(t, a)
	_ = a.JetStream().DeleteConsumer(context.Background(), "ORDERS", "billing")
	waitFor(t, func() bool { return a.Health().Handlers[0].State == "restarting" })
	time.Sleep(300 * time.Millisecond)
	if a.Health().Status != "degraded" {
		t.Fatal(a.Health())
	}
	_, _ = a.JetStream().CreateOrUpdateConsumer(context.Background(), "ORDERS", jetstream.ConsumerConfig{Durable: "billing", AckPolicy: jetstream.AckExplicitPolicy})
	waitFor(t, func() bool { return a.Health().Status == "ok" })
}

func TestSupervisorBatch(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s, restartFast)
	setupStream(t, a)
	a.ConsumeBatch("ORDERS", "billing", TypedBatch(func(context.Context, []*Msg[order]) error { return nil }), BatchWait(100*time.Millisecond))
	start(t, a)
	_ = a.JetStream().DeleteConsumer(context.Background(), "ORDERS", "billing")
	waitFor(t, func() bool { h := a.Health(); return h.Status == "ok" && h.Handlers[0].Restarts == 1 })
}

// The server tells push consumers nothing when they are deleted; with
// IdleHeartbeat set, the missed heartbeats are confirmed with a lookup.
func TestSupervisorPushConsumer(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s, restartFast)
	a.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	a.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "push", DeliverSubject: "deliver.push", AckPolicy: jetstream.AckExplicitPolicy, IdleHeartbeat: 200 * time.Millisecond})
	got := make(chan string, 2)
	a.Consume("ORDERS", "push", Typed(func(_ context.Context, m *Msg[order]) error { got <- m.Data.ID; return nil }))
	start(t, a)
	_ = a.JetStream().DeleteConsumer(context.Background(), "ORDERS", "push")
	waitFor(t, func() bool { h := a.Health(); return h.Status == "ok" && h.Handlers[0].Restarts == 1 })
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "after"})
	if recv(t, got) != "after" {
		t.Fatal()
	}
}

// With no pull request waiting when the consumer is deleted, the server
// never says so; the next pull gets no responders, confirmed with a lookup.
func TestSupervisorPullWithoutWaitingRequest(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s, restartFast)
	setupStream(t, a)
	block := make(chan struct{})
	got := make(chan string, 8)
	a.Consume("ORDERS", "billing", Typed(func(_ context.Context, m *Msg[order]) error { got <- m.Data.ID; <-block; return nil }), PullMaxMessages(1))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "2"})
	recv(t, got)
	time.Sleep(200 * time.Millisecond) // "2" waits for the busy worker, fulfilling the only pull request
	_ = a.JetStream().DeleteConsumer(context.Background(), "ORDERS", "billing")
	close(block)
	waitFor(t, func() bool { h := a.Health(); return h.Status == "ok" && h.Handlers[0].Restarts == 1 })
}

// A deleted stream takes its consumers with it; the lookup that confirms
// the deletion then reports the stream, not the consumer, as missing.
func TestSupervisorRecreatesDeletedStream(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s, restartFast)
	setupStream(t, a)
	block := make(chan struct{})
	got := make(chan string, 8)
	a.Consume("ORDERS", "billing", Typed(func(_ context.Context, m *Msg[order]) error { got <- m.Data.ID; <-block; return nil }), PullMaxMessages(1))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "2"})
	recv(t, got)
	time.Sleep(200 * time.Millisecond) // "2" waits for the busy worker, fulfilling the only pull request
	_ = a.JetStream().DeleteStream(context.Background(), "ORDERS")
	close(block)
	waitFor(t, func() bool { h := a.Health(); return h.Status == "ok" && h.Handlers[0].Restarts == 1 })
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "after"})
	for recv(t, got) != "after" {
	}
}

func TestShutdownDuringRestartBackoff(t *testing.T) { // Review Focus #5
	s := runServer(t)
	seed := newApp(t, s)
	setupStream(t, seed)
	start(t, seed)
	a := newApp(t, s, WithRestartBackoff(func(int) time.Duration { return time.Hour }))
	a.Consume("ORDERS", "billing", Typed(nopOrder))
	start(t, a)
	_ = a.JetStream().DeleteConsumer(context.Background(), "ORDERS", "billing")
	waitFor(t, func() bool { return a.Health().Handlers[0].State == "restarting" })
	begin := time.Now()
	if err := a.Shutdown(context.Background()); err != nil || time.Since(begin) > time.Second {
		t.Fatal(err, time.Since(begin))
	}
}

func TestWithRestartBackoffNil(t *testing.T) {
	if _, err := New(WithRestartBackoff(nil)); err == nil {
		t.Fatal("want error")
	}
}
