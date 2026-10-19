package nats

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

func TestHandlerOptionRules(t *testing.T) {
	if _, err := newHandlerConfig(kindOrdered, []HandlerOption{WithManualAck()}); err == nil {
		t.Fatal("WithManualAck not allowed on ordered")
	}
	if _, err := newHandlerConfig(kindConsume, []HandlerOption{WithQueue("q")}); err == nil {
		t.Fatal("WithQueue not allowed on consume")
	}
	if _, err := newHandlerConfig(kindSubscribe, []HandlerOption{WithConcurrency(0)}); err == nil {
		t.Fatal("WithConcurrency(0) invalid")
	}
	c, err := newHandlerConfig(kindSubscribe, nil)
	if err != nil || c.concurrency != 1 {
		t.Fatal(c, err)
	}
}

func TestDLQSubjectValidation(t *testing.T) {
	for subj, ok := range map[string]bool{"dlq.x": true, "dlq.a*b": true, "dlq.*": false, "dlq.>": false, "dlq..x": false, "dl q": false, ".dlq": false} {
		_, err := newHandlerConfig(kindSubscribe, []HandlerOption{WithDLQ(subj)})
		if ok && err != nil {
			t.Errorf("%q: unexpected error %v", subj, err)
		}
		if !ok && (err == nil || !strings.Contains(err.Error(), "WithDLQ")) {
			t.Errorf("%q: want error naming WithDLQ, got %v", subj, err)
		}
	}
}

func TestOptionValueValidation(t *testing.T) {
	if _, err := newHandlerConfig(kindConsume, []HandlerOption{WithPullMaxMessages(0)}); err == nil || !strings.Contains(err.Error(), "WithPullMaxMessages") {
		t.Errorf("WithPullMaxMessages(0): got %v", err)
	}
	if _, err := newHandlerConfig(kindSubscribe, []HandlerOption{WithTimeout(-time.Second)}); err == nil || !strings.Contains(err.Error(), "WithTimeout") {
		t.Errorf("WithTimeout(-1s): got %v", err)
	}
	if _, err := newHandlerConfig(kindSubscribe, []HandlerOption{WithTimeout(0)}); err != nil {
		t.Errorf("WithTimeout(0): %v", err)
	}
}

func TestPullMaxMessagesOnPushConsumer(t *testing.T) {
	a := newApp(t, runServer(t))
	a.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	a.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "push", DeliverSubject: "push.billing", AckPolicy: jetstream.AckExplicitPolicy})
	a.Consume("ORDERS", "push", Typed(func(context.Context, *Msg[order]) error { return nil }), WithPullMaxMessages(10))
	if err := a.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "WithPullMaxMessages") {
		t.Fatal("want Start error naming WithPullMaxMessages, got", err)
	}
}

func TestOptionErrorsUseNewNames(t *testing.T) {
	s := runServer(t)
	noop := Typed(func(context.Context, *Msg[[]byte]) error { return nil })
	batch := TypedBatch(func(context.Context, []*Msg[[]byte]) error { return nil })
	cases := map[string]struct {
		register func(a *App)
		want     string
	}{
		"handler kind": {func(a *App) { a.Subscribe("x", noop, WithManualAck()) }, "WithManualAck"},
		"max attempts": {func(a *App) { a.Subscribe("x", noop, WithMaxAttempts(0)) }, "WithMaxAttempts"},
		"dlq empty":    {func(a *App) { a.Subscribe("x", noop, WithDLQ("")) }, "WithDLQ"},
		"batch size": {func(a *App) {
			setupStream(t, a)
			a.ConsumeBatch("ORDERS", "billing", batch, WithBatchSize(0))
		}, "WithBatchSize"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			a := newApp(t, s)
			c.register(a)
			if err := a.Start(context.Background()); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Start err = %v, want %q", err, c.want)
			}
			_ = a.Shutdown(context.Background())
		})
	}
	a := newApp(t, s)
	start(t, a)
	if err := a.Publish(context.Background(), "x", 1, WithExpectStream("S")); err == nil || !strings.Contains(err.Error(), "WithExpectStream") {
		t.Fatalf("Publish err = %v, want WithExpectStream", err)
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

func TestIdempotencyConfigErrors(t *testing.T) {
	s := runServer(t)
	noop := Typed(func(context.Context, *Msg[[]byte]) error { return nil })
	reply := TypedReply(func(_ context.Context, m *Msg[int]) (int, error) { return m.Data, nil })
	batch := TypedBatch(func(context.Context, []*Msg[[]byte]) error { return nil })
	st := NewMemoryStore()
	cases := map[string]struct {
		register func(a *App)
		want     []string
	}{
		"batch": {func(a *App) {
			setupStream(t, a)
			a.ConsumeBatch("ORDERS", "billing", batch, WithIdempotency(WithIdempotencyStore(st)))
		}, []string{"WithIdempotency not allowed for batch"}},
		"scope without idempotency": {func(a *App) { a.Subscribe("x", noop, WithIdempotencyScope("s")) }, []string{"WithIdempotencyScope requires WithIdempotency"}},
		"empty scope": {func(a *App) {
			a.Subscribe("x", noop, WithIdempotency(WithIdempotencyStore(st)), WithIdempotencyScope(""))
		}, []string{"WithIdempotencyScope: empty scope"}},
		"subscribe without queue": {func(a *App) {
			a.Subscribe("audit.>", noop, WithIdempotency(WithIdempotencyStore(st)))
		}, []string{"WithIdempotencyScope", `"audit.>"`}},
		"reply without queue": {func(a *App) {
			a.SubscribeReply("x", reply, WithIdempotency(WithIdempotencyStore(st)))
		}, []string{"WithIdempotencyScope"}},
		"ordered": {func(a *App) {
			setupStream(t, a)
			a.ConsumeOrdered("ORDERS", noop, WithIdempotency(WithIdempotencyStore(st)))
		}, []string{"WithIdempotencyScope"}},
		"nil store": {func(a *App) {
			a.Subscribe("x", noop, WithQueue("q"), WithIdempotency(WithIdempotencyStore(nil)))
		}, []string{"nil store"}},
		"nil key": {func(a *App) {
			a.Subscribe("x", noop, WithQueue("q"), WithIdempotency(WithIdempotencyStore(st), WithIdempotencyKey(nil)))
		}, []string{"nil key"}},
		"zero claim TTL": {func(a *App) {
			a.Subscribe("x", noop, WithQueue("q"), WithIdempotency(WithIdempotencyStore(st), WithClaimTTL(0)))
		}, []string{"TTL"}},
		"negative done TTL": {func(a *App) {
			a.Subscribe("x", noop, WithQueue("q"), WithIdempotency(WithIdempotencyStore(st), WithDoneTTL(-time.Second)))
		}, []string{"TTL"}},
		"endpoint without queue group": {func(a *App) {
			svc := a.Service(micro.Config{Name: "fan", Version: "1.0.0", QueueGroupDisabled: true})
			svc.Endpoint("get", reply, WithIdempotency(WithIdempotencyStore(st)))
		}, []string{"WithIdempotencyScope", `"fan.get"`}},
		"app nil store":                   {func(a *App) { a.Idempotency(nil) }, []string{"App.Idempotency: nil store"}},
		"app bad option":                  {func(a *App) { a.Idempotency(st, WithDoneTTL(0)) }, []string{"App.Idempotency", "TTL"}},
		"app nil store with store option": {func(a *App) { a.Idempotency(nil, WithIdempotencyStore(st)) }, []string{"App.Idempotency: nil store"}},
		"app twice":                       {func(a *App) { a.Idempotency(st); a.Idempotency(st) }, []string{"App.Idempotency called twice"}},
		"no store at all":                 {func(a *App) { setupStream(t, a); a.Consume("ORDERS", "billing", noop, WithIdempotency()) }, []string{"needs a store", `"ORDERS/billing"`}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			a := newApp(t, s)
			c.register(a)
			err := a.Start(context.Background())
			_ = a.Shutdown(context.Background())
			for _, w := range c.want {
				if err == nil || !strings.Contains(err.Error(), w) {
					t.Fatalf("Start err = %v, want %q", err, w)
				}
			}
		})
	}
}

func TestIdempotencyConfigValid(t *testing.T) {
	s := runServer(t)
	noop := Typed(func(context.Context, *Msg[[]byte]) error { return nil })
	st := NewMemoryStore()
	var buf syncBuf
	a := newApp(t, s, WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
	setupStream(t, a)
	a.Subscribe("x", noop, WithQueue("q"), WithIdempotency(WithIdempotencyStore(st)))
	a.Subscribe("y", noop, WithIdempotency(), WithIdempotencyScope("y-svc"))
	a.Consume("ORDERS", "billing", noop, WithIdempotency())
	a.Idempotency(st) // after the registrations that rely on it
	start(t, a)
	a.Idempotency(NewMemoryStore()) // late: ignored and logged
	if !strings.Contains(buf.String(), "registration ignored") {
		t.Fatalf("late App.Idempotency not logged: %s", buf.String())
	}
	a.Subscribe("late", noop, WithQueue("q"), WithIdempotency()) // late: not recorded
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.idem == nil || a.idem.store != st {
		t.Fatalf("a.idem = %+v, want the store set before Start", a.idem)
	}
	if len(a.idemNoStore) != 2 {
		t.Fatalf("idemNoStore = %q, want only the two handlers registered before Start", a.idemNoStore)
	}
}
