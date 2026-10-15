package nats

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
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
	if err := a.Publish(context.Background(), "x", 1, WithMsgID("1")); err == nil || !strings.Contains(err.Error(), "WithMsgID") {
		t.Fatalf("Publish err = %v, want WithMsgID", err)
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
