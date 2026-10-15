package nats

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Test helpers shared by the retry and DLQ tests.
var (
	fast     = WithBackoff(func(int) time.Duration { return 50 * time.Millisecond })
	nopOrder = func(context.Context, *Msg[order]) error { return nil }
)

func setupDLQ(t *testing.T, a *App) {
	t.Helper()
	a.Stream(jetstream.StreamConfig{Name: "DLQ", Subjects: []string{"dlq.>"}})
}

// dlqMsgs reads every message of the DLQ stream.
func dlqMsgs(t *testing.T, a *App) []*RawMsg {
	t.Helper()
	c, err := a.JetStream().OrderedConsumer(context.Background(), "DLQ", jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := c.Fetch(100, jetstream.FetchMaxWait(500*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	var out []*RawMsg
	for m := range batch.Messages() {
		out = append(out, toRaw(m))
	}
	return out
}

func TestConsumeRetriesThenDLQ(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	setupDLQ(t, a)
	var calls atomic.Int32
	Consume(a, "ORDERS", "billing", func(_ context.Context, m *Msg[order]) error {
		calls.Add(1)
		return errors.New("db down")
	}, MaxAttempts(3), fast, DLQ("dlq.orders"))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"}, MsgID("m-1"))
	waitFor(t, func() bool { return len(dlqMsgs(t, a)) == 1 })
	d := dlqMsgs(t, a)[0]
	if calls.Load() != 3 || string(d.Data) != `{"id":"1"}` {
		t.Fatal(calls.Load(), string(d.Data))
	}
	for k, v := range map[string]string{"Nats-DLQ-Error": "db down", "Nats-DLQ-Subject": "orders.x", "Nats-DLQ-Stream": "ORDERS",
		"Nats-DLQ-Consumer": "billing", "Nats-DLQ-Sequence": "1", "Nats-DLQ-Deliveries": "3", "Nats-DLQ-Msg-Id": "m-1"} {
		if d.Header.Get(k) != v {
			t.Fatalf("%s = %q want %q", k, d.Header.Get(k), v)
		}
	}
	waitFor(t, func() bool { return pendingAcks(t, a, "billing") == 0 })
}

func TestConsumeTerminalGoesToDLQWithoutRetry(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	setupDLQ(t, a)
	var calls atomic.Int32
	Consume(a, "ORDERS", "billing", func(context.Context, *Msg[order]) error { calls.Add(1); return Terminal(errors.New("bad")) }, DLQ("dlq.orders"))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", []byte("{not json"))
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "2"})
	waitFor(t, func() bool { return len(dlqMsgs(t, a)) == 2 })
	if calls.Load() != 1 {
		t.Fatal("decode failure must not call handler; terminal must not retry", calls.Load())
	}
}

func TestConsumeDefaultBackoffDelaysRedelivery(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	times := make(chan time.Time, 2)
	var calls atomic.Int32
	Consume(a, "ORDERS", "billing", func(context.Context, *Msg[order]) error {
		times <- time.Now()
		if calls.Add(1) == 1 {
			return errors.New("retry")
		}
		return nil
	})
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	t1 := recv(t, times)
	t2 := recv(t, times)
	if d := t2.Sub(t1); d < 700*time.Millisecond {
		t.Fatal("default backoff ~1s expected, got", d)
	}
}

func TestConsumeExhaustedWithoutDLQTerms(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	var calls atomic.Int32
	Consume(a, "ORDERS", "billing", func(context.Context, *Msg[order]) error { calls.Add(1); return errors.New("x") }, MaxAttempts(2), fast)
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	time.Sleep(time.Second)
	if calls.Load() != 2 || pendingAcks(t, a, "billing") != 0 {
		t.Fatal(calls.Load())
	}
}

func TestConsumeExceededSkipsHandler(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	setupDLQ(t, a)
	// AckWait 500ms (setupStream): a handler that hangs past AckWait gets redelivered.
	var calls atomic.Int32
	Consume(a, "ORDERS", "billing", func(ctx context.Context, _ *Msg[order]) error {
		calls.Add(1)
		time.Sleep(700 * time.Millisecond)
		return errors.New("slow")
	}, MaxAttempts(1), DLQ("dlq.orders"), Concurrency(2))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	waitFor(t, func() bool { return len(dlqMsgs(t, a)) >= 1 })
	time.Sleep(500 * time.Millisecond)
	if calls.Load() != 1 || len(dlqMsgs(t, a)) != 1 {
		t.Fatal("redelivery past MaxAttempts must skip handler and DLQ once", calls.Load(), len(dlqMsgs(t, a)))
	}
}

func TestDLQPublishFailureNaks(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	setupDLQ(t, a)
	var calls atomic.Int32
	Consume(a, "ORDERS", "billing", func(context.Context, *Msg[order]) error { calls.Add(1); return Terminal(errors.New("bad")) }, DLQ("dlq.orders"), fast)
	start(t, a)
	_ = a.JetStream().DeleteStream(context.Background(), "DLQ") // DLQ publish now fails
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	waitFor(t, func() bool { return delivered(t, a, "billing") >= 2 }) // Nak'd, not Term'd
	_, _ = a.JetStream().CreateStream(context.Background(), jetstream.StreamConfig{Name: "DLQ", Subjects: []string{"dlq.>"}})
	waitFor(t, func() bool { return len(dlqMsgs(t, a)) == 1 && pendingAcks(t, a, "billing") == 0 })
}

func TestStartChecks(t *testing.T) {
	cases := map[string]func(a *App){
		"no DLQ stream":        func(a *App) { Consume(a, "ORDERS", "billing", nopOrder, DLQ("nowhere.x")) },
		"DLQ in source stream": func(a *App) { Consume(a, "ORDERS", "billing", nopOrder, DLQ("orders.dlq")) }, // Review Focus #1
		"MaxDeliver too low":   func(a *App) { Consume(a, "ORDERS", "billing", nopOrder, MaxAttempts(5)) },    // setupStream MaxDeliver 5
		"core DLQ no stream": func(a *App) {
			Subscribe(a, "x", func(context.Context, *Msg[[]byte]) error { return nil }, DLQ("nowhere.x"))
		},
		// orders.dlq is captured by ORDERS, so only the subject match can fail Start.
		"subscribe DLQ matches own subject": func(a *App) {
			Subscribe(a, "orders.*", func(context.Context, *Msg[[]byte]) error { return nil }, DLQ("orders.dlq"))
		},
		"reply DLQ matches own subject": func(a *App) {
			Reply(a, "orders.>", func(context.Context, *Msg[[]byte]) ([]byte, error) { return nil, nil }, DLQ("orders.dlq"))
		},
	}
	for name, add := range cases {
		t.Run(name, func(t *testing.T) {
			a := newApp(t, runServer(t))
			setupStream(t, a)
			add(a)
			if err := a.Start(context.Background()); err == nil {
				t.Fatal("want Start error")
			}
		})
	}
}
