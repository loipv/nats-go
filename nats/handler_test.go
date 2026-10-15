package nats

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type fakeJSMsg struct {
	jetstream.Msg
	called string
	delay  time.Duration
}

func (f *fakeJSMsg) Ack() error  { f.called = "ack"; return nil }
func (f *fakeJSMsg) Term() error { f.called = "term"; return nil }
func (f *fakeJSMsg) NakWithDelay(d time.Duration) error {
	f.called, f.delay = "nak", d
	return nil
}

// TestDispose covers the decision table rows without a DLQ; the DLQ rows
// need a server and are covered in dlq_test.go.
func TestDispose(t *testing.T) {
	rc := retryConfig{maxAttempts: 3, backoff: func(n int) time.Duration { return time.Duration(n) * time.Second }}
	cases := []struct {
		err     error
		attempt int
		want    string
	}{
		{nil, 1, "ack"},
		{errors.New("x"), 2, "nak"},
		{errors.New("x"), 3, "term"},
		{errMaxAttempts, 4, "term"},
		{Terminal(errors.New("x")), 1, "term"},
	}
	log := slog.New(slog.DiscardHandler)
	for _, c := range cases {
		f := &fakeJSMsg{}
		(&App{}).dispose(context.Background(), f, &RawMsg{}, c.err, rc, dlqSource{deliveries: c.attempt}, log)
		if f.called != c.want {
			t.Fatalf("%v attempt %d: got %q want %q", c.err, c.attempt, f.called, c.want)
		}
		if f.called == "nak" && f.delay != time.Duration(c.attempt)*time.Second {
			t.Fatal("nak delay", f.delay)
		}
	}
}

type order struct {
	ID string `json:"id"`
}

func (o order) Validate() error {
	if o.ID == "" {
		return errors.New("id required")
	}
	return nil
}

func TestTypedDecodeAndValidate(t *testing.T) {
	var got order
	h := typed(JSONCodec{}, func(_ context.Context, m *Msg[order]) error { got = m.Data; return nil })
	if err := h(context.Background(), &RawMsg{Subject: "o", Data: []byte(`{"id":"1"}`)}); err != nil || got.ID != "1" {
		t.Fatal(err, got)
	}
	if err := h(context.Background(), &RawMsg{Data: []byte(`{`)}); !IsTerminal(err) {
		t.Fatal("decode error must be terminal", err)
	}
	if err := h(context.Background(), &RawMsg{Data: []byte(`{}`)}); !IsTerminal(err) {
		t.Fatal("validate error must be terminal", err)
	}
}

func TestMsgAckOnCoreReturnsErrNotJetStream(t *testing.T) {
	if err := (&Msg[[]byte]{}).Ack(); !errors.Is(err, ErrNotJetStream) {
		t.Fatal(err)
	}
}

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

func TestPoolConcurrency(t *testing.T) {
	var cur, peak atomic.Int32
	p := newPool(3, func(context.Context, *RawMsg) {
		n := cur.Add(1)
		for {
			o := peak.Load()
			if n <= o || peak.CompareAndSwap(o, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		cur.Add(-1)
	})
	for range 9 {
		p.dispatch(context.Background(), &RawMsg{})
	}
	_ = p.close(context.Background())
	if peak.Load() != 3 {
		t.Fatal("peak", peak.Load())
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
