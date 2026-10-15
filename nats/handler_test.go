package nats

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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

func TestNilHandlerIsRegistrationError(t *testing.T) {
	s := runServer(t)
	var nilFn func(context.Context, *Msg[string]) error
	var nilReplyFn func(context.Context, *Msg[string]) (string, error)
	var nilBatchFn func(context.Context, []*Msg[string]) error
	cases := map[string]func(a *App){
		"subscribe zero":  func(a *App) { a.Subscribe("x", MsgHandler{}) },
		"subscribe nilfn": func(a *App) { a.Subscribe("x", Typed(nilFn)) },
		"reply zero":      func(a *App) { a.SubscribeReply("x", ReplyHandler{}) },
		"reply nilfn":     func(a *App) { a.SubscribeReply("x", TypedReply(nilReplyFn)) },
		"consume zero":    func(a *App) { a.Consume("ORDERS", "billing", MsgHandler{}) },
		"ordered zero":    func(a *App) { a.ConsumeOrdered("ORDERS", MsgHandler{}) },
		"batch zero":      func(a *App) { a.ConsumeBatch("ORDERS", "billing", BatchHandler{}) },
		"batch nilfn":     func(a *App) { a.ConsumeBatch("ORDERS", "billing", TypedBatch(nilBatchFn)) },
	}
	for name, register := range cases {
		t.Run(name, func(t *testing.T) {
			a := newApp(t, s)
			setupStream(t, a)
			register(a)
			err := a.Start(context.Background())
			if err == nil || !strings.Contains(err.Error(), "nil handler") {
				t.Fatalf("Start err = %v, want nil handler error", err)
			}
			_ = a.Shutdown(context.Background())
		})
	}
}

func TestTypedHandlerReused(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	var mu sync.Mutex
	seen := map[string]int{}
	h := Typed(func(_ context.Context, m *Msg[string]) error {
		mu.Lock()
		seen[m.Subject]++
		mu.Unlock()
		return nil
	})
	a.Subscribe("reuse.core", h)
	a.Consume("ORDERS", "billing", h)
	start(t, a)
	if err := a.Publish(context.Background(), "reuse.core", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.PublishJS(context.Background(), "orders.reuse", "b"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return seen["reuse.core"] == 1 && seen["orders.reuse"] == 1
	})
}

func TestAttemptOutsideHandler(t *testing.T) {
	if Attempt(context.Background()) != 1 || Attempt(withAttempt(context.Background(), 3)) != 3 {
		t.Fatal()
	}
}
