package nats

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEventOptionRejectsNil(t *testing.T) {
	cases := map[string]Option{
		"WithOnRetry":             WithOnRetry(nil),
		"WithOnFailed":            WithOnFailed(nil),
		"WithOnDeadLettered":      WithOnDeadLettered(nil),
		"WithOnDeadLetterFailed":  WithOnDeadLetterFailed(nil),
		"WithOnHandlerRestarting": WithOnHandlerRestarting(nil),
		"WithOnHandlerRestarted":  WithOnHandlerRestarted(nil),
		"WithOnHandlerStopped":    WithOnHandlerStopped(nil),
		"WithOnDisconnected":      WithOnDisconnected(nil),
		"WithOnReconnected":       WithOnReconnected(nil),
		"WithOnClosed":            WithOnClosed(nil),
		"WithOnAsyncError":        WithOnAsyncError(nil),
	}
	for name, opt := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := New(opt)
			if err == nil || !strings.Contains(err.Error(), name+": nil callback") {
				t.Fatalf("err = %v, want %q: nil callback", err, name)
			}
		})
	}
}

func TestFireOrderAndPanic(t *testing.T) {
	var buf syncBuf
	log := slog.New(slog.NewTextHandler(&buf, nil))
	var got []int
	fns := []func(MsgEvent){
		func(MsgEvent) { panic("boom") },
		func(MsgEvent) { got = append(got, 1) },
		func(MsgEvent) { got = append(got, 2) },
	}
	fire(log, fns, MsgEvent{Subject: "s"})
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("got %v, want [1 2]", got)
	}
	if !strings.Contains(buf.String(), "nats: event callback panicked") {
		t.Fatalf("log = %q, want panic message", buf.String())
	}
}

func TestEventOptionsAppend(t *testing.T) {
	a, err := New(WithOnClosed(func() {}), WithOnClosed(func() {}))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(a.opts.events.closed); n != 2 {
		t.Fatalf("len(closed) = %d, want 2", n)
	}
}

// collect returns a callback that records its events and a getter for them.
func collect[E any]() (func(E), func() []E) {
	var mu sync.Mutex
	var got []E
	return func(e E) {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, e)
		}, func() []E {
			mu.Lock()
			defer mu.Unlock()
			return append([]E(nil), got...)
		}
}

func TestEventConsumeRetryThenDeadLettered(t *testing.T) {
	s := runServer(t)
	onRetry, retries := collect[MsgEvent]()
	onDLQ, dead := collect[MsgEvent]()
	a := newApp(t, s, WithOnRetry(onRetry), WithOnDeadLettered(onDLQ))
	setupStream(t, a)
	setupDLQ(t, a)
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error {
		return errors.New("db down")
	}), WithMaxAttempts(3), fast, WithDLQ("dlq.orders"))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	waitFor(t, func() bool { return len(dead()) == 1 })
	rs := retries()
	if len(rs) != 2 {
		t.Fatalf("retries = %d, want 2", len(rs))
	}
	for i, e := range rs {
		if e.Attempt != i+1 || e.Handler != "ORDERS/billing" || e.Subject != "orders.x" || e.Err == nil || e.Err.Error() != "db down" || e.Msg == nil {
			t.Fatalf("retry %d = %+v", i, e)
		}
	}
	if d := dead()[0]; d.Attempt != 3 || d.Handler != "ORDERS/billing" || d.Err == nil || d.Msg == nil {
		t.Fatalf("dead-lettered = %+v", d)
	}
}

func TestEventConsumeFailedWithoutDLQ(t *testing.T) {
	s := runServer(t)
	onRetry, retries := collect[MsgEvent]()
	onFailed, failed := collect[MsgEvent]()
	a := newApp(t, s, WithOnRetry(onRetry), WithOnFailed(onFailed))
	setupStream(t, a)
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error {
		return Terminal(errors.New("bad"))
	}))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	waitFor(t, func() bool { return len(failed()) == 1 })
	if e := failed()[0]; e.Attempt != 1 || e.Handler != "ORDERS/billing" || e.Subject != "orders.x" {
		t.Fatalf("failed = %+v", e)
	}
	if n := len(retries()); n != 0 {
		t.Fatalf("retries = %d, want 0", n)
	}
}

func TestEventSubscribeRetryThenFailed(t *testing.T) {
	s := runServer(t)
	onRetry, retries := collect[MsgEvent]()
	onFailed, failed := collect[MsgEvent]()
	a := newApp(t, s, WithOnRetry(onRetry), WithOnFailed(onFailed))
	a.Subscribe("s", Typed(func(context.Context, *Msg[[]byte]) error { return errors.New("nope") }), WithMaxAttempts(2), fast)
	start(t, a)
	_ = a.Conn().Publish("s", nil)
	waitFor(t, func() bool { return len(failed()) == 1 })
	rs := retries()
	if len(rs) != 1 || rs[0].Attempt != 1 || rs[0].Handler != "s" {
		t.Fatalf("retries = %+v", rs)
	}
	if e := failed()[0]; e.Attempt != 2 || e.Handler != "s" {
		t.Fatalf("failed = %+v", e)
	}
}

func TestEventDeadLetterFailed(t *testing.T) {
	s := runServer(t)
	onDLF, dlf := collect[MsgEvent]()
	a := newApp(t, s, WithOnDeadLetterFailed(onDLF))
	setupDLQ(t, a)
	handlerErr := errors.New("nope")
	a.Subscribe("s", Typed(func(context.Context, *Msg[[]byte]) error { return handlerErr }), WithDLQ("dlq.s"))
	start(t, a)
	if err := a.JetStream().DeleteStream(context.Background(), "DLQ"); err != nil {
		t.Fatal(err)
	}
	_ = a.Conn().Publish("s", nil)
	waitFor(t, func() bool { return len(dlf()) == 1 })
	e := dlf()[0]
	if !errors.Is(e.Err, handlerErr) || e.Err.Error() == handlerErr.Error() {
		t.Fatalf("Err = %v, want handler error joined with the DLQ error", e.Err)
	}
}

func TestEventManualAckNoMessageEvents(t *testing.T) {
	s := runServer(t)
	onAny, got := collect[MsgEvent]()
	a := newApp(t, s, WithOnRetry(onAny), WithOnFailed(onAny), WithOnDeadLettered(onAny), WithOnDeadLetterFailed(onAny))
	setupStream(t, a)
	called := make(chan struct{}, 1)
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error {
		called <- struct{}{}
		return errors.New("x")
	}), WithManualAck())
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	recv(t, called)
	time.Sleep(300 * time.Millisecond)
	if n := len(got()); n != 0 {
		t.Fatalf("got %d message events, want none", n)
	}
}

func TestEventHandlerRestartingRestarted(t *testing.T) {
	s := runServer(t)
	onRestarting, restarting := collect[HandlerEvent]()
	onRestarted, restarted := collect[HandlerEvent]()
	var a *App
	// Health takes the runner state lock: firing under it would deadlock.
	a = newApp(t, s, restartFast,
		WithOnHandlerRestarting(func(e HandlerEvent) { _ = a.Health(); onRestarting(e) }),
		WithOnHandlerRestarted(func(e HandlerEvent) { _ = a.Health(); onRestarted(e) }))
	setupStream(t, a)
	a.Consume("ORDERS", "billing", Typed(nopOrder))
	start(t, a)
	_ = a.JetStream().DeleteConsumer(context.Background(), "ORDERS", "billing")
	waitFor(t, func() bool { return len(restarted()) == 1 })
	rs := restarting()
	if len(rs) != 1 || rs[0].Handler != "ORDERS/billing" || rs[0].Kind != "consume" || rs[0].Err == nil {
		t.Fatalf("restarting = %+v", rs)
	}
	if e := restarted()[0]; e.Handler != "ORDERS/billing" || e.Kind != "consume" || e.Err != nil {
		t.Fatalf("restarted = %+v", e)
	}
}

func TestEventHandlerStopped(t *testing.T) {
	s := runServer(t)
	onStopped, stopped := collect[HandlerEvent]()
	a := newApp(t, s, WithOnHandlerStopped(onStopped))
	setupStream(t, a)
	a.Subscribe("s", Typed(nopOrder))
	a.Consume("ORDERS", "billing", Typed(nopOrder))
	start(t, a)
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range stopped() {
		if e.Err != nil {
			t.Fatalf("stopped = %+v", e)
		}
		names = append(names, e.Handler)
	}
	slices.Sort(names)
	if !slices.Equal(names, []string{"ORDERS/billing", "s"}) {
		t.Fatalf("stopped = %v", names)
	}
}
