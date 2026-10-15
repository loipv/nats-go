package nats

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
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

func TestEventConnectionChainsUserCallbacks(t *testing.T) {
	s := runServer(t)
	var mu sync.Mutex
	got := map[string]bool{}
	mark := func(k string) { mu.Lock(); got[k] = true; mu.Unlock() }
	a := newApp(t, s,
		WithNATSOptions(
			natsio.ClosedHandler(func(*natsio.Conn) { mark("userClosed") }),
			natsio.DisconnectErrHandler(func(*natsio.Conn, error) { mark("userDisc") })),
		WithOnClosed(func() { mark("closed") }),
		WithOnDisconnected(func(error) { mark("disc") }))
	start(t, a)
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, k := range []string{"userClosed", "userDisc", "closed", "disc"} {
		if !got[k] {
			t.Errorf("%s not called (got %v)", k, got)
		}
	}
}

func TestEventConnectionChainsDeprecatedDisconnectedCB(t *testing.T) {
	s := runServer(t)
	user := make(chan struct{}, 1)
	a := newApp(t, s, WithNATSOptions(natsio.DisconnectHandler(func(*natsio.Conn) { user <- struct{}{} })))
	start(t, a)
	_ = a.Shutdown(context.Background())
	recv(t, user)
}

func TestRunReturnsWithUserClosedHandler(t *testing.T) {
	s := runServer(t)
	userClosed := make(chan struct{}, 1)
	a := newApp(t, s, WithNATSOptions(natsio.MaxReconnects(0),
		natsio.ClosedHandler(func(*natsio.Conn) { userClosed <- struct{}{} })))
	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()
	waitFor(t, func() bool { return a.Conn() != nil })
	s.Shutdown()
	select {
	case err := <-done:
		if !errors.Is(err, natsio.ErrConnectionClosed) {
			t.Fatalf("Run = %v, want ErrConnectionClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	recv(t, userClosed)
}

func TestEventReconnected(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := natstest.RunServer(&opts)
	opts.Port = s.Addr().(*net.TCPAddr).Port
	reconnected := make(chan struct{}, 1)
	a := newApp(t, s, WithNATSOptions(natsio.ReconnectWait(50*time.Millisecond)),
		WithOnReconnected(func() { reconnected <- struct{}{} }))
	start(t, a)
	s.Shutdown()
	s.WaitForShutdown()
	s2 := natstest.RunServer(&opts)
	t.Cleanup(s2.Shutdown)
	recv(t, reconnected)
}

func TestEventConnAsyncError(t *testing.T) {
	s := runServer(t)
	onErr, got := collect[AsyncErrorEvent]()
	a := newApp(t, s, WithOnAsyncError(onErr))
	start(t, a)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	sub, err := a.Conn().Subscribe("slow", func(*natsio.Msg) { <-block })
	if err != nil {
		t.Fatal(err)
	}
	_ = sub.SetPendingLimits(1, -1)
	for range 10 {
		_ = a.Conn().Publish("slow", []byte("x"))
	}
	_ = a.Conn().Flush()
	waitFor(t, func() bool { return len(got()) > 0 })
	e := got()[0]
	if e.Subject != "slow" || !errors.Is(e.Err, natsio.ErrSlowConsumer) || !strings.HasPrefix(e.Err.Error(), "conn: ") {
		t.Fatalf("event = %+v", e)
	}
}

func TestEventConnCallbackPanicRecovered(t *testing.T) {
	s := runServer(t)
	recorded := make(chan struct{}, 1)
	a := newApp(t, s,
		WithOnDisconnected(func(error) { panic("boom") }),
		WithOnDisconnected(func(error) { recorded <- struct{}{} }))
	start(t, a)
	_ = a.Shutdown(context.Background())
	recv(t, recorded)
}

func TestEventAsyncRestart(t *testing.T) {
	s := runServer(t)
	onErr, errs := collect[AsyncErrorEvent]()
	seed := newApp(t, s)
	setupStream(t, seed)
	start(t, seed)
	a := newApp(t, s, restartFast, WithOnAsyncError(onErr))
	a.Consume("ORDERS", "billing", Typed(nopOrder))
	start(t, a)
	_ = a.JetStream().DeleteConsumer(context.Background(), "ORDERS", "billing")
	waitFor(t, func() bool {
		return slices.ContainsFunc(errs(), func(e AsyncErrorEvent) bool {
			return e.Handler == "ORDERS/billing" && strings.HasPrefix(e.Err.Error(), "restart: ")
		})
	})
	_, _ = a.JetStream().CreateOrUpdateConsumer(context.Background(), "ORDERS", jetstream.ConsumerConfig{Durable: "billing", AckPolicy: jetstream.AckExplicitPolicy})
	waitFor(t, func() bool { return a.Health().Status == "ok" })
}

func TestEventAsyncIdempotencyComplete(t *testing.T) {
	s := runServer(t)
	onErr, errs := collect[AsyncErrorEvent]()
	a := newApp(t, s, WithOnAsyncError(onErr))
	setupStream(t, a)
	a.Use(Idempotent(completeFails{NewMemoryStore()}))
	a.Consume("ORDERS", "billing", Typed(nopOrder))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"}, WithMsgID("e-1"))
	waitFor(t, func() bool {
		return slices.ContainsFunc(errs(), func(e AsyncErrorEvent) bool {
			return e.Subject == "orders.x" && strings.HasPrefix(e.Err.Error(), "idempotency complete: ")
		})
	})
}

func TestEventHandlerStoppedNamesServiceEndpoints(t *testing.T) {
	s := runServer(t)
	onStopped, stopped := collect[HandlerEvent]()
	a := newApp(t, s, WithOnHandlerStopped(onStopped))
	svc := a.Service(micro.Config{Name: "inventory", Version: "1.0.0"})
	h := TypedReply(func(_ context.Context, m *Msg[getReq]) (order, error) { return order{ID: m.Data.ID}, nil })
	svc.Endpoint("get", h)
	svc.Group("v2").Endpoint("list", h)
	start(t, a)
	var want []string
	for _, st := range a.Health().Handlers {
		want = append(want, st.Name)
	}
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range stopped() {
		if e.Kind != "endpoint" || e.Err != nil {
			t.Fatalf("stopped = %+v", e)
		}
		got = append(got, e.Handler)
	}
	slices.Sort(want)
	slices.Sort(got)
	if len(want) != 2 || !slices.Equal(got, want) {
		t.Fatalf("stopped = %v, health = %v", got, want)
	}
}

func TestEventHandlerStoppedOnFailedStart(t *testing.T) {
	s := runServer(t)
	onStopped, stopped := collect[HandlerEvent]()
	a := newApp(t, s, WithOnHandlerStopped(onStopped))
	a.Subscribe("ok", Typed(nopOrder))
	a.Subscribe("foo..bar", Typed(nopOrder))
	if err := a.Start(context.Background()); err == nil {
		t.Fatal("Start must fail")
	}
	es := stopped()
	if len(es) != 1 || es[0].Handler != "ok" {
		t.Fatalf("stopped = %+v", es)
	}
}

func TestShutdownWithNoCallbacksAfterClientClose(t *testing.T) {
	s := runServer(t)
	closed := make(chan struct{}, 1)
	a := newApp(t, s, WithNATSOptions(natsio.NoCallbacksAfterClientClose()),
		WithOnClosed(func() { closed <- struct{}{} }))
	start(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown = %v, want nil", err)
	}
	recv(t, closed)
}
