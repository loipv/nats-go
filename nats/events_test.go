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
	for _, want := range []string{"level=ERROR", "nats: event callback panicked", "panic=boom", "stack="} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("log = %q, want %q", buf.String(), want)
		}
	}
}

func TestEventOptionsAppend(t *testing.T) {
	lens := func(e events) map[string]int {
		return map[string]int{
			"retry": len(e.retry), "failed": len(e.failed), "deadLettered": len(e.deadLettered),
			"deadLetterFailed": len(e.deadLetterFailed), "restarting": len(e.restarting),
			"restarted": len(e.restarted), "stopped": len(e.stopped), "disconnected": len(e.disconnected),
			"reconnected": len(e.reconnected), "closed": len(e.closed), "asyncError": len(e.asyncError),
		}
	}
	msg, hnd, nop := func(MsgEvent) {}, func(HandlerEvent) {}, func() {}
	cases := []struct {
		field string
		opt   Option
	}{
		{"retry", WithOnRetry(msg)},
		{"failed", WithOnFailed(msg)},
		{"deadLettered", WithOnDeadLettered(msg)},
		{"deadLetterFailed", WithOnDeadLetterFailed(msg)},
		{"restarting", WithOnHandlerRestarting(hnd)},
		{"restarted", WithOnHandlerRestarted(hnd)},
		{"stopped", WithOnHandlerStopped(hnd)},
		{"disconnected", WithOnDisconnected(func(error) {})},
		{"reconnected", WithOnReconnected(nop)},
		{"closed", WithOnClosed(nop)},
		{"asyncError", WithOnAsyncError(func(AsyncErrorEvent) {})},
	}
	for _, c := range cases {
		t.Run(c.field, func(t *testing.T) {
			a, err := New(c.opt)
			if err != nil {
				t.Fatal(err)
			}
			for field, n := range lens(a.opts.events) {
				want := 0
				if field == c.field {
					want = 1
				}
				if n != want {
					t.Errorf("len(%s) = %d, want %d", field, n, want)
				}
			}
		})
	}
	a, err := New(WithOnClosed(nop), WithOnClosed(nop))
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
	if d := dead()[0]; d.Attempt != 3 || d.Handler != "ORDERS/billing" || d.Err == nil || d.Err.Error() != "db down" || d.Msg == nil {
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
	if len(rs) != 1 || rs[0].Attempt != 1 || rs[0].Handler != "s" || rs[0].Subject != "s" || rs[0].Err == nil || rs[0].Err.Error() != "nope" || rs[0].Msg == nil {
		t.Fatalf("retries = %+v", rs)
	}
	if e := failed()[0]; e.Attempt != 2 || e.Handler != "s" || e.Subject != "s" || e.Err == nil || e.Err.Error() != "nope" || e.Msg == nil {
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
	if e.Attempt != 1 || e.Handler != "s" {
		t.Fatalf("event = %+v", e)
	}
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
	reconnected, userReconnected := make(chan struct{}, 1), make(chan struct{}, 1)
	a := newApp(t, s, WithNATSOptions(natsio.ReconnectWait(50*time.Millisecond),
		natsio.ReconnectHandler(func(*natsio.Conn) { userReconnected <- struct{}{} })),
		WithOnReconnected(func() { reconnected <- struct{}{} }))
	start(t, a)
	s.Shutdown()
	s.WaitForShutdown()
	s2 := natstest.RunServer(&opts)
	t.Cleanup(s2.Shutdown)
	recv(t, reconnected)
	recv(t, userReconnected)
}

// slowConsumer makes nats.go report a slow consumer on subject "slow".
func slowConsumer(t *testing.T, a *App) {
	t.Helper()
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
}

func TestEventConnAsyncErrorChainsUserErrorHandler(t *testing.T) {
	s := runServer(t)
	user := make(chan struct{}, 1)
	onErr, got := collect[AsyncErrorEvent]()
	a := newApp(t, s, WithOnAsyncError(onErr), WithNATSOptions(natsio.ErrorHandler(func(_ *natsio.Conn, _ *natsio.Subscription, err error) {
		if errors.Is(err, natsio.ErrSlowConsumer) {
			select {
			case user <- struct{}{}:
			default:
			}
		}
	})))
	start(t, a)
	slowConsumer(t, a)
	recv(t, user)
	waitFor(t, func() bool { return len(got()) > 0 })
}

func TestEventConnAsyncErrorLoggedWithoutUserHandler(t *testing.T) {
	s := runServer(t)
	var buf syncBuf
	a := newApp(t, s, WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
	start(t, a)
	slowConsumer(t, a)
	waitFor(t, func() bool { return strings.Contains(buf.String(), "nats: connection error") })
}

func TestEventConnAsyncError(t *testing.T) {
	s := runServer(t)
	onErr, got := collect[AsyncErrorEvent]()
	a := newApp(t, s, WithOnAsyncError(onErr))
	start(t, a)
	slowConsumer(t, a)
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
	a.Idempotency(completeFails{NewMemoryStore()})
	a.Consume("ORDERS", "billing", Typed(nopOrder), WithIdempotency())
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

func TestEventSubscribeDeadLettered(t *testing.T) {
	s := runServer(t)
	onDLQ, dead := collect[MsgEvent]()
	a := newApp(t, s, WithOnDeadLettered(onDLQ))
	setupDLQ(t, a)
	a.Subscribe("s", Typed(func(context.Context, *Msg[[]byte]) error { return errors.New("nope") }),
		WithMaxAttempts(2), fast, WithDLQ("dlq.s"))
	start(t, a)
	_ = a.Conn().Publish("s", nil)
	waitFor(t, func() bool { return len(dead()) == 1 })
	if e := dead()[0]; e.Handler != "s" || e.Subject != "s" || e.Attempt != 2 || e.Err == nil || e.Err.Error() != "nope" || e.Msg == nil {
		t.Fatalf("dead-lettered = %+v", e)
	}
}

func TestEventConsumeDeadLetterFailed(t *testing.T) {
	s := runServer(t)
	onDLF, dlf := collect[MsgEvent]()
	a := newApp(t, s, WithOnDeadLetterFailed(onDLF))
	setupStream(t, a)
	setupDLQ(t, a)
	handlerErr := Terminal(errors.New("bad"))
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error { return handlerErr }), fast, WithDLQ("dlq.orders"))
	start(t, a)
	if err := a.JetStream().DeleteStream(context.Background(), "DLQ"); err != nil {
		t.Fatal(err)
	}
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	waitFor(t, func() bool { return len(dlf()) > 0 })
	e := dlf()[0]
	if e.Handler != "ORDERS/billing" || e.Subject != "orders.x" || e.Attempt != 1 || !errors.Is(e.Err, handlerErr) || e.Err.Error() == handlerErr.Error() {
		t.Fatalf("event = %+v", e)
	}
}

func TestEventReplyEncodeFailure(t *testing.T) {
	s := runServer(t)
	onFailed, failed := collect[MsgEvent]()
	a := newApp(t, s, WithOnFailed(onFailed))
	// The JSON codec cannot encode a channel.
	a.SubscribeReply("enc", TypedReply(func(context.Context, *Msg[[]byte]) (chan int, error) { return make(chan int), nil }))
	start(t, a)
	_, _ = a.Conn().Request("enc", nil, time.Second)
	waitFor(t, func() bool { return len(failed()) == 1 })
	if e := failed()[0]; e.Handler != "enc" || e.Subject != "enc" || e.Attempt != 0 || e.Err == nil || !strings.Contains(e.Err.Error(), "encode response") {
		t.Fatalf("failed = %+v", e)
	}
}

func TestEventConsumeBatchRetryThenDeadLettered(t *testing.T) {
	s := runServer(t)
	onRetry, retries := collect[MsgEvent]()
	onDLQ, dead := collect[MsgEvent]()
	a := newApp(t, s, WithOnRetry(onRetry), WithOnDeadLettered(onDLQ))
	setupStream(t, a)
	setupDLQ(t, a)
	errBad := errors.New("bad row")
	a.ConsumeBatch("ORDERS", "billing", TypedBatch(func(_ context.Context, ms []*Msg[order]) error {
		be := &BatchError{Failed: map[int]error{}}
		for i, m := range ms {
			if m.Data.ID == "bad" {
				be.Failed[i] = errBad
			}
		}
		return be
	}), WithBatchWait(100*time.Millisecond), WithMaxAttempts(2), fast, WithDLQ("dlq.orders"))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.bad", order{ID: "bad"})
	_, _ = a.PublishJS(context.Background(), "orders.ok", order{ID: "ok"})
	waitFor(t, func() bool { return len(dead()) == 1 && pendingAcks(t, a, "billing") == 0 })
	// Only the listed message gets events; the other one is acked.
	rs := retries()
	if len(rs) != 1 || rs[0].Attempt != 1 || rs[0].Handler != "ORDERS/billing" || rs[0].Subject != "orders.bad" || !errors.Is(rs[0].Err, errBad) {
		t.Fatalf("retries = %+v", rs)
	}
	if d := dead()[0]; d.Attempt != 2 || d.Handler != "ORDERS/billing" || d.Subject != "orders.bad" || !errors.Is(d.Err, errBad) {
		t.Fatalf("dead-lettered = %+v", d)
	}
}

func TestEventConsumeOrderedRetry(t *testing.T) {
	s := runServer(t)
	onRetry, retries := collect[MsgEvent]()
	onFailed, failed := collect[MsgEvent]()
	a := newApp(t, s, WithOnRetry(onRetry), WithOnFailed(onFailed))
	setupStream(t, a)
	a.ConsumeOrdered("ORDERS", Typed(func(context.Context, *Msg[order]) error { return errors.New("nope") }),
		WithMaxAttempts(2), fast)
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	waitFor(t, func() bool { return len(failed()) == 1 })
	rs := retries()
	if len(rs) != 1 || rs[0].Attempt != 1 || rs[0].Handler != "ORDERS (ordered)" || rs[0].Subject != "orders.x" || rs[0].Err == nil || rs[0].Err.Error() != "nope" {
		t.Fatalf("retries = %+v", rs)
	}
	if e := failed()[0]; e.Attempt != 2 || e.Handler != "ORDERS (ordered)" {
		t.Fatalf("failed = %+v", e)
	}
}

func TestEventHandlerStoppedCallbackMayCallApp(t *testing.T) {
	s := runServer(t)
	var a *App
	called := make(chan error, 1)
	a = newApp(t, s, WithOnHandlerStopped(func(HandlerEvent) {
		_ = a.Health()
		select {
		case called <- a.Publish(context.Background(), "x", nil):
		default:
		}
	}))
	a.Subscribe("s", Typed(nopOrder))
	start(t, a)
	done := make(chan error, 1)
	go func() { done <- a.Shutdown(context.Background()) }()
	if err := recv(t, done); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, called); err != nil {
		t.Fatalf("Publish from OnHandlerStopped: %v", err)
	}
}

// smallBatchConsumer declares a consumer whose pull requests may ask for at
// most 1 message, so any larger pull fails with a 409 from the server.
func smallBatchConsumer(t *testing.T, a *App) {
	t.Helper()
	a.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	a.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "billing", AckPolicy: jetstream.AckExplicitPolicy, MaxRequestBatch: 1})
}

func hasAsyncPrefix(errs func() []AsyncErrorEvent, handler, prefix string) func() bool {
	return func() bool {
		return slices.ContainsFunc(errs(), func(e AsyncErrorEvent) bool {
			return e.Handler == handler && strings.HasPrefix(e.Err.Error(), prefix)
		})
	}
}

func TestEventAsyncConsume(t *testing.T) {
	s := runServer(t)
	onErr, errs := collect[AsyncErrorEvent]()
	a := newApp(t, s, WithOnAsyncError(onErr))
	smallBatchConsumer(t, a)
	a.Consume("ORDERS", "billing", Typed(nopOrder), WithPullMaxMessages(10))
	start(t, a)
	waitFor(t, hasAsyncPrefix(errs, "ORDERS/billing", "consume: "))
}

func TestEventAsyncFetch(t *testing.T) {
	s := runServer(t)
	onErr, errs := collect[AsyncErrorEvent]()
	a := newApp(t, s, WithOnAsyncError(onErr))
	smallBatchConsumer(t, a)
	a.ConsumeBatch("ORDERS", "billing", TypedBatch(func(context.Context, []*Msg[order]) error { return nil }),
		WithBatchSize(10), WithBatchWait(100*time.Millisecond))
	start(t, a)
	waitFor(t, hasAsyncPrefix(errs, "ORDERS/billing", "fetch: "))
}

// A Shutdown during restart attempts reports no error caused by stopping.
func TestEventNoRestartErrorOnShutdown(t *testing.T) {
	s := runServer(t)
	onErr, errs := collect[AsyncErrorEvent]()
	seed := newApp(t, s)
	setupStream(t, seed)
	start(t, seed)
	a := newApp(t, s, WithOnAsyncError(onErr),
		WithRestartBackoff(func(int) time.Duration { return time.Millisecond }))
	a.Consume("ORDERS", "billing", Typed(nopOrder))
	start(t, a)
	_ = a.JetStream().DeleteConsumer(context.Background(), "ORDERS", "billing")
	waitFor(t, hasAsyncPrefix(errs, "ORDERS/billing", "restart: "))
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, e := range errs() {
		if errors.Is(e.Err, context.Canceled) {
			t.Fatalf("event caused by Shutdown: %+v", e)
		}
	}
}
