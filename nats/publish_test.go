package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPublishNotRunning(t *testing.T) {
	a, _ := New()
	if err := a.Publish(context.Background(), "x", nil); !errors.Is(err, ErrNotRunning) {
		t.Fatal(err)
	}
}

func TestPublishMiddlewareSetsHeader(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	got := make(chan string, 1)
	a.UsePublish(func(next PublishFunc) PublishFunc {
		return func(ctx context.Context, m *RawMsg) error {
			if m.Header == nil {
				m.Header = Header{}
			}
			m.Header.Set("X-Trace", "t1")
			return next(ctx, m)
		}
	})
	a.Subscribe("h", Typed(func(_ context.Context, m *Msg[[]byte]) error { got <- m.Header.Get("X-Trace"); return nil }))
	start(t, a)
	if err := a.Publish(context.Background(), "h", map[string]int{"a": 1}); err != nil {
		t.Fatal(err)
	}
	if recv(t, got) != "t1" {
		t.Fatal("header missing")
	}
}

func TestRequestTypedAndServiceError(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	a.SubscribeReply("inc", TypedReply(func(_ context.Context, m *Msg[int]) (int, error) {
		if m.Data < 0 {
			return 0, Terminal(errors.New("negative"))
		}
		return m.Data + 1, nil
	}))
	start(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var r int
	_, err := a.Request(ctx, "inc", 1, &r)
	if err != nil || r != 2 {
		t.Fatal(r, err)
	}
	_, err = a.Request(ctx, "inc", -1, new(int))
	var se *ServiceError
	if !errors.As(err, &se) || se.Code != 400 || se.Description != "negative" {
		t.Fatal(err)
	}
}

func TestRequestNoResponders(t *testing.T) {
	a := newApp(t, runServer(t))
	start(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	begin := time.Now()
	_, err := a.Request(ctx, "nobody.home", 1, new(int))
	if !errors.Is(err, natsio.ErrNoResponders) || time.Since(begin) > time.Second {
		t.Fatal(err, time.Since(begin))
	}
}

func TestPublishJSDedup(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	a.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	start(t, a)
	ack1, err := a.PublishJS(context.Background(), "orders.created", map[string]string{"id": "1"}, WithMsgID("o-1"))
	if err != nil || ack1.Duplicate {
		t.Fatal(ack1, err)
	}
	ack2, err := a.PublishJS(context.Background(), "orders.created", map[string]string{"id": "1"}, WithMsgID("o-1"))
	if err != nil || !ack2.Duplicate || ack2.Sequence != ack1.Sequence {
		t.Fatal(ack2, err)
	}
}

func TestJSOnlyOptionRejectedOnPublish(t *testing.T) {
	a := newApp(t, runServer(t))
	start(t, a)
	if err := a.Publish(context.Background(), "x", nil, WithMsgID("1")); err == nil {
		t.Fatal("WithMsgID requires PublishJS")
	}
}

func TestHandlerCanPublishDuringShutdown(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	started, pubErr := make(chan struct{}), make(chan error, 1)
	a.Subscribe("work", Typed(func(ctx context.Context, _ *Msg[[]byte]) error {
		close(started)
		time.Sleep(200 * time.Millisecond)
		pubErr <- a.Publish(ctx, "done", nil)
		return nil
	}))
	start(t, a)
	_ = a.Conn().Publish("work", nil)
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-pubErr; err != nil {
		t.Fatal("in-flight handler must be able to publish during shutdown:", err)
	}
	if err := a.Publish(context.Background(), "x", nil); !errors.Is(err, ErrNotRunning) {
		t.Fatal("publish after shutdown:", err)
	}
}

func requestApp(t *testing.T) *App {
	t.Helper()
	a := newApp(t, runServer(t))
	a.SubscribeReply("req.order", TypedReply(func(context.Context, *Msg[[]byte]) (map[string]string, error) {
		return map[string]string{"ID": "1"}, nil
	}))
	start(t, a)
	if _, err := a.Conn().Subscribe("req.bad", func(m *natsio.Msg) { _ = m.Respond([]byte("not json")) }); err != nil {
		t.Fatal(err)
	}
	return a
}

func reqCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestRequestOutNil(t *testing.T) {
	a := requestApp(t)
	reply, err := a.Request(reqCtx(t), "req.order", "x", nil)
	if err != nil || string(reply.Data) != `{"ID":"1"}` {
		t.Fatal(reply, err)
	}
}

func TestRequestOutBytes(t *testing.T) {
	a := requestApp(t)
	var b []byte
	if _, err := a.Request(reqCtx(t), "req.order", "x", &b); err != nil || string(b) != `{"ID":"1"}` {
		t.Fatal(string(b), err)
	}
}

func TestRequestOutStruct(t *testing.T) {
	a := requestApp(t)
	var inv struct{ ID string }
	reply, err := a.Request(reqCtx(t), "req.order", "x", &inv)
	if err != nil || inv.ID != "1" || reply.Subject == "" {
		t.Fatal(inv, reply, err)
	}
}

func TestRequestOutMustBePointer(t *testing.T) {
	a := requestApp(t)
	var n atomic.Int32
	if _, err := a.Conn().Subscribe("req.count", func(m *natsio.Msg) { n.Add(1); _ = m.Respond(nil) }); err != nil {
		t.Fatal(err)
	}
	for _, out := range []any{struct{}{}, (*struct{})(nil)} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := a.Request(ctx, "req.count", "x", out)
		cancel()
		if err == nil || !strings.Contains(err.Error(), "non-nil pointer") {
			t.Fatalf("out %T: err = %v", out, err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if n.Load() != 0 {
		t.Fatalf("%d requests sent", n.Load())
	}
}

func TestRequestDecodeFailureReturnsReply(t *testing.T) {
	a := requestApp(t)
	var inv struct{ ID string }
	reply, err := a.Request(reqCtx(t), "req.bad", "x", &inv)
	if err == nil || reply == nil || string(reply.Data) != "not json" {
		t.Fatal(reply, err)
	}
}

func TestMessageEncodesAndSetsJSHeaders(t *testing.T) {
	a, _ := New() // not started: Message needs no connection
	m, err := a.Message("orders.x", order{ID: "1"}, WithHeader(Header{"X-A": []string{"1"}}), WithMsgID("m-1"), WithExpectLastSeq(7), WithExpectStream("ORDERS"), WithExpectLastSubjectSeq(3))
	if err != nil {
		t.Fatal(err)
	}
	if string(m.Data) != `{"id":"1"}` || m.Subject != "orders.x" {
		t.Fatal(m.Subject, string(m.Data))
	}
	for k, v := range map[string]string{"X-A": "1", "Nats-Msg-Id": "m-1", "Nats-Expected-Last-Sequence": "7", "Nats-Expected-Stream": "ORDERS", "Nats-Expected-Last-Subject-Sequence": "3"} {
		if m.Header.Get(k) != v {
			t.Fatalf("%s=%q", k, m.Header.Get(k))
		}
	}
	if _, err := a.Message("x", make(chan int)); err == nil {
		t.Fatal("encode error expected")
	}
}

func TestPublishJSExpectHeaderEnforced(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	start(t, a)
	if _, err := a.PublishJS(context.Background(), "orders.x", order{ID: "1"}, WithExpectLastSeq(42)); err == nil {
		t.Fatal("wrong WithExpectLastSeq must be rejected by the server")
	}
	ack1, _ := a.PublishJS(context.Background(), "orders.x", order{ID: "1"}, WithMsgID("d"))
	ack2, _ := a.PublishJS(context.Background(), "orders.x", order{ID: "1"}, WithMsgID("d"))
	if !ack2.Duplicate || ack2.Sequence != ack1.Sequence {
		t.Fatal(ack2)
	}
}

func TestWrapPublishAppliesMiddlewareInOrder(t *testing.T) {
	a, _ := New()
	var trace []string
	mk := func(n string) PublishMiddleware {
		return func(next PublishFunc) PublishFunc {
			return func(ctx context.Context, m *RawMsg) error {
				trace = append(trace, n)
				return next(ctx, m)
			}
		}
	}
	a.UsePublish(mk("p1"), mk("p2"))
	_ = a.WrapPublish(func(context.Context, *RawMsg) error { trace = append(trace, "send"); return nil })(context.Background(), &RawMsg{})
	if strings.Join(trace, ",") != "p1,p2,send" {
		t.Fatal(trace)
	}
}

// I3: handlers that run as soon as Start opens intake must be able to publish.
func TestHandlerCanPublishDuringStartup(t *testing.T) {
	s := runServer(t)
	seed := newApp(t, s)
	setupStream(t, seed)
	start(t, seed)
	for range 20 {
		_, _ = seed.PublishJS(context.Background(), "orders.x", order{ID: "backlog"})
	}
	a := newApp(t, s)
	var failed atomic.Int32
	var handled atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(ctx context.Context, _ *Msg[order]) error {
		if err := a.Publish(ctx, "audit", nil); err != nil {
			failed.Add(1)
		}
		handled.Add(1)
		return nil
	}))
	// More registrations keep Start busy after billing has begun receiving.
	for i := range 10 {
		name := fmt.Sprintf("extra%d", i)
		a.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: name, AckPolicy: jetstream.AckExplicitPolicy})
		a.Consume("ORDERS", name, Typed(func(context.Context, *Msg[order]) error { return nil }))
	}
	start(t, a)
	waitFor(t, func() bool { return handled.Load() == 20 })
	if failed.Load() != 0 {
		t.Fatalf("%d publishes failed during startup", failed.Load())
	}
}

// A publish middleware that short-circuits must not panic Request.
func TestRequestShortCircuitMiddlewareDoesNotPanic(t *testing.T) {
	a := newApp(t, runServer(t))
	a.UsePublish(func(PublishFunc) PublishFunc {
		return func(context.Context, *RawMsg) error { return nil }
	})
	start(t, a)
	if _, err := a.Request(context.Background(), "x", 1, new(int)); err == nil {
		t.Fatal("want error when no reply was produced")
	}
}

func TestPublishJSWithJSPublishOpts(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	start(t, a)
	ctx := context.Background()
	if _, err := a.PublishJS(ctx, "orders.x", order{ID: "1"}, WithMsgID("m-1")); err != nil {
		t.Fatal(err)
	}
	_, err := a.PublishJS(ctx, "orders.x", order{ID: "2"}, WithJSPublishOpts(jetstream.WithExpectLastMsgID("other")))
	if err == nil {
		t.Fatal("expected wrong last msg id to fail")
	}
	if _, err := a.PublishJS(ctx, "orders.x", order{ID: "2"}, WithJSPublishOpts(jetstream.WithExpectLastMsgID("m-1"))); err != nil {
		t.Fatal(err)
	}
}

func TestJSPublishOptsRejectedOutsidePublishJS(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	start(t, a)
	ctx := context.Background()
	opt := WithJSPublishOpts(jetstream.WithMsgID("x"))
	_, msgErr := a.Message("x", 1, opt)
	_, reqErr := a.Request(ctx, "x", 1, nil, opt)
	_, batchErr := a.PublishJSBatch(ctx, []BatchMsg{{Subject: "x", Data: 1, Opts: []PubOption{opt}}})
	for name, err := range map[string]error{
		"Publish": a.Publish(ctx, "x", 1, opt), "Request": reqErr, "Message": msgErr, "PublishJSBatch": batchErr,
	} {
		if err == nil || !strings.Contains(err.Error(), "WithJSPublishOpts") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// An empty JetStream option value sets no header but is still JS-only.
func TestEmptyJSOptionSetsNoHeader(t *testing.T) {
	a, err := New()
	if err != nil {
		t.Fatal(err)
	}
	m, err := a.Message("orders.x", 1, WithMsgID(""), WithExpectStream(""))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Header) != 0 {
		t.Fatalf("headers %v, want none", m.Header)
	}
	_, c, _ := a.build("orders.x", 1, []PubOption{WithMsgID("")})
	if len(c.jsOnly) != 1 {
		t.Fatalf("jsOnly = %v, want WithMsgID recorded", c.jsOnly)
	}
}

func TestEmptyMsgIDClearsEarlierOne(t *testing.T) {
	a, err := New()
	if err != nil {
		t.Fatal(err)
	}
	m, err := a.Message("orders.x", 1, WithMsgID("x"), WithMsgID(""))
	if err != nil {
		t.Fatal(err)
	}
	if id := m.Header.Get(jetstream.MsgIDHeader); id != "" {
		t.Fatalf("Nats-Msg-Id = %q, want none", id)
	}
}
