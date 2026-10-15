package nats

import (
	"context"
	"errors"
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
	a.Reply("inc", TypedReply(func(_ context.Context, m *Msg[int]) (int, error) {
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
	ack1, err := a.PublishJS(context.Background(), "orders.created", map[string]string{"id": "1"}, MsgID("o-1"))
	if err != nil || ack1.Duplicate {
		t.Fatal(ack1, err)
	}
	ack2, err := a.PublishJS(context.Background(), "orders.created", map[string]string{"id": "1"}, MsgID("o-1"))
	if err != nil || !ack2.Duplicate || ack2.Sequence != ack1.Sequence {
		t.Fatal(ack2, err)
	}
}

func TestJSOnlyOptionRejectedOnPublish(t *testing.T) {
	a := newApp(t, runServer(t))
	start(t, a)
	if err := a.Publish(context.Background(), "x", nil, MsgID("1")); err == nil {
		t.Fatal("MsgID requires PublishJS")
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
	a.Reply("req.order", TypedReply(func(context.Context, *Msg[[]byte]) (map[string]string, error) {
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
