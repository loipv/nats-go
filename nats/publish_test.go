package nats

import (
	"context"
	"errors"
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
	Subscribe(a, "h", func(_ context.Context, m *Msg[[]byte]) error { got <- m.Header.Get("X-Trace"); return nil })
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
	Reply(a, "inc", func(_ context.Context, m *Msg[int]) (int, error) {
		if m.Data < 0 {
			return 0, Terminal(errors.New("negative"))
		}
		return m.Data + 1, nil
	})
	start(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := Request[int](ctx, a, "inc", 1)
	if err != nil || r.Data != 2 {
		t.Fatal(r, err)
	}
	_, err = Request[int](ctx, a, "inc", -1)
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
	_, err := Request[int](ctx, a, "nobody.home", 1)
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
	Subscribe(a, "work", func(ctx context.Context, _ *Msg[[]byte]) error {
		close(started)
		time.Sleep(200 * time.Millisecond)
		pubErr <- a.Publish(ctx, "done", nil)
		return nil
	})
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
