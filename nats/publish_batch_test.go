package nats

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestPublishJSBatchAcksInOrder(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	start(t, a)
	var msgs []BatchMsg
	for i := range 5 {
		msgs = append(msgs, BatchMsg{Subject: "orders.x", Data: order{ID: fmt.Sprint(i)}})
	}
	acks, err := a.PublishJSBatch(context.Background(), msgs)
	if err != nil || len(acks) != 5 {
		t.Fatal(err, len(acks))
	}
	for i, ack := range acks {
		if ack == nil || ack.Sequence != uint64(i+1) {
			t.Fatal(i, ack)
		}
	}
}

func TestPublishJSBatchPartialFailure(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	start(t, a)
	acks, err := a.PublishJSBatch(context.Background(), []BatchMsg{
		{Subject: "orders.x", Data: order{ID: "a"}},
		{Subject: "orders.x", Data: order{ID: "b"}, Opts: []PubOption{ExpectLastSeq(99)}},
		{Subject: "orders.x", Data: order{ID: "c"}},
	})
	var be *PublishBatchError
	if !errors.As(err, &be) || len(be.Failed) != 1 || be.Failed[1] == nil {
		t.Fatal(err)
	}
	if acks[0] == nil || acks[1] != nil || acks[2] == nil {
		t.Fatal(acks)
	}
}

func TestPublishJSBatchEncodeErrorSendsNothing(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	start(t, a)
	_, err := a.PublishJSBatch(context.Background(), []BatchMsg{{Subject: "orders.x", Data: order{ID: "a"}}, {Subject: "orders.x", Data: make(chan int)}})
	if err == nil {
		t.Fatal("want encode error")
	}
	st, _ := a.JetStream().Stream(context.Background(), "ORDERS")
	info, _ := st.Info(context.Background())
	if info.State.Msgs != 0 {
		t.Fatal("nothing may be sent", info.State.Msgs)
	}
}

func TestPublishJSBatchMiddlewarePerMessage(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	var n atomic.Int32
	a.UsePublish(func(next PublishFunc) PublishFunc {
		return func(ctx context.Context, m *RawMsg) error { n.Add(1); return next(ctx, m) }
	})
	start(t, a)
	_, _ = a.PublishJSBatch(context.Background(), []BatchMsg{{Subject: "orders.x", Data: 1}, {Subject: "orders.x", Data: 2}})
	if n.Load() != 2 {
		t.Fatal(n.Load())
	}
}

func TestPublishJSBatchContextDeadline(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	start(t, a) // no stream: the futures fail fast or the context expires first
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	begin := time.Now()
	_, err := a.PublishJSBatch(ctx, []BatchMsg{{Subject: "nostream.x", Data: 1}, {Subject: "nostream.y", Data: 2}})
	var be *PublishBatchError
	if !errors.As(err, &be) || len(be.Failed) != 2 || time.Since(begin) > time.Second {
		t.Fatal(err, time.Since(begin))
	}
}

func TestPublishJSBatchNotRunning(t *testing.T) {
	a, _ := New()
	if _, err := a.PublishJSBatch(context.Background(), []BatchMsg{{Subject: "x", Data: 1}}); !errors.Is(err, ErrNotRunning) {
		t.Fatal(err)
	}
}
