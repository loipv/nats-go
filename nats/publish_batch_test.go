package nats

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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
		{Subject: "orders.x", Data: order{ID: "b"}, Opts: []PubOption{WithExpectLastSeq(99)}},
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
	// No stream: both messages are sent, then jetstream retries the
	// no-responders publish after 250ms, so ctx expires before any future
	// settles and both outcomes are reported as unknown (the ctx error).
	start(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	begin := time.Now()
	_, err := a.PublishJSBatch(ctx, []BatchMsg{{Subject: "nostream.x", Data: 1}, {Subject: "nostream.y", Data: 2}})
	var be *PublishBatchError
	if !errors.As(err, &be) || len(be.Failed) != 2 || time.Since(begin) > time.Second {
		t.Fatal(err, time.Since(begin))
	}
	for i, err := range be.Failed {
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(i, err)
		}
	}
}

func TestPublishJSBatchNotRunning(t *testing.T) {
	a, _ := New()
	if _, err := a.PublishJSBatch(context.Background(), []BatchMsg{{Subject: "x", Data: 1}}); !errors.Is(err, ErrNotRunning) {
		t.Fatal(err)
	}
}

// Acks that arrived before ctx expired must be reported as stored.
func TestPublishJSBatchReadyAcksBeatExpiredContext(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var n atomic.Int32
	a.UsePublish(func(next PublishFunc) PublishFunc {
		return func(ctx context.Context, m *RawMsg) error {
			err := next(ctx, m)
			if n.Add(1) == 4 { // last message: let every ack arrive, then expire ctx
				time.Sleep(200 * time.Millisecond)
				cancel()
			}
			return err
		}
	})
	start(t, a)
	var msgs []BatchMsg
	for i := range 4 {
		msgs = append(msgs, BatchMsg{Subject: "orders.x", Data: order{ID: fmt.Sprint(i)}})
	}
	acks, err := a.PublishJSBatch(ctx, msgs)
	if err != nil {
		t.Fatal(err)
	}
	for i, ack := range acks {
		if ack == nil {
			t.Fatal("ack lost", i)
		}
	}
}

// A ctx that is already done must stop the batch before anything is sent.
func TestPublishJSBatchCancelledContextSendsNothing(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	start(t, a)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var msgs []BatchMsg
	for i := range 5 {
		msgs = append(msgs, BatchMsg{Subject: "orders.x", Data: order{ID: fmt.Sprint(i)}})
	}
	_, err := a.PublishJSBatch(ctx, msgs)
	var be *PublishBatchError
	if !errors.As(err, &be) || len(be.Failed) != 5 || !errors.Is(be.Failed[4], context.Canceled) {
		t.Fatal(err)
	}
	st, _ := a.JetStream().Stream(context.Background(), "ORDERS")
	info, _ := st.Info(context.Background())
	if info.State.Msgs != 0 {
		t.Fatal("nothing may be sent", info.State.Msgs)
	}
}

// Beyond the async in-flight limit (4000) every send stalls up to 200ms; an
// expired ctx must stop sending instead of stalling for each remaining message.
func TestPublishJSBatchDeadlineStopsSending(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	start(t, a)
	sink, err := a.Conn().Subscribe("sink.>", func(*natsio.Msg) {}) // receives, never acks
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Unsubscribe()
	msgs := make([]BatchMsg, 4010)
	for i := range msgs {
		msgs[i] = BatchMsg{Subject: "sink.x", Data: i}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	begin := time.Now()
	_, err = a.PublishJSBatch(ctx, msgs)
	var be *PublishBatchError
	if !errors.As(err, &be) || len(be.Failed) != len(msgs) || time.Since(begin) > time.Second {
		t.Fatal(err, time.Since(begin))
	}
}

// Unanswered async publishes must time out, or they fill the in-flight limit
// and every later batch stalls and fails with ErrTooManyStalledMsgs.
func TestPublishJSBatchUnansweredFuturesTimeOut(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	a.publishAsyncTimeout = 300 * time.Millisecond
	setupStream(t, a)
	start(t, a)
	sink, err := a.Conn().Subscribe("sink.>", func(*natsio.Msg) {}) // receives, never acks
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Unsubscribe()
	msgs := make([]BatchMsg, 4000)
	for i := range msgs {
		msgs[i] = BatchMsg{Subject: "sink.x", Data: i}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = a.PublishJSBatch(ctx, msgs)
	var be *PublishBatchError
	if !errors.As(err, &be) {
		t.Fatal(err)
	}
	if !errors.Is(be.Failed[0], jetstream.ErrAsyncPublishTimeout) {
		t.Error("want ack timeout, got", be.Failed[0])
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := a.PublishJSBatch(ctx, []BatchMsg{{Subject: "orders.x", Data: 1}}); err != nil {
		if errors.As(err, &be) {
			t.Fatal(be.Failed)
		}
		t.Fatal(err)
	}
}

func atomicApp(t *testing.T, name, subj string, allow bool) *App {
	t.Helper()
	a := newApp(t, runServer(t))
	a.Stream(jetstream.StreamConfig{Name: name, Subjects: []string{subj}, AllowAtomicPublish: allow})
	return a
}

func startAtomic(t *testing.T, name, subj string, allow bool) *App {
	t.Helper()
	a := atomicApp(t, name, subj, allow)
	start(t, a)
	return a
}

func streamMsgs(t *testing.T, a *App, name string) uint64 {
	t.Helper()
	s, err := a.JetStream().Stream(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

func streamMsgHeader(t *testing.T, a *App, name string, seq uint64, key string) string {
	t.Helper()
	s, err := a.JetStream().Stream(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.GetMsg(context.Background(), seq)
	if err != nil {
		t.Fatal(err)
	}
	return m.Header.Get(key)
}

func TestPublishJSAtomicBatch(t *testing.T) {
	a := startAtomic(t, "ATOMIC", "atomic.>", true)
	msgs := []BatchMsg{{Subject: "atomic.a", Data: 1}, {Subject: "atomic.b", Data: 2}, {Subject: "atomic.c", Data: 3}}
	ack, err := a.PublishJSAtomicBatch(context.Background(), msgs)
	if err != nil || ack.BatchSize != 3 {
		t.Fatal(ack, err)
	}
	if streamMsgs(t, a, "ATOMIC") != 3 {
		t.Fatal()
	}
}

func TestPublishJSAtomicBatchRejectedWithoutAllow(t *testing.T) {
	a := startAtomic(t, "PLAIN", "plain.>", false)
	if _, err := a.PublishJSAtomicBatch(context.Background(), []BatchMsg{{Subject: "plain.a", Data: 1}, {Subject: "plain.b", Data: 2}}); err == nil {
		t.Fatal("want error")
	}
	if streamMsgs(t, a, "PLAIN") != 0 {
		t.Fatal("nothing may be stored")
	}
}

func TestPublishJSAtomicBatchSubjectOutsideStream(t *testing.T) {
	a := startAtomic(t, "ATOMIC", "atomic.>", true)
	if _, err := a.PublishJSAtomicBatch(context.Background(), []BatchMsg{{Subject: "atomic.a", Data: 1}, {Subject: "elsewhere.b", Data: 2}}); err == nil {
		t.Fatal("want error")
	}
	if streamMsgs(t, a, "ATOMIC") != 0 {
		t.Fatal("nothing may be stored")
	}
}

func TestPublishJSAtomicBatchAppliesMiddleware(t *testing.T) {
	a := atomicApp(t, "ATOMIC", "atomic.>", true)
	a.UsePublish(func(next PublishFunc) PublishFunc {
		return func(ctx context.Context, m *RawMsg) error {
			if m.Header == nil {
				m.Header = Header{}
			}
			m.Header.Set("X-Trace", "t")
			return next(ctx, m)
		}
	})
	start(t, a)
	if _, err := a.PublishJSAtomicBatch(context.Background(), []BatchMsg{{Subject: "atomic.a", Data: 1}, {Subject: "atomic.b", Data: 2}}); err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 2; seq++ {
		if streamMsgHeader(t, a, "ATOMIC", seq, "X-Trace") != "t" {
			t.Fatal(seq)
		}
	}
}

func TestPublishJSAtomicBatchEmptyAndNotRunning(t *testing.T) {
	a, _ := New()
	if _, err := a.PublishJSAtomicBatch(context.Background(), []BatchMsg{{Subject: "x", Data: 1}}); !errors.Is(err, ErrNotRunning) {
		t.Fatal(err)
	}
	b := newApp(t, runServer(t))
	start(t, b)
	if _, err := b.PublishJSAtomicBatch(context.Background(), nil); err == nil {
		t.Fatal("empty batch must fail")
	}
}

func TestPublishJSAtomicBatchAfterFailedStart(t *testing.T) {
	a := atomicApp(t, "ATOMIC", "atomic.>", true)
	a.Consume("ATOMIC", "missing", Typed(func(context.Context, *Msg[[]byte]) error { return nil }))
	if err := a.Start(context.Background()); err == nil {
		t.Fatal("want start error")
	}
	if _, err := a.PublishJSAtomicBatch(context.Background(), []BatchMsg{{Subject: "atomic.x", Data: 1}}); !errors.Is(err, ErrNotRunning) {
		t.Fatal(err)
	}
}

func TestPublishJSAtomicBatchSingleMessageNoHeaders(t *testing.T) {
	a := startAtomic(t, "ATOMIC", "atomic.>", true)
	ack, err := a.PublishJSAtomicBatch(context.Background(), []BatchMsg{{Subject: "atomic.a", Data: 1}})
	if err != nil || ack.BatchSize != 1 {
		t.Fatal(ack, err)
	}
}
