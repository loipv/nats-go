package nats

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testStoreContract(t *testing.T, newStore func(t *testing.T) IdempotencyStore) {
	ctx := context.Background()
	t.Run("begin complete duplicate", func(t *testing.T) {
		s := newStore(t)
		if err := s.Begin(ctx, "k1", time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := s.Begin(ctx, "k1", time.Minute); !errors.Is(err, ErrInProgress) {
			t.Fatal(err)
		}
		if err := s.Complete(ctx, "k1", time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := s.Begin(ctx, "k1", time.Minute); !errors.Is(err, ErrDuplicate) {
			t.Fatal(err)
		}
	})
	t.Run("abort allows retry", func(t *testing.T) {
		s := newStore(t)
		_ = s.Begin(ctx, "k2", time.Minute)
		if err := s.Abort(ctx, "k2"); err != nil {
			t.Fatal(err)
		}
		if err := s.Begin(ctx, "k2", time.Minute); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("claim expires", func(t *testing.T) {
		s := newStore(t)
		_ = s.Begin(ctx, "k3", time.Second)
		waitFor(t, func() bool { return s.Begin(ctx, "k3", time.Second) == nil })
	})
	t.Run("keys with any characters", func(t *testing.T) {
		s := newStore(t)
		if err := s.Begin(ctx, "order #1/ü*>", time.Minute); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMemoryStoreContract(t *testing.T) {
	testStoreContract(t, func(*testing.T) IdempotencyStore { return NewMemoryStore() })
}

func TestIdempotentSkipsDuplicates(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	// Nats-Msg-Id would be deduplicated by the stream itself, so key on another header.
	a.Use(Idempotent(NewMemoryStore(), IdempotencyKey(func(m *RawMsg) string { return m.Header.Get("X-Order-Id") })))
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error { calls.Add(1); return nil }))
	start(t, a)
	h := Header{"X-Order-Id": []string{"same"}}
	_, _ = a.PublishJS(context.Background(), "orders.a", order{ID: "1"}, WithHeader(h))
	_, _ = a.PublishJS(context.Background(), "orders.b", order{ID: "1"}, WithHeader(h))
	waitFor(t, func() bool { return pendingAcks(t, a, "billing") == 0 && delivered(t, a, "billing") == 2 })
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
}

func TestIdempotentAbortsOnError(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	a.Use(Idempotent(NewMemoryStore()))
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error {
		if calls.Add(1) == 1 {
			return errors.New("retry")
		}
		return nil
	}), fast)
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"}, MsgID("x-1"))
	waitFor(t, func() bool { return calls.Load() == 2 && pendingAcks(t, a, "billing") == 0 })
}

func TestIdempotentReplyDuplicateGets400(t *testing.T) { // Review Focus #3
	s := runServer(t)
	a := newApp(t, s)
	a.Use(Idempotent(NewMemoryStore()))
	a.Reply("once", TypedReply(func(_ context.Context, m *Msg[int]) (int, error) { return m.Data, nil }))
	start(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h := Header{"Nats-Msg-Id": []string{"r-1"}}
	if _, err := a.Request(ctx, "once", 1, new(int), WithHeader(h)); err != nil {
		t.Fatal(err)
	}
	_, err := a.Request(ctx, "once", 1, new(int), WithHeader(h))
	var se *ServiceError
	if !errors.As(err, &se) || se.Code != 400 {
		t.Fatal(err)
	}
}

func TestIdempotentPanicsOnInvalid(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want panic")
		}
	}()
	Idempotent(nil)
}

func TestIdempotentReleasesClaimOnPanic(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	a.Use(Recover(), Idempotent(NewMemoryStore()))
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error {
		if calls.Add(1) == 1 {
			panic("boom")
		}
		return nil
	}), fast, MaxAttempts(3))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"}, MsgID("p-1"))
	waitFor(t, func() bool { return calls.Load() == 2 && pendingAcks(t, a, "billing") == 0 })
}

// completeFails is a store whose Complete always fails.
type completeFails struct{ IdempotencyStore }

func (completeFails) Complete(context.Context, string, time.Duration) error {
	return errors.New("store down")
}

// syncBuf is a bytes.Buffer safe for concurrent log writes and test reads.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func TestIdempotentLogsToAppLogger(t *testing.T) {
	s := runServer(t)
	var buf syncBuf
	a := newApp(t, s, WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
	setupStream(t, a)
	a.Use(Idempotent(completeFails{NewMemoryStore()}))
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error { calls.Add(1); return nil }))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"}, MsgID("l-1"))
	waitFor(t, func() bool { return calls.Load() == 1 && pendingAcks(t, a, "billing") == 0 })
	waitFor(t, func() bool { return strings.Contains(buf.String(), "idempotency complete failed") })
}

func TestIdempotentManualAckAcksDuplicate(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	a.Use(Idempotent(NewMemoryStore(), IdempotencyKey(func(m *RawMsg) string { return m.Header.Get("X-Order-Id") })))
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(_ context.Context, m *Msg[order]) error { calls.Add(1); return m.Ack() }), ManualAck())
	start(t, a)
	h := Header{"X-Order-Id": []string{"same"}}
	_, _ = a.PublishJS(context.Background(), "orders.a", order{ID: "1"}, WithHeader(h))
	_, _ = a.PublishJS(context.Background(), "orders.b", order{ID: "1"}, WithHeader(h))
	waitFor(t, func() bool { return pendingAcks(t, a, "billing") == 0 && delivered(t, a, "billing") == 2 })
	time.Sleep(700 * time.Millisecond) // past AckWait: an unacked duplicate would be redelivered
	if calls.Load() != 1 || delivered(t, a, "billing") != 2 {
		t.Fatal(calls.Load(), delivered(t, a, "billing"))
	}
}

func TestIdempotentReplyDuplicateSkipsDLQ(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupDLQ(t, a)
	a.Use(Idempotent(NewMemoryStore()))
	a.Reply("once", TypedReply(func(_ context.Context, m *Msg[int]) (int, error) { return m.Data, nil }), DLQ("dlq.once"))
	start(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h := Header{"Nats-Msg-Id": []string{"r-1"}}
	if _, err := a.Request(ctx, "once", 1, new(int), WithHeader(h)); err != nil {
		t.Fatal(err)
	}
	_, err := a.Request(ctx, "once", 1, new(int), WithHeader(h))
	var se *ServiceError
	if !errors.As(err, &se) || se.Code != 400 {
		t.Fatal(err)
	}
	// The DLQ publish happens before the error response, so it would be visible now.
	if n := len(dlqMsgs(t, a)); n != 0 {
		t.Fatal("duplicate dead-lettered", n)
	}
}
