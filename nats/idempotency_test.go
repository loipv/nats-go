package nats

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
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

func TestIdempotencySkipsDuplicates(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	// Nats-Msg-Id would be deduplicated by the stream itself, so key on another header.
	a.Idempotency(NewMemoryStore(), orderKey)
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error { calls.Add(1); return nil }), WithIdempotency())
	start(t, a)
	h := Header{"X-Order-Id": []string{"same"}}
	_, _ = a.PublishJS(context.Background(), "orders.a", order{ID: "1"}, WithHeader(h))
	_, _ = a.PublishJS(context.Background(), "orders.b", order{ID: "1"}, WithHeader(h))
	waitFor(t, func() bool { return pendingAcks(t, a, "billing") == 0 && delivered(t, a, "billing") == 2 })
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
}

func TestIdempotencyAbortsOnError(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	a.Idempotency(NewMemoryStore())
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error {
		if calls.Add(1) == 1 {
			return errors.New("retry")
		}
		return nil
	}), fast, WithIdempotency())
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"}, WithMsgID("x-1"))
	waitFor(t, func() bool { return calls.Load() == 2 && pendingAcks(t, a, "billing") == 0 })
}

func TestIdempotencyReplyDuplicateGets400(t *testing.T) { // Review Focus #3
	s := runServer(t)
	a := newApp(t, s)
	a.Idempotency(NewMemoryStore())
	a.SubscribeReply("once", TypedReply(func(_ context.Context, m *Msg[int]) (int, error) { return m.Data, nil }), WithQueue("q"), WithIdempotency())
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

func TestIdempotencyReleasesClaimOnPanic(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	a.Use(Recover())
	a.Idempotency(NewMemoryStore())
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error {
		if calls.Add(1) == 1 {
			panic("boom")
		}
		return nil
	}), fast, WithMaxAttempts(3), WithIdempotency())
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"}, WithMsgID("p-1"))
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

func TestIdempotencyLogsToAppLogger(t *testing.T) {
	s := runServer(t)
	var buf syncBuf
	a := newApp(t, s, WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
	setupStream(t, a)
	a.Idempotency(completeFails{NewMemoryStore()})
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error { calls.Add(1); return nil }), WithIdempotency())
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"}, WithMsgID("l-1"))
	waitFor(t, func() bool { return calls.Load() == 1 && pendingAcks(t, a, "billing") == 0 })
	waitFor(t, func() bool { return strings.Contains(buf.String(), "idempotency complete failed") })
}

func TestIdempotencyManualAckAcksDuplicate(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	a.Idempotency(NewMemoryStore(), orderKey)
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(_ context.Context, m *Msg[order]) error { calls.Add(1); return m.Ack() }), WithManualAck(), WithIdempotency())
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

func TestIdempotencyReplyDuplicateSkipsDLQ(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupDLQ(t, a)
	a.Idempotency(NewMemoryStore())
	a.SubscribeReply("once", TypedReply(func(_ context.Context, m *Msg[int]) (int, error) { return m.Data, nil }), WithDLQ("dlq.once"), WithQueue("q"), WithIdempotency())
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

// abortFails is a store whose Abort always fails.
type abortFails struct{ IdempotencyStore }

func (abortFails) Abort(context.Context, string) error { return errors.New("store down") }

func TestIdempotencyAbortFailureEmits(t *testing.T) {
	got := make(chan AsyncErrorEvent, 1)
	log := &sink{Logger: slog.New(slog.DiscardHandler), handler: "h", ev: &events{asyncError: []func(AsyncErrorEvent){func(e AsyncErrorEvent) { got <- e }}}}
	h := idempotent(idemConfig{store: abortFails{NewMemoryStore()}, key: func(*RawMsg) string { return "k" }}.over(idemDefaults), "s")(
		func(context.Context, *RawMsg) error { return errors.New("handler failed") })
	if err := h(withSink(context.Background(), log), &RawMsg{Subject: "s"}); err == nil || err.Error() != "handler failed" {
		t.Fatalf("err = %v, want the handler error", err)
	}
	e := recv(t, got)
	if e.Handler != "h" || e.Subject != "s" || e.Err.Error() != "idempotency abort: store down" {
		t.Fatalf("event = %+v", e)
	}
}

// A duplicate the handler chain already acked is not acked again with an
// error: ErrMsgAlreadyAckd is expected there.
func TestIdempotencyManualAckDuplicateAlreadyAcked(t *testing.T) {
	s := runServer(t)
	onErr, errs := collect[AsyncErrorEvent]()
	a := newApp(t, s, WithOnAsyncError(onErr))
	setupStream(t, a)
	var seen atomic.Int32
	a.Use(func(next Handler) Handler {
		return func(ctx context.Context, m *RawMsg) error {
			defer seen.Add(1)
			_ = jsMsgFrom(ctx).Ack()
			return next(ctx, m)
		}
	})
	a.Idempotency(NewMemoryStore(), WithIdempotencyKey(func(*RawMsg) string { return "same" }))
	a.Consume("ORDERS", "billing", Typed(nopOrder), WithManualAck(), WithIdempotency())
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.a", order{ID: "1"})
	_, _ = a.PublishJS(context.Background(), "orders.b", order{ID: "1"})
	waitFor(t, func() bool { return seen.Load() == 2 })
	time.Sleep(100 * time.Millisecond) // the worker acks after the chain returns
	if got := errs(); len(got) != 0 {
		t.Fatalf("async errors %v, want none", got)
	}
}

// orderKey keys on X-Order-Id: the stream itself drops a repeated Nats-Msg-Id.
var orderKey = WithIdempotencyKey(func(m *RawMsg) string { return m.Header.Get("X-Order-Id") })

func orderHeader(id string) PubOption { return WithHeader(Header{"X-Order-Id": []string{id}}) }

// recordingStore is a MemoryStore that records the keys passed to Begin.
type recordingStore struct {
	*MemoryStore
	mu   sync.Mutex
	keys []string
}

func newRecordingStore() *recordingStore { return &recordingStore{MemoryStore: NewMemoryStore()} }

func (s *recordingStore) Begin(ctx context.Context, key string, ttl time.Duration) error {
	s.mu.Lock()
	s.keys = append(s.keys, key)
	s.mu.Unlock()
	return s.MemoryStore.Begin(ctx, key, ttl)
}

func (s *recordingStore) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.keys)
}

func addConsumer(a *App, name string) {
	a.Consumer("ORDERS", jetstream.ConsumerConfig{
		Durable:    name,
		AckPolicy:  jetstream.AckExplicitPolicy,
		AckWait:    500 * time.Millisecond,
		MaxDeliver: 5,
	})
}

func counting(n *atomic.Int32) MsgHandler {
	return Typed(func(context.Context, *Msg[order]) error { n.Add(1); return nil })
}

func TestIdempotencyPerHandler(t *testing.T) {
	a := newApp(t, runServer(t))
	setupStream(t, a)
	addConsumer(a, "shipping")
	a.Idempotency(NewMemoryStore(), orderKey)
	var billing, shipping atomic.Int32
	a.Consume("ORDERS", "billing", counting(&billing), WithIdempotency())
	a.Consume("ORDERS", "shipping", counting(&shipping), WithIdempotency())
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.a", order{ID: "1"}, orderHeader("o1"))
	_, _ = a.PublishJS(context.Background(), "orders.b", order{ID: "1"}, orderHeader("o1"))
	waitFor(t, func() bool {
		return pendingAcks(t, a, "billing") == 0 && delivered(t, a, "billing") == 2 &&
			pendingAcks(t, a, "shipping") == 0 && delivered(t, a, "shipping") == 2
	})
	if billing.Load() != 1 || shipping.Load() != 1 {
		t.Fatalf("billing ran %d times, shipping %d, want 1 each", billing.Load(), shipping.Load())
	}
}

func TestIdempotencyDefaultScopes(t *testing.T) {
	a := newApp(t, runServer(t))
	setupStream(t, a)
	rec := newRecordingStore()
	a.Idempotency(rec, orderKey)
	reply := TypedReply(func(_ context.Context, m *Msg[int]) (int, error) { return m.Data, nil })
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", counting(&calls), WithIdempotency())
	a.SubscribeReply("price.get", reply, WithQueue("pricer"), WithIdempotency())
	svc := a.Service(micro.Config{Name: "svc", Version: "1.0.0"})
	svc.Group("a").Endpoint("get", reply, WithIdempotency())
	svc.Group("b").Endpoint("get", reply, WithIdempotency())
	a.Subscribe("audit.x", Typed(func(context.Context, *Msg[int]) error { return nil }), WithIdempotency(), WithIdempotencyScope("custom"))
	start(t, a)
	ctx := context.Background()
	_, _ = a.PublishJS(ctx, "orders.x", order{ID: "1"}, orderHeader("k"))
	for _, subj := range []string{"price.get", "a.get", "b.get"} {
		if _, err := a.Request(ctx, subj, 1, new(int), orderHeader("k")); err != nil {
			t.Fatal(subj, err)
		}
	}
	_ = a.Publish(ctx, "audit.x", 1, orderHeader("k"))
	want := []string{"ORDERS/billing\x00k", "custom\x00k", "price.get@pricer\x00k", "svc.a.get\x00k", "svc.b.get\x00k"}
	waitFor(t, func() bool { return len(rec.recorded()) == len(want) })
	got := rec.recorded()
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("keys %q, want %q", got, want)
	}
}

func TestIdempotencyFieldOverride(t *testing.T) {
	a := newApp(t, runServer(t))
	setupStream(t, a)
	stA, stB := newRecordingStore(), newRecordingStore()
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", counting(&calls), WithIdempotency(WithIdempotencyStore(stB)))
	a.Idempotency(stA, orderKey) // after the registration: still supplies the key
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"}, orderHeader("o1"))
	waitFor(t, func() bool { return pendingAcks(t, a, "billing") == 0 && delivered(t, a, "billing") == 1 })
	if got := stB.recorded(); !slices.Equal(got, []string{"ORDERS/billing\x00o1"}) {
		t.Fatalf("handler store keys %q", got)
	}
	if got := stA.recorded(); len(got) != 0 {
		t.Fatalf("App store keys %q, want none", got)
	}
}

func TestIdempotencyInnermost(t *testing.T) {
	a := newApp(t, runServer(t))
	setupStream(t, a)
	a.Idempotency(NewMemoryStore(), orderKey)
	var dups atomic.Int32
	a.Use(func(next Handler) Handler {
		return func(ctx context.Context, m *RawMsg) error {
			err := next(ctx, m)
			if errors.Is(err, ErrDuplicate) {
				dups.Add(1)
			}
			return err
		}
	})
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", counting(&calls), WithIdempotency())
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.a", order{ID: "1"}, orderHeader("o1"))
	_, _ = a.PublishJS(context.Background(), "orders.b", order{ID: "1"}, orderHeader("o1"))
	waitFor(t, func() bool { return dups.Load() == 1 })
}

func TestIdempotencyEmptyKeyNotDeduplicated(t *testing.T) { // Review Focus #1
	a := newApp(t, runServer(t))
	setupStream(t, a)
	rec := newRecordingStore()
	a.Idempotency(rec)
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", counting(&calls), WithIdempotency())
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.a", order{ID: "1"})
	_, _ = a.PublishJS(context.Background(), "orders.b", order{ID: "2"})
	waitFor(t, func() bool { return pendingAcks(t, a, "billing") == 0 && delivered(t, a, "billing") == 2 })
	if calls.Load() != 2 || len(rec.recorded()) != 0 {
		t.Fatalf("calls %d, keys %q; want 2 calls, no keys", calls.Load(), rec.recorded())
	}
}

func TestIdempotencyOrderedWithScope(t *testing.T) { // Review Focus #3
	a := newApp(t, runServer(t))
	a.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	got := make(chan string, 4)
	a.ConsumeOrdered("ORDERS", Typed(func(_ context.Context, m *Msg[order]) error { got <- m.Header.Get("X-Order-Id"); return nil }),
		WithIdempotency(WithIdempotencyStore(NewMemoryStore()), orderKey), WithIdempotencyScope("ord"))
	start(t, a)
	for _, id := range []string{"o1", "o1", "o2"} {
		_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: id}, orderHeader(id))
	}
	if first, second := recv(t, got), recv(t, got); first != "o1" || second != "o2" {
		t.Fatalf("handled %q then %q, want o1 then o2", first, second)
	}
}
