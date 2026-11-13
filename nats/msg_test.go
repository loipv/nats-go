package nats

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func requestCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestSubscribeRespond(t *testing.T) {
	a := newApp(t, runServer(t))
	a.Subscribe("math.double", Typed(func(_ context.Context, m *Msg[int]) error {
		return m.Respond(m.Data * 2)
	}))
	start(t, a)
	var out int
	if _, err := a.Request(requestCtx(t), "math.double", 21, &out); err != nil || out != 42 {
		t.Fatal(out, err)
	}
}

func TestSubscribeRespondError(t *testing.T) {
	a := newApp(t, runServer(t))
	a.Subscribe("check", Typed(func(_ context.Context, m *Msg[int]) error {
		if m.RespondError(nil) == nil {
			t.Error("RespondError(nil) must fail")
		}
		return m.RespondError(Terminal(errors.New("negative\nstack")))
	}))
	start(t, a)
	_, err := a.Request(requestCtx(t), "check", -1, nil)
	var se *ServiceError
	if !errors.As(err, &se) || se.Code != 400 || se.Description != "negative" {
		t.Fatal(err)
	}
}

func TestRespondWithoutReplySubject(t *testing.T) {
	a := newApp(t, runServer(t))
	errs := make(chan error, 1)
	a.Subscribe("fire", Typed(func(_ context.Context, m *Msg[int]) error {
		errs <- m.Respond(1)
		return nil
	}))
	start(t, a)
	if err := a.Publish(context.Background(), "fire", 1); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, errs); err == nil {
		t.Fatal("expected an error without a reply subject")
	}
}

func TestRespondRejectedOnJetStream(t *testing.T) {
	a := newApp(t, runServer(t))
	setupStream(t, a)
	errs := make(chan error, 2)
	a.Consume("ORDERS", "billing", Typed(func(_ context.Context, m *Msg[order]) error {
		errs <- m.Respond("x")
		errs <- m.RespondError(errors.New("x"))
		return nil
	}), WithManualAck())
	start(t, a)
	if _, err := a.PublishJS(context.Background(), "orders.x", order{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := recv(t, errs); !errors.Is(err, ErrNotCore) {
			t.Fatal(err)
		}
	}
	if n := pendingAcks(t, a, "billing"); n != 1 {
		t.Fatal("message must stay unacked, pending", n)
	}
}

func TestRespondRejectedInSubscribeReply(t *testing.T) {
	a := newApp(t, runServer(t))
	errs := make(chan error, 2)
	a.SubscribeReply("echo", TypedReply(func(_ context.Context, m *Msg[int]) (int, error) {
		errs <- m.Respond(0)
		errs <- m.RespondError(errors.New("x"))
		return m.Data, nil
	}))
	start(t, a)
	var out int
	if _, err := a.Request(requestCtx(t), "echo", 7, &out); err != nil || out != 7 {
		t.Fatal(out, err)
	}
	for range 2 {
		if err := recv(t, errs); !errors.Is(err, errReplyOwned) {
			t.Fatal(err)
		}
	}
}

func TestRespondOnHandBuiltMsg(t *testing.T) {
	m := &Msg[int]{Data: 1}
	if err := m.Respond(1); !errors.Is(err, natsio.ErrMsgNotBound) {
		t.Fatal(err)
	}
	if err := m.RespondError(errors.New("x")); !errors.Is(err, natsio.ErrMsgNotBound) {
		t.Fatal(err)
	}
}

func TestMsgAckOnCoreReturnsErrNotJetStream(t *testing.T) {
	m := &Msg[[]byte]{}
	for name, err := range map[string]error{
		"Ack": m.Ack(), "Nak": m.Nak(), "NakWithDelay": m.NakWithDelay(time.Second),
		"Term": m.Term(), "InProgress": m.InProgress(),
	} {
		if !errors.Is(err, ErrNotJetStream) {
			t.Errorf("%s on a core message = %v, want ErrNotJetStream", name, err)
		}
	}
}

// manualAck starts an App whose billing handler settles each message itself
// with settle, on a consumer with the given AckWait, and publishes one message.
func manualAck(t *testing.T, ackWait time.Duration, settle func(m *Msg[order], call int32) error) (*App, *atomic.Int32) {
	t.Helper()
	a := newApp(t, runServer(t))
	a.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	a.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "billing", AckPolicy: jetstream.AckExplicitPolicy, AckWait: ackWait})
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(_ context.Context, m *Msg[order]) error {
		return settle(m, calls.Add(1))
	}), WithManualAck())
	start(t, a)
	if _, err := a.PublishJS(context.Background(), "orders.x", order{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	return a, &calls
}

// The Nak tests use an AckWait far beyond the test's deadline, so only the nak
// can cause the second delivery.
func TestMsgNak(t *testing.T) {
	a, calls := manualAck(t, 30*time.Second, func(m *Msg[order], call int32) error {
		if call == 1 {
			return m.Nak()
		}
		return m.Ack()
	})
	waitFor(t, func() bool { return calls.Load() == 2 && pendingAcks(t, a, "billing") == 0 })
}

func TestMsgNakWithDelay(t *testing.T) {
	var first, second atomic.Int64
	_, calls := manualAck(t, 30*time.Second, func(m *Msg[order], call int32) error {
		if call == 1 {
			first.Store(time.Now().UnixNano())
			return m.NakWithDelay(300 * time.Millisecond)
		}
		second.Store(time.Now().UnixNano())
		return m.Ack()
	})
	waitFor(t, func() bool { return calls.Load() == 2 })
	if gap := time.Duration(second.Load() - first.Load()); gap < 300*time.Millisecond {
		t.Fatalf("redelivered after %s, want at least 300ms", gap)
	}
}

func TestMsgTerm(t *testing.T) {
	a, calls := manualAck(t, 500*time.Millisecond, func(m *Msg[order], _ int32) error { return m.Term() })
	// Only an ack or a term clears the pending message; the handler never acks.
	waitFor(t, func() bool { return calls.Load() == 1 && pendingAcks(t, a, "billing") == 0 })
	if n := delivered(t, a, "billing"); n != 1 {
		t.Fatalf("delivered %d times, want 1", n)
	}
}

func TestMsgInProgress(t *testing.T) {
	// The handler runs for 800ms, past the consumer's 500ms AckWait, and keeps
	// the message from being redelivered by signalling progress.
	a, calls := manualAck(t, 500*time.Millisecond, func(m *Msg[order], _ int32) error {
		for range 4 {
			time.Sleep(200 * time.Millisecond)
			if err := m.InProgress(); err != nil {
				return err
			}
		}
		return m.Ack()
	})
	waitFor(t, func() bool { return calls.Load() >= 1 && pendingAcks(t, a, "billing") == 0 })
	if c, n := calls.Load(), delivered(t, a, "billing"); c != 1 || n != 1 {
		t.Fatalf("handler ran %d times, delivered %d times, want 1 and 1", c, n)
	}
}

func TestAttemptCore(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	got := make(chan int, 1)
	a.Subscribe("a", Typed(func(_ context.Context, m *Msg[[]byte]) error { got <- m.Attempt(); return nil }))
	start(t, a)
	_ = a.Conn().Publish("a", nil)
	if recv(t, got) != 1 {
		t.Fatal()
	}
}
