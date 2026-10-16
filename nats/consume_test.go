package nats

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func setupStream(t *testing.T, a *App) {
	t.Helper()
	a.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	a.Consumer("ORDERS", jetstream.ConsumerConfig{
		Durable:    "billing",
		AckPolicy:  jetstream.AckExplicitPolicy,
		AckWait:    500 * time.Millisecond,
		MaxDeliver: 5,
	})
}

func consumerInfo(t *testing.T, a *App, name string) *jetstream.ConsumerInfo {
	t.Helper()
	c, err := a.JetStream().Consumer(context.Background(), "ORDERS", name)
	if err != nil {
		t.Fatal(err)
	}
	info, err := c.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info
}

// delivered counts deliveries, redeliveries included.
func delivered(t *testing.T, a *App, name string) uint64 {
	return consumerInfo(t, a, name).Delivered.Consumer
}

func pendingAcks(t *testing.T, a *App, name string) int {
	return consumerInfo(t, a, name).NumAckPending
}

func TestConsumeAutoAck(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(_ context.Context, m *Msg[order]) error {
		if calls.Add(1) == 1 {
			return errors.New("retry me")
		}
		return nil
	}))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "fail"})
	waitFor(t, func() bool { return calls.Load() >= 1 })
	waitFor(t, func() bool { return pendingAcks(t, a, "billing") == 0 && delivered(t, a, "billing") == 2 })
}

func TestConsumePoisonPayloadIsTerminated(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error { calls.Add(1); return nil }))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", []byte("{not json"))
	time.Sleep(1500 * time.Millisecond)
	if delivered(t, a, "billing") != 1 || calls.Load() != 0 {
		t.Fatal("poison must be Term'd once, handler not called")
	}
}

func TestConsumeManualAck(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error { return errors.New("ignored") }), WithManualAck())
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	waitFor(t, func() bool { return delivered(t, a, "billing") >= 2 })
}

func TestConsumeManualAckCallsAck(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	a.Consume("ORDERS", "billing", Typed(func(_ context.Context, m *Msg[order]) error { return m.Ack() }), WithManualAck())
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	waitFor(t, func() bool { return delivered(t, a, "billing") == 1 && pendingAcks(t, a, "billing") == 0 })
}

func TestConsumePushDetected(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	a.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	a.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "audit", DeliverSubject: "deliver.audit", AckPolicy: jetstream.AckExplicitPolicy})
	got := make(chan string, 1)
	a.Consume("ORDERS", "audit", Typed(func(_ context.Context, m *Msg[order]) error { got <- m.Data.ID; return nil }))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "p1"})
	if recv(t, got) != "p1" {
		t.Fatal()
	}
}

func TestConsumeOrdered(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	got := make(chan string, 3)
	a.ConsumeOrdered("ORDERS", Typed(func(_ context.Context, m *Msg[order]) error { got <- m.Data.ID; return nil }), WithFilterSubjects("orders.eu.>"))
	start(t, a)
	for _, subj := range []string{"orders.eu.a", "orders.us.b", "orders.eu.c"} {
		_, _ = a.PublishJS(context.Background(), subj, order{ID: subj})
	}
	if recv(t, got) != "orders.eu.a" || recv(t, got) != "orders.eu.c" {
		t.Fatal("order/filter wrong")
	}
}

func TestConsumeMissingConsumerFailsStart(t *testing.T) {
	a := newApp(t, runServer(t))
	a.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	a.Consume("ORDERS", "nope", Typed(func(context.Context, *Msg[order]) error { return nil }))
	if err := a.Start(context.Background()); !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatal(err)
	}
}

func TestConsumeDuplicateRegistrationFailsStart(t *testing.T) {
	a := newApp(t, runServer(t))
	setupStream(t, a)
	fn := func(context.Context, *Msg[order]) error { return nil }
	a.Consume("ORDERS", "billing", Typed(fn))
	a.Consume("ORDERS", "billing", Typed(fn))
	if err := a.Start(context.Background()); err == nil {
		t.Fatal("duplicate consumer must fail")
	}
}

func TestConsumePanicWithRecoverKeepsWorking(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	a.Use(Recover())
	got := make(chan string, 2)
	a.Consume("ORDERS", "billing", Typed(func(_ context.Context, m *Msg[order]) error {
		if m.Data.ID == "panic" {
			panic("boom")
		}
		got <- m.Data.ID
		return nil
	}))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "panic"})
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "ok"})
	if recv(t, got) != "ok" {
		t.Fatal()
	}
}

// A deleted consumer makes the handler restart, never stop while
// the App runs.
func TestConsumeRestartingWhenConsumerDeleted(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s, WithRestartBackoff(func(int) time.Duration { return time.Hour }))
	setupStream(t, a)
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error { return nil }))
	start(t, a)
	// The server drops a pull request that is not waiting yet when the consumer
	// is deleted, so detection would take a heartbeat timeout.
	waitFor(t, func() bool { return consumerInfo(t, a, "billing").NumWaiting >= 1 })
	if err := a.JetStream().DeleteConsumer(context.Background(), "ORDERS", "billing"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		st := a.runners[0].status()
		return st.State == "restarting" && st.LastError != ""
	})
}

func TestConsumePanicWithoutRecoverIsRetried(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	var calls atomic.Int32
	a.Consume("ORDERS", "billing", Typed(func(_ context.Context, m *Msg[order]) error {
		if calls.Add(1) == 1 {
			panic("first try")
		}
		return nil
	}), WithBackoff(func(int) time.Duration { return 50 * time.Millisecond }))
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	waitFor(t, func() bool { return calls.Load() == 2 && pendingAcks(t, a, "billing") == 0 })
}

func TestConsumeTermCarriesReason(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error {
		return Terminal(errors.New("bad input\nsecret stack"))
	}))
	start(t, a)
	reasons := make(chan string, 1)
	sub, err := a.Conn().Subscribe("$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.ORDERS.billing", func(m *natsio.Msg) {
		var adv struct {
			Reason string `json:"reason"`
		}
		_ = json.Unmarshal(m.Data, &adv)
		reasons <- adv.Reason
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if _, err := a.PublishJS(context.Background(), "orders.x", order{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	if r := recv(t, reasons); r != "bad input" {
		t.Fatalf("reason = %q", r)
	}
}

// A ManualAck handler that acks and then reports a duplicate is not acked
// twice with an error.
func TestManualAckDuplicateAfterAck(t *testing.T) {
	s := runServer(t)
	onErr, errs := collect[AsyncErrorEvent]()
	a := newApp(t, s, WithOnAsyncError(onErr))
	setupStream(t, a)
	handled := make(chan struct{}, 1)
	a.Consume("ORDERS", "billing", Typed(func(_ context.Context, m *Msg[order]) error {
		defer func() { handled <- struct{}{} }()
		if err := m.Ack(); err != nil {
			return err
		}
		return Terminal(ErrDuplicate)
	}), WithManualAck())
	start(t, a)
	if _, err := a.PublishJS(context.Background(), "orders.x", order{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	recv(t, handled)
	time.Sleep(100 * time.Millisecond) // the ack runs after the handler returns
	if got := errs(); len(got) != 0 {
		t.Fatalf("async errors %v, want none", got)
	}
}
