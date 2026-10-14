package nats

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

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
	Consume(a, "ORDERS", "billing", func(_ context.Context, m *Msg[order]) error {
		if calls.Add(1) == 1 {
			return errors.New("retry me")
		}
		return nil
	})
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
	Consume(a, "ORDERS", "billing", func(context.Context, *Msg[order]) error { calls.Add(1); return nil })
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
	Consume(a, "ORDERS", "billing", func(context.Context, *Msg[order]) error { return errors.New("ignored") }, ManualAck())
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "1"})
	waitFor(t, func() bool { return delivered(t, a, "billing") >= 2 })
}

func TestConsumeManualAckCallsAck(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	Consume(a, "ORDERS", "billing", func(_ context.Context, m *Msg[order]) error { return m.Ack() }, ManualAck())
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
	Consume(a, "ORDERS", "audit", func(_ context.Context, m *Msg[order]) error { got <- m.Data.ID; return nil })
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
	ConsumeOrdered(a, "ORDERS", func(_ context.Context, m *Msg[order]) error { got <- m.Data.ID; return nil }, FilterSubjects("orders.eu.>"))
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
	Consume(a, "ORDERS", "nope", func(context.Context, *Msg[order]) error { return nil })
	if err := a.Start(context.Background()); !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatal(err)
	}
}

func TestConsumeDuplicateRegistrationFailsStart(t *testing.T) {
	a := newApp(t, runServer(t))
	setupStream(t, a)
	fn := func(context.Context, *Msg[order]) error { return nil }
	Consume(a, "ORDERS", "billing", fn)
	Consume(a, "ORDERS", "billing", fn)
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
	Consume(a, "ORDERS", "billing", func(_ context.Context, m *Msg[order]) error {
		if m.Data.ID == "panic" {
			panic("boom")
		}
		got <- m.Data.ID
		return nil
	})
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "panic"})
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "ok"})
	if recv(t, got) != "ok" {
		t.Fatal()
	}
}

func TestConsumeStoppedWhenConsumerDeleted(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	Consume(a, "ORDERS", "billing", func(context.Context, *Msg[order]) error { return nil })
	start(t, a)
	if err := a.JetStream().DeleteConsumer(context.Background(), "ORDERS", "billing"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		st := a.runners[0].status()
		return st.State == "stopped" && st.LastError != ""
	})
}
