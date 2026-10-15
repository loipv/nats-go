package cloudevents_test

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/loipv/nats-go/nats"
	"github.com/loipv/nats-go/nats/cloudevents"
	"github.com/loipv/nats-go/nats/internal/testutil"
)

var (
	runServer = testutil.RunServer
	newApp    = testutil.NewApp
	start     = testutil.Start
)

func recv[T any](t *testing.T, ch <-chan T) T { t.Helper(); return testutil.Recv(t, ch) }

type orderEvt struct {
	ID string `json:"id"`
}

func TestRoundTrip(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s.ClientURL())
	ev := cloudevents.New(a, "https://orders.example.com")
	got := make(chan cloudevents.Event, 1)
	data := make(chan orderEvt, 1)
	cloudevents.Handle(ev, "order.created", func(_ context.Context, e cloudevents.Event, m *nats.Msg[orderEvt]) error {
		got <- e
		data <- m.Data
		return nil
	})
	start(t, a)
	at := time.Date(2026, 10, 15, 8, 30, 0, 123456789, time.UTC)
	err := ev.Publish(context.Background(), "order.created", orderEvt{ID: "o-1"}, cloudevents.WithID("e-1"), cloudevents.WithSubject("order/o-1"),
		cloudevents.WithTime(at), cloudevents.WithExtension("tenant", "acme"), cloudevents.WithDataSchema("https://schemas.example.com/order"))
	if err != nil {
		t.Fatal(err)
	}
	e := recv(t, got)
	if e.ID != "e-1" || e.Source != "https://orders.example.com" || e.Type != "order.created" || e.Subject != "order/o-1" ||
		!e.Time.Equal(at) || e.DataContentType != "application/json" || e.DataSchema != "https://schemas.example.com/order" || e.Extensions["tenant"] != "acme" {
		t.Fatalf("%+v", e)
	}
	if recv(t, data).ID != "o-1" {
		t.Fatal()
	}
	if ev.Subject("order.created") != "events.order.created" {
		t.Fatal()
	}
}

func TestBytesDataContentType(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s.ClientURL())
	ev := cloudevents.New(a, "src")
	got := make(chan []byte, 1)
	ct := make(chan string, 1)
	cloudevents.Handle(ev, "blob", func(_ context.Context, e cloudevents.Event, m *nats.Msg[[]byte]) error {
		ct <- e.DataContentType
		got <- m.Data
		return nil
	})
	start(t, a)
	_ = ev.Publish(context.Background(), "blob", []byte{0, 1, 2})
	if recv(t, ct) != "application/octet-stream" || !bytes.Equal(recv(t, got), []byte{0, 1, 2}) {
		t.Fatal()
	}
}

func TestMissingAttributesSkipsHandler(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s.ClientURL())
	ev := cloudevents.New(a, "src")
	got := make(chan string, 4)
	cloudevents.Handle(ev, "x", func(_ context.Context, e cloudevents.Event, _ *nats.Msg[int]) error { got <- e.ID; return nil })
	start(t, a)
	ctx := context.Background()
	if err := a.Publish(ctx, "events.x", 1); err != nil { // no ce-* headers
		t.Fatal(err)
	}
	if err := ev.Publish(ctx, "x", 1, cloudevents.WithID("good")); err != nil {
		t.Fatal(err)
	}
	if id := recv(t, got); id != "good" {
		t.Fatal("handler saw", id)
	}
	select {
	case id := <-got:
		t.Fatal("unexpected call", id)
	case <-time.After(100 * time.Millisecond):
	}
}

// A stream capturing the handled subject makes the PubAck inbox the Reply of
// the core delivery; Handle must not answer it.
func TestInvalidEventDoesNotDisturbPubAck(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s.ClientURL(), func(a *nats.App) {
		a.Stream(jetstream.StreamConfig{Name: "EVT", Subjects: []string{"events.>"}})
	})
	ev := cloudevents.New(a, "src")
	var called atomic.Int32
	cloudevents.Handle(ev, "x", func(context.Context, cloudevents.Event, *nats.Msg[int]) error { called.Add(1); return nil })
	start(t, a)
	for i := 0; i < 5; i++ {
		ack, err := a.PublishJS(context.Background(), "events.x", 1) // no ce-* headers
		if err != nil || ack == nil || ack.Stream != "EVT" {
			t.Fatal(i, ack, err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if called.Load() != 0 {
		t.Fatal("handler called")
	}
}

func TestCustomSubjectFuncAndJSDedup(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s.ClientURL(), func(a *nats.App) {
		a.Stream(jetstream.StreamConfig{Name: "EVT", Subjects: []string{"com.>"}})
	})
	ev := cloudevents.New(a, "src", cloudevents.SubjectFunc(func(t string) string { return strings.ReplaceAll("com.example."+t, "/", ".") }))
	start(t, a)
	if ev.Subject("order/created") != "com.example.order.created" {
		t.Fatal(ev.Subject("order/created"))
	}
	ack1, err := ev.PublishJS(context.Background(), "order/created", 1, cloudevents.WithID("same"))
	ack2, _ := ev.PublishJS(context.Background(), "order/created", 1, cloudevents.WithID("same"))
	if err != nil || ack1.Duplicate || !ack2.Duplicate {
		t.Fatal(err, ack1, ack2)
	}
}

func TestInvalidExtensionAndEmptyType(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s.ClientURL())
	ev := cloudevents.New(a, "src")
	start(t, a)
	if err := ev.Publish(context.Background(), "t", 1, cloudevents.WithExtension("Bad-Name", "v")); err == nil {
		t.Fatal("bad extension")
	}
	for _, name := range []string{"", "abcdefghij0123456789x"} {
		if err := ev.Publish(context.Background(), "t", 1, cloudevents.WithExtension(name, "v")); err == nil {
			t.Fatal("bad extension length", name)
		}
	}
	if err := ev.Publish(context.Background(), "t", 1, cloudevents.WithExtension("abcdefghij0123456789", "v")); err != nil {
		t.Fatal("20-char extension", err)
	}
	if err := ev.Publish(context.Background(), "t", 1, cloudevents.WithExtension("source", "v")); err == nil {
		t.Fatal("reserved name")
	}
	if err := ev.Publish(context.Background(), "", 1); err == nil {
		t.Fatal("empty type")
	}
}

func TestFromHeaderParsesFractionalTime(t *testing.T) {
	h := nats.Header{"ce-specversion": {"1.0"}, "ce-id": {"1"}, "ce-source": {"s"}, "ce-type": {"t"}, "ce-time": {"2026-10-15T08:30:00.5+07:00"}, "ce-region": {"eu"}}
	e, err := cloudevents.FromHeader(h)
	if err != nil || !e.Time.Equal(time.Date(2026, 10, 15, 1, 30, 0, 5e8, time.UTC)) || e.Extensions["region"] != "eu" {
		t.Fatal(e, err)
	}
	h["ce-specversion"] = []string{"0.3"}
	if _, err := cloudevents.FromHeader(h); err == nil {
		t.Fatal("unsupported specversion")
	}
}

func TestNewPanicsOnEmptySource(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want panic")
		}
	}()
	a, _ := nats.New()
	cloudevents.New(a, "")
}

func TestWithHeaderMergeLibWins(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s.ClientURL())
	ev := cloudevents.New(a, "src")
	hdr := make(chan nats.Header, 1)
	nats.Subscribe(a, "events.h", func(_ context.Context, m *nats.Msg[[]byte]) error { hdr <- m.Header; return nil })
	start(t, a)
	err := ev.Publish(context.Background(), "h", 1, cloudevents.WithHeader(nats.Header{"X-Trace": {"t1"}, "ce-source": {"evil"}}))
	if err != nil {
		t.Fatal(err)
	}
	h := recv(t, hdr)
	if h.Get("X-Trace") != "t1" || h.Get("ce-source") != "src" || h.Get("ce-specversion") != "1.0" {
		t.Fatal(h)
	}
}

func TestHandleEmptyTypeRegistrationError(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s.ClientURL())
	ev := cloudevents.New(a, "src")
	cloudevents.Handle(ev, "", func(context.Context, cloudevents.Event, *nats.Msg[int]) error { return nil })
	if err := a.Start(context.Background()); err == nil {
		t.Fatal("want registration error")
	}
}
