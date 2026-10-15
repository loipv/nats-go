package nats

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/micro"
)

func TestSubscribeWildcard(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	got := make(chan string, 2)
	a.Subscribe("orders.*", Typed(func(_ context.Context, m *Msg[[]byte]) error { got <- m.Subject; return nil }))
	start(t, a)
	_ = a.Conn().Publish("orders.created", nil)
	_ = a.Conn().Publish("orders.eu.created", nil)
	if s := recv(t, got); s != "orders.created" {
		t.Fatal(s)
	}
	expectNone(t, got, 200*time.Millisecond)
}

func TestSubscribeQueueGroupDeliversOnce(t *testing.T) {
	s := runServer(t)
	var n atomic.Int32
	for range 2 {
		a := newApp(t, s)
		a.Subscribe("jobs", Typed(func(context.Context, *Msg[[]byte]) error { n.Add(1); return nil }), WithQueue("w"))
		start(t, a)
	}
	pub := newApp(t, s)
	start(t, pub)
	for range 10 {
		_ = pub.Conn().Publish("jobs", nil)
	}
	_ = pub.Conn().Flush()
	time.Sleep(300 * time.Millisecond)
	if n.Load() != 10 {
		t.Fatal(n.Load())
	}
}

func TestReplyRespondsAndMapsErrors(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	a.SubscribeReply("math.double", TypedReply(func(_ context.Context, m *Msg[int]) (int, error) {
		if m.Data < 0 {
			return 0, Terminal(errors.New("negative"))
		}
		if m.Data == 0 {
			return 0, errors.New("boom")
		}
		return m.Data * 2, nil
	}))
	start(t, a)
	r, err := a.Conn().Request("math.double", []byte("21"), time.Second)
	if err != nil || string(r.Data) != "42" {
		t.Fatal(r, err)
	}
	r, _ = a.Conn().Request("math.double", []byte("-1"), time.Second)
	if r.Header.Get("Nats-Service-Error-Code") != "400" || r.Header.Get("Nats-Service-Error") != "negative" {
		t.Fatal(r.Header)
	}
	r, _ = a.Conn().Request("math.double", []byte("0"), time.Second)
	if r.Header.Get("Nats-Service-Error-Code") != "500" {
		t.Fatal(r.Header)
	}
}

func TestSubscribeTimeoutSetsDeadline(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	got := make(chan bool, 1)
	a.Subscribe("t", Typed(func(ctx context.Context, _ *Msg[[]byte]) error {
		_, ok := ctx.Deadline()
		got <- ok
		return nil
	}), WithTimeout(time.Second))
	start(t, a)
	_ = a.Conn().Publish("t", nil)
	if !recv(t, got) {
		t.Fatal("handler ctx must have deadline")
	}
}

func TestInvalidSubjectFailsStart(t *testing.T) {
	for _, subj := range []string{"", "foo..bar"} {
		a := newApp(t, runServer(t))
		a.Subscribe(subj, Typed(func(context.Context, *Msg[[]byte]) error { return nil }))
		if err := a.Start(context.Background()); err == nil {
			t.Fatalf("subject %q must fail Start", subj)
		}
		if a.running() {
			t.Fatal("must not be running")
		}
	}
}

func TestPanicSendsInternalError(t *testing.T) {
	boom := TypedReply(func(context.Context, *Msg[int]) (int, error) { panic("kaboom") })
	for name, register := range map[string]func(*App){
		"reply with Recover": func(a *App) { a.Use(Recover()); a.SubscribeReply("boom", boom) },
		"reply":              func(a *App) { a.SubscribeReply("boom", boom) },
		"endpoint":           func(a *App) { a.Service(micro.Config{Name: "svc", Version: "1.0.0"}).Endpoint("boom", boom) },
		"endpoint with Recover": func(a *App) {
			a.Use(Recover())
			a.Service(micro.Config{Name: "svc", Version: "1.0.0"}).Endpoint("boom", boom)
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := runServer(t)
			a := newApp(t, s)
			register(a)
			start(t, a)
			r, err := a.Conn().Request("boom", []byte("1"), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if r.Header.Get("Nats-Service-Error-Code") != "500" || r.Header.Get("Nats-Service-Error") != "internal error" {
				t.Fatalf("%q", r.Header)
			}
		})
	}
}

func TestSubjectMatches(t *testing.T) {
	for _, c := range []struct {
		pattern, subject string
		want             bool
	}{
		{"a.b", "a.b", true},
		{"a.b", "a.c", false},
		{"a.b", "a.b.c", false},
		{"a.b.c", "a.b", false},
		{"a.*", "a.b", true},
		{"a.*", "a.b.c", false},
		{"*.b", "a.b", true},
		{"a.>", "a.b", true},
		{"a.>", "a.b.c", true},
		{"a.>", "a", false},
		{">", "a", true},
		{"*", "a.b", false},
		{"a.*.c", "a.b.c", true},
		{"a.*.c", "a.b.d", false},
		{"dlq.>", "dlq.orders", true},
		{"orders", "dlq.orders", false},
	} {
		if got := subjectMatches(c.pattern, c.subject); got != c.want {
			t.Errorf("subjectMatches(%q, %q) = %v, want %v", c.pattern, c.subject, got, c.want)
		}
	}
}
