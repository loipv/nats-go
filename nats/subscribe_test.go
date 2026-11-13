package nats

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsio "github.com/nats-io/nats.go"
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
	a.SubscribeReply("math.double", TypedReply(func(_ context.Context, m *Msg[int]) (any, error) {
		switch m.Data {
		case -1:
			return nil, Terminal(errors.New("negative"))
		case 0:
			return nil, errors.New("db down")
		case 404:
			return nil, &ServiceError{Code: 404, Description: "no such order"}
		case 7:
			return make(chan int), nil // cannot be encoded
		}
		return m.Data * 2, nil
	}))
	start(t, a)
	r, err := a.Conn().Request("math.double", []byte("21"), time.Second)
	if err != nil || string(r.Data) != "42" {
		t.Fatal(r, err)
	}
	for _, c := range []struct {
		in   int
		code int
		desc string
	}{
		{-1, 400, "bad request"},
		{0, 500, "internal error"},
		{404, 404, "no such order"},
		{7, 500, "internal error"},
	} {
		_, err := a.Request(requestCtx(t), "math.double", c.in, new(int))
		var se *ServiceError
		if !errors.As(err, &se) || se.Code != c.code || se.Description != c.desc {
			t.Errorf("request %d: %v, want %d %q", c.in, err, c.code, c.desc)
		}
	}
}

// The requester gets the generic text; the operator still sees the error.
func TestReplyHiddenErrorStillReachesOperator(t *testing.T) {
	s := runServer(t)
	onDL, dead := collect[MsgEvent]()
	a := newApp(t, s, WithOnDeadLettered(onDL))
	setupDLQ(t, a)
	a.SubscribeReply("secret", TypedReply(func(context.Context, *Msg[int]) (int, error) {
		return 0, errors.New("db down: 10.0.0.5")
	}), WithDLQ("dlq.secret"))
	start(t, a)
	_, err := a.Request(requestCtx(t), "secret", 1, new(int))
	var se *ServiceError
	if !errors.As(err, &se) || se.Description != "internal error" {
		t.Fatalf("requester got %v, want internal error", err)
	}
	waitFor(t, func() bool { return len(dead()) == 1 })
	if e := dead()[0].Err; e == nil || !strings.Contains(e.Error(), "db down: 10.0.0.5") {
		t.Fatalf("dead-letter event error = %v, want the handler error", e)
	}
	msgs := dlqMsgs(t, a)
	if len(msgs) != 1 || msgs[0].Header.Get("Nats-DLQ-Error") != "db down: 10.0.0.5" {
		t.Fatalf("DLQ copy = %v, want Nats-DLQ-Error with the handler error", msgs)
	}
}

// A typed-nil *ServiceError is still an error; nothing that prints it may panic.
func TestReplyTypedNilServiceError(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupDLQ(t, a)
	a.SubscribeReply("nil", TypedReply(func(context.Context, *Msg[int]) (int, error) {
		var se *ServiceError
		return 0, se
	}), WithDLQ("dlq.nil"))
	start(t, a)
	_, err := a.Request(requestCtx(t), "nil", 1, new(int))
	var se *ServiceError
	if !errors.As(err, &se) || se.Code != 500 || se.Description != "internal error" {
		t.Fatalf("requester got %v, want 500 internal error", err)
	}
	if msgs := dlqMsgs(t, a); len(msgs) != 1 || msgs[0].Header.Get("Nats-DLQ-Error") != "nats: service error" {
		t.Fatalf("DLQ copy = %v", msgs)
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

func TestSubscribePanicWithoutRecoverDoesNotCrash(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	got := make(chan string, 2)
	a.Subscribe("p", Typed(func(_ context.Context, m *Msg[string]) error {
		if m.Data == "boom" {
			panic("boom")
		}
		got <- m.Data
		return nil
	}))
	start(t, a)
	_ = a.Publish(context.Background(), "p", "boom")
	_ = a.Publish(context.Background(), "p", "ok")
	if recv(t, got) != "ok" {
		t.Fatal()
	}
}

func TestReplyRespondFailureEmits(t *testing.T) {
	s := runServer(t)
	onErr, errs := collect[AsyncErrorEvent]()
	a := newApp(t, s, WithOnAsyncError(onErr))
	// A response over the server's max payload cannot be sent.
	big := make([]byte, 2<<20)
	a.SubscribeReply("big", TypedReply(func(context.Context, *Msg[[]byte]) ([]byte, error) { return big, nil }))
	start(t, a)
	_ = a.Conn().PublishRequest("big", "reply.big", nil)
	waitFor(t, func() bool {
		return slices.ContainsFunc(errs(), func(e AsyncErrorEvent) bool {
			return e.Handler == "big" && e.Subject == "big" && strings.HasPrefix(e.Err.Error(), "respond: ") && errors.Is(e.Err, natsio.ErrMaxPayload)
		})
	})
}

// FuzzSubjectMatches checks subjectMatches against the server's own matching
// for every valid pattern and literal subject.
func FuzzSubjectMatches(f *testing.F) {
	for _, c := range [][2]string{
		{"a.b", "a.b"}, {"a.b", "a.c"}, {"a.b", "a.b.c"}, {"a.b.c", "a.b"}, {"a.*", "a.b"},
		{"a.*", "a.b.c"}, {"*.b", "a.b"}, {"a.>", "a.b"}, {"a.>", "a.b.c"}, {"a.>", "a"},
		{">", "a"}, {"*", "a.b"}, {"a.*.c", "a.b.c"}, {"a.*.c", "a.b.d"}, {"dlq.>", "dlq.orders"},
		{"orders", "dlq.orders"}, {"a.*.>", "a.b.c.d"}, {"*.*", "a.b"}, {"a.b*", "a.bc"}, {"a.>x", "a.>x"},
	} {
		f.Add(c[0], c[1])
	}
	f.Fuzz(func(t *testing.T, pattern, subject string) {
		if !server.IsValidSubject(pattern) || !server.IsValidLiteralSubject(subject) {
			t.Skip()
		}
		if got, want := subjectMatches(pattern, subject), server.SubjectsCollide(pattern, subject); got != want {
			t.Fatalf("subjectMatches(%q, %q) = %v, the server says %v", pattern, subject, got, want)
		}
	})
}

// BenchmarkSubscribeRoundTrip measures one core publish and its delivery to a
// Subscribe handler.
func BenchmarkSubscribeRoundTrip(b *testing.B) {
	a := newApp(b, runServer(b))
	done := make(chan struct{}, 1)
	a.Subscribe("bench.sub", Typed(func(context.Context, *Msg[order]) error { done <- struct{}{}; return nil }))
	start(b, a)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if err := a.Publish(ctx, "bench.sub", order{ID: "1"}); err != nil {
			b.Fatal(err)
		}
		<-done
	}
}
