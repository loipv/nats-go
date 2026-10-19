package nats

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

var echoInt = func(_ context.Context, m *Msg[int]) (int, error) { return m.Data, nil }

// ctlRequest sends a micro control request (PING, INFO, STATS) for the
// service name and decodes the single reply into T.
func ctlRequest[T any](t *testing.T, a *App, verb micro.Verb, name string) T {
	t.Helper()
	subject, err := micro.ControlSubject(verb, name, "")
	if err != nil {
		t.Fatal(err)
	}
	m, err := a.Conn().Request(subject, nil, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var v T
	if err := json.Unmarshal(m.Data, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

type getReq struct {
	ID string `json:"id"`
}

func (g getReq) Validate() error {
	if g.ID == "" {
		return errors.New("id required")
	}
	return nil
}

func TestServiceDiscoverableAndServes(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	svc := a.Service(micro.Config{Name: "orders", Version: "1.0.0", Description: "Order API"})
	svc.Endpoint("get", TypedReply(func(_ context.Context, m *Msg[getReq]) (order, error) { return order{ID: m.Data.ID}, nil }),
		WithEndpointSubject("orders.get"), WithEndpointMetadata(map[string]string{"auth": "none"}))
	g := svc.Group("orders.v2")
	g.Endpoint("get", TypedReply(func(_ context.Context, m *Msg[getReq]) (order, error) { return order{ID: "v2-" + m.Data.ID}, nil }))
	start(t, a)
	info := ctlRequest[micro.Info](t, a, micro.InfoVerb, "orders")
	if info.Name != "orders" || info.Version != "1.0.0" || len(info.Endpoints) != 2 {
		t.Fatal(info)
	}
	subjects := map[string]bool{}
	for _, e := range info.Endpoints {
		subjects[e.Subject] = true
	}
	if !subjects["orders.get"] || !subjects["orders.v2.get"] {
		t.Fatal(subjects)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var r order
	_, err := a.Request(ctx, "orders.v2.get", getReq{ID: "7"}, &r)
	if err != nil || r.ID != "v2-7" {
		t.Fatal(r, err)
	}
}

func TestServiceErrorsAndStats(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	svc := a.Service(micro.Config{Name: "calc", Version: "0.1.0"})
	svc.Endpoint("div", TypedReply(func(_ context.Context, m *Msg[[2]int]) (int, error) {
		if m.Data[1] == 0 {
			return 0, errors.New("division by zero")
		}
		return m.Data[0] / m.Data[1], nil
	}), WithEndpointSubject("calc.div"))
	start(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var r int
	if _, err := a.Request(ctx, "calc.div", [2]int{6, 3}, &r); err != nil || r != 2 {
		t.Fatal(r, err)
	}
	var se *ServiceError
	if _, err := a.Request(ctx, "calc.div", [2]int{1, 0}, new(int)); !errors.As(err, &se) || se.Code != 500 || se.Description != "division by zero" {
		t.Fatal(err)
	}
	if _, err := a.Request(ctx, "calc.div", "not-an-array", new(int)); !errors.As(err, &se) || se.Code != 400 {
		t.Fatal(err)
	}
	st := ctlRequest[micro.Stats](t, a, micro.StatsVerb, "calc")
	if st.Endpoints[0].NumRequests != 3 || st.Endpoints[0].NumErrors != 2 {
		t.Fatal(st.Endpoints[0])
	}
}

func TestServiceRetryAndDLQ(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupDLQ(t, a)
	var calls atomic.Int32
	svc := a.Service(micro.Config{Name: "flaky", Version: "1.0.0"})
	svc.Endpoint("do", TypedReply(func(context.Context, *Msg[int]) (int, error) { calls.Add(1); return 0, errors.New("down") }),
		WithEndpointSubject("flaky.do"), WithMaxAttempts(2), fast, WithDLQ("dlq.flaky"))
	start(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = a.Request(ctx, "flaky.do", 1, new(int))
	waitFor(t, func() bool { return len(dlqMsgs(t, a)) == 1 })
	if calls.Load() != 2 {
		t.Fatal(calls.Load())
	}
}

func TestServiceRegistrationErrors(t *testing.T) {
	cases := map[string]func(a *App){
		"no version":     func(a *App) { a.Service(micro.Config{Name: "x"}).Endpoint("e", TypedReply(echoInt)) },
		"no name":        func(a *App) { a.Service(micro.Config{Version: "1.0.0"}).Endpoint("e", TypedReply(echoInt)) },
		"empty endpoint": func(a *App) { a.Service(micro.Config{Name: "x", Version: "1.0.0"}).Endpoint("", TypedReply(echoInt)) },
		"empty prefix": func(a *App) {
			a.Service(micro.Config{Name: "x", Version: "1.0.0"}).Group("").Endpoint("e", TypedReply(echoInt))
		},
		"concurrency": func(a *App) {
			a.Service(micro.Config{Name: "x", Version: "1.0.0"}).Endpoint("e", TypedReply(echoInt), WithConcurrency(2))
		},
		"subject on reply": func(a *App) {
			a.SubscribeReply("r", TypedReply(echoInt), WithEndpointSubject("r"))
		},
		"duplicate service": func(a *App) {
			a.Service(micro.Config{Name: "x", Version: "1.0.0"})
			a.Service(micro.Config{Name: "x", Version: "1.0.0"})
		},
		"dlq on own subject": func(a *App) {
			a.Service(micro.Config{Name: "x", Version: "1.0.0"}).Endpoint("e", TypedReply(echoInt), WithEndpointSubject("dlq.e"), WithDLQ("dlq.e"))
		},
		"dlq on group subject": func(a *App) {
			a.Service(micro.Config{Name: "x", Version: "1.0.0"}).Group("dlq").Endpoint("e", TypedReply(echoInt), WithDLQ("dlq.e"))
		},
		"invalid micro name": func(a *App) {
			a.Service(micro.Config{Name: "x y", Version: "1.0.0"}).Endpoint("e", TypedReply(echoInt))
		},
	}
	for name, add := range cases {
		t.Run(name, func(t *testing.T) {
			a := newApp(t, runServer(t))
			add(a)
			if err := a.Start(context.Background()); err == nil {
				t.Fatal("want Start error")
			}
		})
	}
}

func TestServiceShutdownDuringRequest(t *testing.T) { // Review Focus #1
	s := runServer(t)
	a := newApp(t, s)
	started := make(chan struct{}, 1)
	svc := a.Service(micro.Config{Name: "slow", Version: "1.0.0"})
	svc.Endpoint("do", TypedReply(func(ctx context.Context, _ *Msg[int]) (int, error) {
		started <- struct{}{}
		<-ctx.Done()
		return 0, ctx.Err()
	}), WithEndpointSubject("slow.do"))
	start(t, a)
	go func() {
		ctx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_, _ = a.Request(ctx, "slow.do", 1, new(int))
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	begin := time.Now()
	_ = a.Shutdown(ctx)
	if time.Since(begin) > time.Second {
		t.Fatal("Shutdown overran its deadline", time.Since(begin))
	}
}

func TestServiceShutdownDrainsBufferedRequests(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	started := make(chan struct{}, 3)
	a.Service(micro.Config{Name: "drain", Version: "1.0.0"}).Endpoint("do", TypedReply(func(_ context.Context, m *Msg[int]) (int, error) {
		started <- struct{}{}
		time.Sleep(100 * time.Millisecond)
		return m.Data, nil
	}), WithEndpointSubject("drain.do"))
	start(t, a)
	// A separate requester connection, so the app's drain does not drop the replies.
	nc, err := natsio.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	errs := make(chan error, 3)
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Go(func() {
			m, err := nc.Request("drain.do", []byte{'0' + byte(i)}, 3*time.Second)
			if err == nil && m.Header.Get("Nats-Service-Error") != "" {
				err = errors.New(m.Header.Get("Nats-Service-Error"))
			}
			errs <- err
		})
	}
	<-started
	time.Sleep(20 * time.Millisecond) // let the other requests reach the subscription
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestServiceHealth(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	a.Service(micro.Config{Name: "h", Version: "1.0.0"}).Endpoint("ping", TypedReply(echoInt))
	start(t, a)
	var found bool
	for _, st := range a.Health().Handlers {
		if st.Kind == "endpoint" && st.Name == "h.ping" && st.State == "running" {
			found = true
		}
	}
	if !found || a.Health().Status != "ok" {
		t.Fatal(a.Health())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = a.Shutdown(ctx)
	for _, st := range a.Health().Handlers {
		if st.Name == "h.ping" && st.State != "stopped" {
			t.Fatal(st)
		}
	}
}

func TestEndpointIgnoresMessageWithoutReply(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	var calls atomic.Int32
	a.Service(micro.Config{Name: "svc", Version: "1.0.0"}).Endpoint("do", TypedReply(func(_ context.Context, m *Msg[int]) (int, error) {
		calls.Add(1)
		return m.Data, nil
	}))
	start(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.Publish(ctx, "do", 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal(calls.Load())
	}
	if _, err := a.Request(ctx, "do", 1, new(int)); err != nil || calls.Load() != 1 {
		t.Fatal(err, calls.Load())
	}
}

func TestEndpointHealthNamesIncludeGroups(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	svc := a.Service(micro.Config{Name: "orders", Version: "1.0.0"})
	svc.Group("v1").Endpoint("get", TypedReply(echoInt))
	svc.Group("v2").Endpoint("get", TypedReply(echoInt))
	svc.Endpoint("ping", TypedReply(echoInt))
	start(t, a)
	got := map[string]bool{}
	for _, st := range a.Health().Handlers {
		if st.Kind == "endpoint" {
			got[st.Name] = true
		}
	}
	if len(got) != 3 || !got["orders.v1.get"] || !got["orders.v2.get"] || !got["orders.ping"] {
		t.Fatal(got)
	}
}

func TestEndpointNilHandler(t *testing.T) {
	a := newApp(t, runServer(t))
	svc := a.Service(micro.Config{Name: "orders", Version: "1.0.0", Description: "d"})
	svc.Group("v2").Endpoint("get", ReplyHandler{})
	err := a.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), `service "orders"`) || !strings.Contains(err.Error(), "nil handler") {
		t.Fatal(err)
	}
}

func TestEndpointRegisteredTwice(t *testing.T) {
	s := runServer(t)
	h := TypedReply(func(_ context.Context, m *Msg[int]) (int, error) { return m.Data, nil })
	cases := map[string]func(svc *ServiceRef){
		"same name, other subject": func(svc *ServiceRef) {
			svc.Endpoint("get", h)
			svc.Endpoint("get", h, WithEndpointSubject("other"))
		},
		"dotted and nested groups": func(svc *ServiceRef) {
			svc.Group("a.b").Endpoint("get", h)
			svc.Group("a").Group("b").Endpoint("get", h)
		},
	}
	for name, register := range cases {
		t.Run(name, func(t *testing.T) {
			a := newApp(t, s)
			register(a.Service(micro.Config{Name: "svc", Version: "1.0.0"}))
			err := a.Start(context.Background())
			_ = a.Shutdown(context.Background())
			if err == nil || !strings.Contains(err.Error(), "registered twice") {
				t.Fatalf("Start err = %v, want registered twice", err)
			}
		})
	}
}
