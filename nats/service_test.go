package nats

import (
	"context"
	"encoding/json"
	"errors"
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
	svc := Service(a, micro.Config{Name: "orders", Version: "1.0.0", Description: "Order API"})
	Endpoint(svc, "get", func(_ context.Context, m *Msg[getReq]) (order, error) { return order{ID: m.Data.ID}, nil },
		EndpointSubject("orders.get"), EndpointMetadata(map[string]string{"auth": "none"}))
	g := svc.Group("orders.v2")
	Endpoint(g, "get", func(_ context.Context, m *Msg[getReq]) (order, error) { return order{ID: "v2-" + m.Data.ID}, nil })
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
	r, err := Request[order](ctx, a, "orders.v2.get", getReq{ID: "7"})
	if err != nil || r.Data.ID != "v2-7" {
		t.Fatal(r, err)
	}
}

func TestServiceErrorsAndStats(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	svc := Service(a, micro.Config{Name: "calc", Version: "0.1.0"})
	Endpoint(svc, "div", func(_ context.Context, m *Msg[[2]int]) (int, error) {
		if m.Data[1] == 0 {
			return 0, errors.New("division by zero")
		}
		return m.Data[0] / m.Data[1], nil
	}, EndpointSubject("calc.div"))
	start(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if r, err := Request[int](ctx, a, "calc.div", [2]int{6, 3}); err != nil || r.Data != 2 {
		t.Fatal(r, err)
	}
	var se *ServiceError
	if _, err := Request[int](ctx, a, "calc.div", [2]int{1, 0}); !errors.As(err, &se) || se.Code != 500 || se.Description != "division by zero" {
		t.Fatal(err)
	}
	if _, err := Request[int](ctx, a, "calc.div", "not-an-array"); !errors.As(err, &se) || se.Code != 400 {
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
	svc := Service(a, micro.Config{Name: "flaky", Version: "1.0.0"})
	Endpoint(svc, "do", func(context.Context, *Msg[int]) (int, error) { calls.Add(1); return 0, errors.New("down") },
		EndpointSubject("flaky.do"), MaxAttempts(2), fast, DLQ("dlq.flaky"))
	start(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = Request[int](ctx, a, "flaky.do", 1)
	waitFor(t, func() bool { return len(dlqMsgs(t, a)) == 1 })
	if calls.Load() != 2 {
		t.Fatal(calls.Load())
	}
}

func TestServiceRegistrationErrors(t *testing.T) {
	cases := map[string]func(a *App){
		"no version":     func(a *App) { Endpoint(Service(a, micro.Config{Name: "x"}), "e", echoInt) },
		"no name":        func(a *App) { Endpoint(Service(a, micro.Config{Version: "1.0.0"}), "e", echoInt) },
		"empty endpoint": func(a *App) { Endpoint(Service(a, micro.Config{Name: "x", Version: "1.0.0"}), "", echoInt) },
		"empty prefix":   func(a *App) { Endpoint(Service(a, micro.Config{Name: "x", Version: "1.0.0"}).Group(""), "e", echoInt) },
		"concurrency": func(a *App) {
			Endpoint(Service(a, micro.Config{Name: "x", Version: "1.0.0"}), "e", echoInt, Concurrency(2))
		},
		"subject on reply": func(a *App) {
			Reply(a, "r", echoInt, EndpointSubject("r"))
		},
		"duplicate service": func(a *App) {
			Service(a, micro.Config{Name: "x", Version: "1.0.0"})
			Service(a, micro.Config{Name: "x", Version: "1.0.0"})
		},
		"dlq on own subject": func(a *App) {
			Endpoint(Service(a, micro.Config{Name: "x", Version: "1.0.0"}), "e", echoInt, EndpointSubject("dlq.e"), DLQ("dlq.e"))
		},
		"dlq on group subject": func(a *App) {
			Endpoint(Service(a, micro.Config{Name: "x", Version: "1.0.0"}).Group("dlq"), "e", echoInt, DLQ("dlq.e"))
		},
		"invalid micro name": func(a *App) { Endpoint(Service(a, micro.Config{Name: "x y", Version: "1.0.0"}), "e", echoInt) },
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
	svc := Service(a, micro.Config{Name: "slow", Version: "1.0.0"})
	Endpoint(svc, "do", func(ctx context.Context, _ *Msg[int]) (int, error) {
		started <- struct{}{}
		<-ctx.Done()
		return 0, ctx.Err()
	}, EndpointSubject("slow.do"))
	start(t, a)
	go func() {
		ctx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_, _ = Request[int](ctx, a, "slow.do", 1)
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
	Endpoint(Service(a, micro.Config{Name: "drain", Version: "1.0.0"}), "do", func(_ context.Context, m *Msg[int]) (int, error) {
		started <- struct{}{}
		time.Sleep(100 * time.Millisecond)
		return m.Data, nil
	}, EndpointSubject("drain.do"))
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
	Endpoint(Service(a, micro.Config{Name: "h", Version: "1.0.0"}), "ping", echoInt)
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
