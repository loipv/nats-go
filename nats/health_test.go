package nats

import (
	"context"
	"encoding/json"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	natsio "github.com/nats-io/nats.go"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestHealthLifecycle(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	a.Subscribe("x", Typed(func(context.Context, *Msg[[]byte]) error { return nil }))
	if a.Health().Status != "down" {
		t.Fatal("not started → down")
	}
	start(t, a)
	rec := httptest.NewRecorder()
	a.HealthHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	var r Report
	_ = json.Unmarshal(rec.Body.Bytes(), &r)
	if rec.Code != 200 || r.Status != "ok" || len(r.Handlers) != 1 || r.Handlers[0].State != "running" {
		t.Fatal(rec.Code, r)
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatal(rec.Header())
	}
}

// M2 ruling: a deleted consumer makes the handler restarting, not stopped.
func TestHealthDegradedWhenHandlerStopped(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s, WithRestartBackoff(func(int) time.Duration { return time.Hour }))
	setupStream(t, a)
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error { return nil }))
	start(t, a)
	_ = a.JetStream().DeleteConsumer(context.Background(), "ORDERS", "billing")
	waitFor(t, func() bool { h := a.Health(); return h.Status == "degraded" && h.Handlers[0].State == "restarting" })
	rec := httptest.NewRecorder()
	a.HealthHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != 503 {
		t.Fatal(rec.Code)
	}
}

func TestHealthDegradedOnDisconnect(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = freePort(t)
	s := natstest.RunServer(&opts)
	a := newApp(t, s, WithNATSOptions(natsio.ReconnectWait(50*time.Millisecond), natsio.MaxReconnects(-1)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	waitFor(t, a.running)
	s.Shutdown()
	waitFor(t, func() bool { return a.Health().Status == "degraded" })
	s2 := natstest.RunServer(&opts)
	defer s2.Shutdown()
	waitFor(t, func() bool { return a.Health().Status == "ok" })
	select {
	case err := <-done:
		t.Fatal("Run exited", err)
	default:
	}
}
