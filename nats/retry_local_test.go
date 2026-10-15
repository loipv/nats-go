package nats

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSubscribeRetriesInProcess(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	attempts := make(chan int, 3)
	Subscribe(a, "r", func(_ context.Context, m *Msg[[]byte]) error {
		attempts <- m.Attempt()
		if m.Attempt() < 3 {
			return errors.New("not yet")
		}
		return nil
	}, MaxAttempts(3), fast)
	start(t, a)
	_ = a.Conn().Publish("r", nil)
	if recv(t, attempts) != 1 || recv(t, attempts) != 2 || recv(t, attempts) != 3 {
		t.Fatal()
	}
	expectNone(t, attempts, 200*time.Millisecond)
}

func TestSubscribeDefaultNoRetry(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	var calls atomic.Int32
	Subscribe(a, "r", func(context.Context, *Msg[[]byte]) error { calls.Add(1); return errors.New("x") }, fast)
	start(t, a)
	_ = a.Conn().Publish("r", nil)
	time.Sleep(300 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
}

func TestSubscribeExhaustedGoesToDLQ(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupDLQ(t, a)
	Subscribe(a, "core.x", func(context.Context, *Msg[[]byte]) error { return errors.New("nope") }, MaxAttempts(2), fast, DLQ("dlq.core"))
	start(t, a)
	_ = a.Publish(context.Background(), "core.x", []byte("payload"), WithHeader(Header{"Nats-Msg-Id": []string{"c-1"}}))
	waitFor(t, func() bool { return len(dlqMsgs(t, a)) == 1 })
	d := dlqMsgs(t, a)[0]
	if string(d.Data) != "payload" || d.Header.Get("Nats-DLQ-Deliveries") != "2" || d.Header.Get("Nats-DLQ-Subject") != "core.x" ||
		d.Header.Get("Nats-DLQ-Msg-Id") != "c-1" || d.Header.Get("Nats-DLQ-Stream") != "" {
		t.Fatal(d.Header)
	}
}

func TestReplyRetriesThenResponds(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupDLQ(t, a)
	Reply(a, "flaky", func(_ context.Context, m *Msg[int]) (int, error) {
		if m.Attempt() < 2 {
			return 0, errors.New("flaky")
		}
		return m.Data * 10, nil
	}, MaxAttempts(2), fast)
	Reply(a, "broken", func(context.Context, *Msg[int]) (int, error) { return 0, errors.New("broken") }, MaxAttempts(2), fast, DLQ("dlq.reply"))
	start(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := Request[int](ctx, a, "flaky", 4)
	if err != nil || r.Data != 40 {
		t.Fatal(r, err)
	}
	_, err = Request[int](ctx, a, "broken", 1)
	var se *ServiceError
	if !errors.As(err, &se) || se.Code != 500 {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(dlqMsgs(t, a)) == 1 })
}

func TestOrderedRetryKeepsOrder(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	var seen []string
	var mu sync.Mutex
	var failed atomic.Bool
	ConsumeOrdered(a, "ORDERS", func(_ context.Context, m *Msg[order]) error {
		if m.Data.ID == "a" && failed.CompareAndSwap(false, true) {
			return errors.New("once")
		}
		mu.Lock()
		seen = append(seen, m.Data.ID)
		mu.Unlock()
		return nil
	}, MaxAttempts(2), fast)
	start(t, a)
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "a"})
	_, _ = a.PublishJS(context.Background(), "orders.x", order{ID: "b"})
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == 2 })
	if seen[0] != "a" || seen[1] != "b" {
		t.Fatal(seen)
	}
}

func TestShutdownAbandonsRetryWait(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	called := make(chan struct{}, 1)
	Subscribe(a, "slow", func(context.Context, *Msg[[]byte]) error { called <- struct{}{}; return errors.New("x") },
		MaxAttempts(3), WithBackoff(func(int) time.Duration { return time.Hour }))
	start(t, a)
	_ = a.Conn().Publish("slow", nil)
	<-called
	begin := time.Now()
	if err := a.Shutdown(context.Background()); err != nil || time.Since(begin) > time.Second {
		t.Fatal(err, time.Since(begin))
	}
}

func TestReplyEncodeFailureNotRetried(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupDLQ(t, a)
	var calls atomic.Int32
	type bad struct{ F func() }
	Reply(a, "unencodable", func(context.Context, *Msg[int]) (bad, error) {
		calls.Add(1)
		return bad{F: func() {}}, nil
	}, MaxAttempts(3), fast, DLQ("dlq.reply"))
	start(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := Request[int](ctx, a, "unencodable", 1)
	var se *ServiceError
	if !errors.As(err, &se) || se.Code != 500 {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if calls.Load() != 1 || len(dlqMsgs(t, a)) != 0 {
		t.Fatal(calls.Load(), len(dlqMsgs(t, a)))
	}
}
