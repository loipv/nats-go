package nats

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestKVStoreContract(t *testing.T) {
	testStoreContract(t, func(t *testing.T) IdempotencyStore {
		a := newApp(t, runServer(t))
		st := NewKVStore(a, "IDEM")
		start(t, a)
		return st
	})
}

func TestKVStoreBeforeStart(t *testing.T) {
	a, _ := New()
	if err := NewKVStore(a, "IDEM").Begin(context.Background(), "k", time.Minute); !errors.Is(err, ErrNotRunning) {
		t.Fatal(err)
	}
}

func TestKVStoreEmptyBucketFailsStart(t *testing.T) {
	a := newApp(t, runServer(t))
	NewKVStore(a, "")
	if err := a.Start(context.Background()); err == nil {
		t.Fatal("want error")
	}
}

func TestIdempotentAcrossInstancesKV(t *testing.T) {
	s := runServer(t)
	var calls atomic.Int32
	var apps []*App
	for range 2 {
		a := newApp(t, s)
		setupStream(t, a)
		a.Use(Idempotent(NewKVStore(a, "IDEM"), IdempotencyKey(func(m *RawMsg) string { return m.Header.Get("X-Order-Id") })))
		Consume(a, "ORDERS", "billing", func(context.Context, *Msg[order]) error {
			calls.Add(1)
			time.Sleep(50 * time.Millisecond)
			return nil
		}, fast)
		start(t, a)
		apps = append(apps, a)
	}
	for i := range 20 {
		h := Header{"X-Order-Id": []string{fmt.Sprintf("k-%d", i%10)}}
		_, _ = apps[0].PublishJS(context.Background(), "orders.x", order{ID: "1"}, WithHeader(h)) // 10 keys, each twice
	}
	waitFor(t, func() bool { return pendingAcks(t, apps[0], "billing") == 0 && delivered(t, apps[0], "billing") >= 20 })
	if calls.Load() != 10 {
		t.Fatal("each key must be processed exactly once", calls.Load())
	}
}
