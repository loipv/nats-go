package nats

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
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

func TestKVStoreProvisionFailureFailsStart(t *testing.T) {
	a := newApp(t, runServer(t))
	NewKVStore(a, "bad bucket")
	err := a.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "nats: start hook") || !strings.Contains(err.Error(), "nats: sync kv bucket") {
		t.Fatalf("Start = %v", err)
	}
	if a.running() || a.Conn() != nil {
		t.Fatal("app must not be running after a failed Start")
	}
}

func TestIdempotentAcrossInstancesKV(t *testing.T) {
	s := runServer(t)
	var calls atomic.Int32
	var apps []*App
	for range 2 {
		a := newApp(t, s)
		setupStream(t, a)
		a.Use(Idempotent(NewKVStore(a, "IDEM"), WithIdempotencyKey(func(m *RawMsg) string { return m.Header.Get("X-Order-Id") })))
		a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error {
			calls.Add(1)
			time.Sleep(50 * time.Millisecond)
			return nil
		}), fast)
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

// TestKVStoreDoesNotGrow checks that a released claim leaves a purge marker
// with a TTL, so the marker expires instead of living in the bucket forever.
func TestKVStoreDoesNotGrow(t *testing.T) {
	ctx := context.Background()
	a := newApp(t, runServer(t))
	st := NewKVStore(a, "IDEM")
	start(t, a)
	stream, err := a.JetStream().Stream(ctx, "KV_IDEM")
	if err != nil {
		t.Fatal(err)
	}
	for name, release := range map[string]func(key string) error{
		"abort":    func(key string) error { return st.Abort(ctx, key) },
		"complete": func(key string) error { return st.Complete(ctx, key, time.Minute) },
	} {
		key := "grow-" + name
		if err := st.Begin(ctx, key, time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := release(key); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte(key))
		m, err := stream.GetLastMsgForSubject(ctx, "$KV.IDEM.c."+hex.EncodeToString(sum[:]))
		if err != nil {
			t.Fatal(err)
		}
		if m.Header.Get("Nats-TTL") == "" {
			t.Fatalf("%s: claim marker has no TTL, it would never be removed: %v", name, m.Header)
		}
	}
}

func TestKVStoreSubSecondTTL(t *testing.T) {
	ctx := context.Background()
	a := newApp(t, runServer(t))
	st := NewKVStore(a, "IDEM")
	start(t, a)
	if err := st.Begin(ctx, "k", 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := st.Complete(ctx, "k", 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := st.Begin(ctx, "k", 500*time.Millisecond); !errors.Is(err, ErrDuplicate) {
		t.Fatal(err)
	}
}
