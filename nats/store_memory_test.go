package nats

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryStoreContract(t *testing.T) {
	testStoreContract(t, func(*testing.T) IdempotencyStore { return NewMemoryStore() })
}

func TestMemoryStoreDoneExpires(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.Begin(ctx, "k", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, "k", 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := s.Begin(ctx, "k", time.Minute); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("Begin on a completed key = %v, want ErrDuplicate", err)
	}
	waitFor(t, func() bool { return s.Begin(ctx, "k", time.Minute) == nil })
}

func TestMemoryStoreAbortKeepsDone(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.Begin(ctx, "k", time.Minute)
	_ = s.Complete(ctx, "k", time.Minute)
	if err := s.Abort(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if err := s.Begin(ctx, "k", time.Minute); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("Begin after Abort on a completed key = %v, want ErrDuplicate", err)
	}
}

// The sweep only shows in the size of the map, so this test reads the
// unexported fields.
func TestMemoryStoreSweepsExpired(t *testing.T) {
	s := NewMemoryStore()
	past := time.Now().Add(-time.Second)
	s.entries["old-claim"] = memEntry{expires: past}
	s.entries["old-done"] = memEntry{done: true, expires: past}
	s.lastSweep = time.Now().Add(-2 * sweepInterval)
	if err := s.Begin(context.Background(), "fresh", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.entries["fresh"]; !ok || len(s.entries) != 1 {
		t.Fatalf("entries after the sweep = %v, want only fresh", s.entries)
	}
}
