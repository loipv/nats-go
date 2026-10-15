package nats

import (
	"context"
	"sync"
	"time"
)

// sweepInterval is how often MemoryStore drops expired entries.
const sweepInterval = time.Minute

type memEntry struct {
	done    bool
	expires time.Time
}

// MemoryStore is an in-process IdempotencyStore. It is only correct when a
// single instance consumes the messages; use KVStore across instances.
type MemoryStore struct {
	mu        sync.Mutex
	entries   map[string]memEntry
	lastSweep time.Time
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: map[string]memEntry{}, lastSweep: time.Now()}
}

// live returns the unexpired entry for key, sweeping expired entries at most
// once per sweepInterval. The caller must hold s.mu.
func (s *MemoryStore) live(key string, now time.Time) (memEntry, bool) {
	if now.Sub(s.lastSweep) > sweepInterval {
		for k, e := range s.entries {
			if !e.expires.After(now) {
				delete(s.entries, k)
			}
		}
		s.lastSweep = now
	}
	e, ok := s.entries[key]
	if ok && !e.expires.After(now) {
		delete(s.entries, key)
		return memEntry{}, false
	}
	return e, ok
}

// Begin claims key for ttl, or reports ErrDuplicate or ErrInProgress.
func (s *MemoryStore) Begin(_ context.Context, key string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if e, ok := s.live(key, now); ok {
		if e.done {
			return ErrDuplicate
		}
		return ErrInProgress
	}
	s.entries[key] = memEntry{expires: now.Add(ttl)}
	return nil
}

// Complete marks key as done for ttl.
func (s *MemoryStore) Complete(_ context.Context, key string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.live(key, now)
	s.entries[key] = memEntry{done: true, expires: now.Add(ttl)}
	return nil
}

// Abort releases the claim on key so a later Begin succeeds.
func (s *MemoryStore) Abort(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.live(key, time.Now())
	if e, ok := s.entries[key]; ok && !e.done {
		delete(s.entries, key)
	}
	return nil
}
