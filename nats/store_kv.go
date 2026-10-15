package nats

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// KVStore is a JetStream key-value backed IdempotencyStore that is correct
// across instances. Start creates or updates the bucket; it needs nats-server
// 2.11 or later for per-key TTLs. Using it before Start returns ErrNotRunning.
type KVStore struct {
	bucket string
	kv     atomic.Pointer[jetstream.KeyValue]
}

// NewKVStore registers bucket with app so Start creates it. It does no I/O.
// An empty bucket fails Start.
func NewKVStore(app *App, bucket string) *KVStore {
	s := &KVStore{bucket: bucket}
	if bucket == "" {
		app.addErr(errors.New("nats: kv store: empty bucket"))
		return s
	}
	app.mu.Lock()
	defer app.mu.Unlock()
	if app.lateLocked() {
		return s
	}
	app.kvStores = append(app.kvStores, s)
	return s
}

func (s *KVStore) keys(key string) (claim, done string, kv jetstream.KeyValue, err error) {
	p := s.kv.Load()
	if p == nil {
		return "", "", nil, ErrNotRunning
	}
	sum := sha256.Sum256([]byte(key))
	h := hex.EncodeToString(sum[:])
	return "c." + h, "d." + h, *p, nil
}

// exists reports whether key has a live entry.
func exists(ctx context.Context, kv jetstream.KeyValue, key string) (bool, error) {
	_, err := kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

// Begin claims key for ttl, or reports ErrDuplicate or ErrInProgress.
func (s *KVStore) Begin(ctx context.Context, key string, ttl time.Duration) error {
	c, d, kv, err := s.keys(key)
	if err != nil {
		return err
	}
	if ok, err := exists(ctx, kv, d); err != nil {
		return err
	} else if ok {
		return ErrDuplicate
	}
	if _, err := kv.Create(ctx, c, nil, jetstream.KeyTTL(ttl)); err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return ErrInProgress
		}
		return err
	}
	// Another instance may have completed between the check and the claim.
	ok, err := exists(ctx, kv, d)
	if err == nil && !ok {
		return nil
	}
	if derr := kv.Delete(context.WithoutCancel(ctx), c); derr != nil && err == nil {
		err = derr
	}
	if err == nil {
		err = ErrDuplicate
	}
	return err
}

// Complete marks key as done for ttl and releases its claim.
func (s *KVStore) Complete(ctx context.Context, key string, ttl time.Duration) error {
	c, d, kv, err := s.keys(key)
	if err != nil {
		return err
	}
	if _, err := kv.Create(ctx, d, nil, jetstream.KeyTTL(ttl)); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		return err
	}
	return kv.Delete(ctx, c)
}

// Abort releases the claim on key so a later Begin succeeds.
func (s *KVStore) Abort(ctx context.Context, key string) error {
	c, _, kv, err := s.keys(key)
	if err != nil {
		return err
	}
	return kv.Delete(ctx, c)
}
