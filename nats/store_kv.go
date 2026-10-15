package nats

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// KVStore is a JetStream key-value backed IdempotencyStore that is correct
// across instances. A start hook creates or updates the bucket; it needs
// nats-server 2.11 or later for per-key TTLs. Using it before Start returns
// ErrNotRunning.
type KVStore struct {
	app    *App
	bucket string
	kv     atomic.Pointer[jetstream.KeyValue]
}

// NewKVStore registers bucket with app so Start creates it. It does no I/O.
// Call it before Start: a store created later is never provisioned and its
// methods return ErrNotRunning. An empty bucket fails Start. The server only accepts whole-second TTLs of at
// least one second, so every TTL passed to Begin and Complete is rounded up to
// the next whole second (500ms becomes 1s, 1.2s becomes 2s).
func NewKVStore(app *App, bucket string) *KVStore {
	s := &KVStore{app: app, bucket: bucket}
	if bucket == "" {
		app.addErr(errors.New("nats: kv store: empty bucket"))
		return s
	}
	app.Hook(Hook{OnStart: s.provision})
	return s
}

// provision runs as a start hook, so it relies on App.JetStream() being
// non-nil during Start.
func (s *KVStore) provision(ctx context.Context) error {
	kv, err := s.app.JetStream().CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: s.bucket, History: 1, LimitMarkerTTL: time.Minute})
	if err != nil {
		return fmt.Errorf("nats: sync kv bucket %q: %w", s.bucket, err)
	}
	s.kv.Store(&kv)
	return nil
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

// keyTTL rounds ttl up to a whole second, the finest per-key TTL the server accepts.
func keyTTL(ttl time.Duration) jetstream.KVCreateOpt {
	return jetstream.KeyTTL(max(time.Second, (ttl+time.Second-1)/time.Second*time.Second))
}

// release removes a claim. The purge marker carries a TTL so it expires on its
// own; a plain Delete marker would stay in the bucket forever.
func release(ctx context.Context, kv jetstream.KeyValue, claim string) error {
	return kv.Purge(ctx, claim, jetstream.PurgeTTL(time.Minute))
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
	if _, err := kv.Create(ctx, c, nil, keyTTL(ttl)); err != nil {
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
	if derr := release(context.WithoutCancel(ctx), kv, c); derr != nil && err == nil {
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
	if _, err := kv.Create(ctx, d, nil, keyTTL(ttl)); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		return err
	}
	return release(ctx, kv, c)
}

// Abort releases the claim on key so a later Begin succeeds.
func (s *KVStore) Abort(ctx context.Context, key string) error {
	c, _, kv, err := s.keys(key)
	if err != nil {
		return err
	}
	return release(ctx, kv, c)
}
