package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type consumerDecl struct {
	stream string
	cfg    jetstream.ConsumerConfig
}

func (d consumerDecl) name() string {
	if d.cfg.Durable != "" {
		return d.cfg.Durable
	}
	return d.cfg.Name
}

// Stream declares a stream that Start creates or updates. Streams are never
// deleted; a change the server rejects (such as an immutable field) fails Start.
func (a *App) Stream(cfg jetstream.StreamConfig) {
	if cfg.Name == "" {
		a.addErr(errors.New("nats: stream: empty name"))
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lateLocked() {
		return
	}
	a.streams = append(a.streams, cfg)
}

// Consumer declares a durable consumer that Start creates or updates after
// the streams. A config with DeliverSubject creates a push consumer.
func (a *App) Consumer(stream string, cfg jetstream.ConsumerConfig) {
	d := consumerDecl{stream: stream, cfg: cfg}
	if stream == "" || d.name() == "" {
		a.addErr(errors.New("nats: consumer: stream and Durable (or Name) are required"))
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lateLocked() {
		return
	}
	a.consumers = append(a.consumers, d)
}

func (a *App) provision(ctx context.Context) error {
	for _, cfg := range a.streams {
		if _, err := a.js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return fmt.Errorf("nats: sync stream %q: %w", cfg.Name, err)
		}
	}
	for _, d := range a.consumers {
		if err := a.syncConsumer(ctx, d); err != nil {
			return err
		}
	}
	for _, s := range a.kvStores {
		kv, err := a.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: s.bucket, History: 1, LimitMarkerTTL: time.Minute})
		if err != nil {
			return fmt.Errorf("nats: sync kv bucket %q: %w", s.bucket, err)
		}
		s.kv.Store(&kv)
	}
	return nil
}

func (a *App) syncConsumer(ctx context.Context, d consumerDecl) error {
	var err error
	if d.cfg.DeliverSubject != "" {
		_, err = a.js.CreateOrUpdatePushConsumer(ctx, d.stream, d.cfg)
	} else {
		_, err = a.js.CreateOrUpdateConsumer(ctx, d.stream, d.cfg)
	}
	if err != nil {
		return fmt.Errorf("nats: sync consumer %q on stream %q: %w", d.name(), d.stream, err)
	}
	return nil
}
