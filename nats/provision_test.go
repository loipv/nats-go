package nats

import (
	"context"
	"strings"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestProvisionCreatesAndUpdates(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	a.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	a.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "billing", AckPolicy: jetstream.AckExplicitPolicy})
	a.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "audit", DeliverSubject: "deliver.audit", AckPolicy: jetstream.AckExplicitPolicy})
	start(t, a)
	if _, err := a.JetStream().Consumer(context.Background(), "ORDERS", "billing"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.JetStream().PushConsumer(context.Background(), "ORDERS", "audit"); err != nil {
		t.Fatal(err)
	}
	b := newApp(t, s)
	b.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}, Description: "v2"})
	start(t, b)
	info, _ := b.JetStream().Stream(context.Background(), "ORDERS")
	if info.CachedInfo().Config.Description != "v2" {
		t.Fatal("stream not updated")
	}
}

func TestProvisionImmutableFieldFailsStart(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	a.Stream(jetstream.StreamConfig{Name: "S", Subjects: []string{"s.>"}, Storage: jetstream.FileStorage})
	start(t, a)
	b := newApp(t, s)
	b.Stream(jetstream.StreamConfig{Name: "S", Subjects: []string{"s.>"}, Storage: jetstream.MemoryStorage})
	err := b.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), `"S"`) {
		t.Fatal(err)
	}
}

func TestStreamRequiresName(t *testing.T) {
	a := newApp(t, runServer(t))
	a.Stream(jetstream.StreamConfig{})
	if err := a.Start(context.Background()); err == nil {
		t.Fatal("want error")
	}
}
