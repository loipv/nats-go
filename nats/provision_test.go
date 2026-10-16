package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

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

func TestDrift(t *testing.T) {
	got := jetstream.StreamConfig{
		Name: "S", Subjects: []string{"a", "b"}, MaxAge: time.Hour, Replicas: 1,
		Placement: &jetstream.Placement{Cluster: "c1", Tags: []string{"x"}},
		Metadata:  map[string]string{"owner": "billing", "_nats.ver": "2.11"},
	}
	for _, tc := range []struct {
		name string
		want jetstream.StreamConfig
		diff string
	}{
		{"zero fields skipped", jetstream.StreamConfig{Name: "S"}, ""},
		{"same", jetstream.StreamConfig{Name: "S", Subjects: []string{"a", "b"}, MaxAge: time.Hour}, ""},
		{"scalar", jetstream.StreamConfig{MaxAge: 2 * time.Hour}, "MaxAge (declared 2h0m0s, server 1h0m0s)"},
		{"slice length", jetstream.StreamConfig{Subjects: []string{"a"}}, "Subjects (declared [a], server [a b])"},
		{"slice element", jetstream.StreamConfig{Subjects: []string{"a", "c"}}, "Subjects[1] (declared c, server b)"},
		{"pointer partial", jetstream.StreamConfig{Placement: &jetstream.Placement{Cluster: "c1"}}, ""},
		{"pointer field", jetstream.StreamConfig{Placement: &jetstream.Placement{Cluster: "c2"}}, "Placement.Cluster (declared c2, server c1)"},
		{"pointer server unset", jetstream.StreamConfig{RePublish: &jetstream.RePublish{Destination: "x"}}, "RePublish (declared {Source: Destination:x HeadersOnly:false}, server unset)"},
		{"map subset", jetstream.StreamConfig{Metadata: map[string]string{"owner": "billing"}}, ""},
		{"map value and key", jetstream.StreamConfig{Metadata: map[string]string{"owner": "x", "team": "y"}}, "Metadata[owner] (declared x, server billing), Metadata[team] (declared y, server unset)"},
	} {
		if diff := strings.Join(drift(tc.want, got), ", "); diff != tc.diff {
			t.Errorf("%s: drift = %q, want %q", tc.name, diff, tc.diff)
		}
	}
}

func TestProvisionVerify(t *testing.T) {
	s := runServer(t)
	owner := newApp(t, s)
	owner.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}, MaxAge: time.Hour})
	owner.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "billing", AckWait: 10 * time.Second})
	owner.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "audit", DeliverSubject: "deliver.audit"})
	start(t, owner)

	// A partial declaration matches although the server filled defaults.
	ok := newApp(t, s, WithProvision(Verify))
	ok.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	ok.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "billing", AckWait: 10 * time.Second})
	ok.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "audit", DeliverSubject: "deliver.audit"})
	start(t, ok)

	for _, tc := range []struct {
		name    string
		declare func(a *App)
		is      error
		msg     string
	}{
		{"stream drift", func(a *App) {
			a.Stream(jetstream.StreamConfig{Name: "ORDERS", MaxAge: 24 * time.Hour})
		}, ErrConfigDrift, `verify stream "ORDERS": nats: config drift: MaxAge (declared 24h0m0s, server 1h0m0s)`},
		{"stream missing", func(a *App) {
			a.Stream(jetstream.StreamConfig{Name: "NOPE"})
		}, jetstream.ErrStreamNotFound, `verify stream "NOPE"`},
		{"consumer drift", func(a *App) {
			a.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "billing", AckWait: time.Minute})
		}, ErrConfigDrift, "AckWait (declared 1m0s, server 10s)"},
		{"consumer missing", func(a *App) {
			a.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "nope"})
		}, jetstream.ErrConsumerNotFound, `verify consumer "nope"`},
	} {
		a := newApp(t, s, WithProvision(Verify))
		tc.declare(a)
		err := a.Start(context.Background())
		if !errors.Is(err, tc.is) || !strings.Contains(fmt.Sprint(err), tc.msg) {
			t.Errorf("%s: Start = %v, want %v containing %q", tc.name, err, tc.is, tc.msg)
		}
	}

	// Verify never writes.
	st, err := ok.JetStream().Stream(context.Background(), "ORDERS")
	if err != nil || st.CachedInfo().Config.MaxAge != time.Hour {
		t.Fatalf("stream changed by Verify: %v", err)
	}
	if _, err := ok.JetStream().Stream(context.Background(), "NOPE"); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("Verify created a stream: %v", err)
	}
}

func TestProvisionCreateOnly(t *testing.T) {
	s := runServer(t)
	owner := newApp(t, s)
	owner.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}, Description: "v1"})
	owner.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "billing", AckWait: 10 * time.Second})
	start(t, owner)

	var buf syncBuf
	a := newApp(t, s, WithProvision(CreateOnly), WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
	a.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}, Description: "v2"})
	a.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "billing", AckWait: time.Minute})
	a.Stream(jetstream.StreamConfig{Name: "NEW", Subjects: []string{"new.>"}})
	a.Consumer("NEW", jetstream.ConsumerConfig{Durable: "c", DeliverSubject: "deliver.c"})
	start(t, a)

	js, ctx := a.JetStream(), context.Background()
	st, _ := js.Stream(ctx, "ORDERS")
	if d := st.CachedInfo().Config.Description; d != "v1" {
		t.Fatalf("existing stream updated: Description = %q", d)
	}
	c, _ := js.Consumer(ctx, "ORDERS", "billing")
	if w := c.CachedInfo().Config.AckWait; w != 10*time.Second {
		t.Fatalf("existing consumer updated: AckWait = %v", w)
	}
	if _, err := js.PushConsumer(ctx, "NEW", "c"); err != nil {
		t.Fatalf("missing push consumer not created: %v", err)
	}
	logs := buf.String()
	for _, want := range []string{
		`stream=ORDERS fields="Description (declared v2, server v1)"`,
		`consumer=ORDERS/billing fields="AckWait (declared 1m0s, server 10s)"`,
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs missing %q:\n%s", want, logs)
		}
	}
	if strings.Count(logs, "config drift") != 2 {
		t.Errorf("want 2 drift warnings:\n%s", logs)
	}
}

func TestProvisionModePrecedence(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s, WithProvision(Verify))
	a.Stream(jetstream.StreamConfig{Name: "OWNED", Subjects: []string{"owned.>"}}, Upsert)
	start(t, a)
	if _, err := a.JetStream().Stream(context.Background(), "OWNED"); err != nil {
		t.Fatalf("per-declaration Upsert did not override WithProvision(Verify): %v", err)
	}
}

func TestProvisionModeInvalid(t *testing.T) {
	s := runServer(t)
	if _, err := New(WithProvision(0)); err == nil {
		t.Fatal("WithProvision(0): want error")
	}
	for name, declare := range map[string]func(a *App){
		"two modes":        func(a *App) { a.Stream(jetstream.StreamConfig{Name: "S"}, Verify, Upsert) },
		"unknown mode":     func(a *App) { a.Stream(jetstream.StreamConfig{Name: "S"}, ProvisionMode(9)) },
		"consumer unknown": func(a *App) { a.Consumer("S", jetstream.ConsumerConfig{Durable: "c"}, ProvisionMode(-1)) },
	} {
		a := newApp(t, s)
		declare(a)
		if err := a.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "ProvisionMode") {
			t.Errorf("%s: Start = %v, want ProvisionMode error", name, err)
		}
	}
}
