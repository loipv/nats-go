package natsotel_test

import (
	"context"
	"slices"
	"testing"

	"go.opentelemetry.io/otel/propagation"

	"github.com/loipv/nats-go/nats"
	"github.com/loipv/nats-go/nats/natsotel"
)

// probePropagator hands the carrier the middleware built to the test.
type probePropagator struct {
	onInject, onExtract func(propagation.TextMapCarrier)
}

func (p probePropagator) Inject(_ context.Context, c propagation.TextMapCarrier) {
	if p.onInject != nil {
		p.onInject(c)
	}
}

func (p probePropagator) Extract(ctx context.Context, c propagation.TextMapCarrier) context.Context {
	if p.onExtract != nil {
		p.onExtract(c)
	}
	return ctx
}

func (probePropagator) Fields() []string { return nil }

func TestCarrierKeys(t *testing.T) {
	_, _, _, _, opts := setup(t)
	var keys []string
	opts = append(opts, natsotel.WithPropagator(probePropagator{onExtract: func(c propagation.TextMapCarrier) { keys = c.Keys() }}))
	m := &nats.RawMsg{Subject: "a", Header: nats.Header{"A": {"1"}, "B": {"2"}}}
	_ = natsotel.Middleware(opts...)(func(context.Context, *nats.RawMsg) error { return nil })(context.Background(), m)
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"A", "B"}) {
		t.Fatalf("carrier keys = %v, want [A B]", keys)
	}
}

func TestCarrierSetGet(t *testing.T) {
	_, _, _, _, opts := setup(t)
	var readBack string
	opts = append(opts, natsotel.WithPropagator(probePropagator{onInject: func(c propagation.TextMapCarrier) {
		c.Set("X-Test", "1")
		readBack = c.Get("X-Test")
	}}))
	m := &nats.RawMsg{Subject: "a"}
	_ = natsotel.PublishMiddleware(opts...)(func(context.Context, *nats.RawMsg) error { return nil })(context.Background(), m)
	if readBack != "1" || m.Header.Get("X-Test") != "1" {
		t.Fatalf("carrier Get = %q, message header = %q, want 1 and 1", readBack, m.Header.Get("X-Test"))
	}
}
