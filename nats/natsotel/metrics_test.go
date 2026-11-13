package natsotel_test

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/loipv/nats-go/nats"
	"github.com/loipv/nats-go/nats/natsotel"
)

func collectMetricNames(t *testing.T, reader *sdkmetric.ManualReader) map[string]bool {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names[m.Name] = true
		}
	}
	return names
}

func TestMetricsRecorded(t *testing.T) {
	_, _, _, reader, opts := setup(t)
	pub := natsotel.PublishMiddleware(opts...)(func(context.Context, *nats.RawMsg) error { return nil })
	con := natsotel.Middleware(opts...)(func(context.Context, *nats.RawMsg) error { return nil })
	_ = pub(context.Background(), &nats.RawMsg{Subject: "a"})
	_ = con(context.Background(), &nats.RawMsg{Subject: "a"})
	names := collectMetricNames(t, reader)
	for _, n := range []string{"messaging.process.duration", "messaging.client.consumed.messages", "messaging.client.operation.duration", "messaging.client.sent.messages"} {
		if !names[n] {
			t.Fatal("missing metric", n, names)
		}
	}
}

// failingProvider hands out a meter that cannot create any instrument.
type failingProvider struct{ noop.MeterProvider }

func (failingProvider) Meter(string, ...metric.MeterOption) metric.Meter { return failingMeter{} }

type failingMeter struct{ noop.Meter }

func (failingMeter) Float64Histogram(string, ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	return nil, errors.New("no histogram")
}

func (failingMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return nil, errors.New("no counter")
}

// An instrument that cannot be created must not make the middleware panic.
func TestInstrumentCreationErrorFallsBack(t *testing.T) {
	_, _, _, _, opts := setup(t)
	opts = append(opts, natsotel.WithMeterProvider(failingProvider{}))
	nop := func(context.Context, *nats.RawMsg) error { return nil }
	if err := natsotel.Middleware(opts...)(nop)(context.Background(), &nats.RawMsg{Subject: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := natsotel.PublishMiddleware(opts...)(nop)(context.Background(), &nats.RawMsg{Subject: "a"}); err != nil {
		t.Fatal(err)
	}
}
