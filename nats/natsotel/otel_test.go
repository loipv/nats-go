package natsotel_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/loipv/nats-go/nats"
	"github.com/loipv/nats-go/nats/internal/testutil"
	"github.com/loipv/nats-go/nats/natsotel"
)

func setup(t *testing.T) (*sdktrace.TracerProvider, *tracetest.SpanRecorder, *sdkmetric.MeterProvider, *sdkmetric.ManualReader, []natsotel.Option) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
	})
	opts := []natsotel.Option{
		natsotel.WithTracerProvider(tp),
		natsotel.WithMeterProvider(mp),
		natsotel.WithPropagator(propagation.TraceContext{}),
	}
	return tp, sr, mp, reader, opts
}

func waitSpans(t *testing.T, sr *tracetest.SpanRecorder, n int) {
	t.Helper()
	testutil.WaitFor(t, func() bool { return len(sr.Ended()) >= n })
}

func spanNamed(t *testing.T, sr *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, s := range sr.Ended() {
		if s.Name() == name {
			return s
		}
	}
	t.Fatalf("no span named %q", name)
	return nil
}

func attrValue(s sdktrace.ReadOnlySpan, key string) (attribute.Value, bool) {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

func attrString(s sdktrace.ReadOnlySpan, key string) string {
	v, _ := attrValue(s, key)
	return v.AsString()
}

func attrBool(s sdktrace.ReadOnlySpan, key string) bool {
	v, _ := attrValue(s, key)
	return v.AsBool()
}

func wantAttr(t *testing.T, s sdktrace.ReadOnlySpan, key, want string) {
	t.Helper()
	if got := attrString(s, key); got != want {
		t.Fatalf("%s: %s = %q, want %q", s.Name(), key, got, want)
	}
}

func wantIntAttr(t *testing.T, s sdktrace.ReadOnlySpan, key string, want int64) {
	t.Helper()
	v, ok := attrValue(s, key)
	if !ok || v.AsInt64() != want {
		t.Fatalf("%s: %s = %v (present %v), want %d", s.Name(), key, v.AsInterface(), ok, want)
	}
}

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

func TestTracePropagatesProducerToConsumer(t *testing.T) {
	_, sr, _, _, opts := setup(t)
	s := testutil.RunServer(t)
	a := testutil.NewApp(t, s.ClientURL())
	a.Use(natsotel.Middleware(opts...))
	a.UsePublish(natsotel.PublishMiddleware(opts...))
	done := make(chan struct{})
	a.Subscribe("orders.created", nats.Typed(func(context.Context, *nats.Msg[int]) error { close(done); return nil }))
	testutil.Start(t, a)
	_ = a.Publish(context.Background(), "orders.created", 1, nats.WithHeader(nats.Header{"Nats-Msg-Id": []string{"m1"}}))
	testutil.Recv(t, done)
	waitSpans(t, sr, 2)
	send, proc := spanNamed(t, sr, "send orders.created"), spanNamed(t, sr, "process orders.created")
	if send.SpanKind() != trace.SpanKindProducer || proc.SpanKind() != trace.SpanKindConsumer {
		t.Fatal("wrong span kinds")
	}
	if proc.Parent().SpanID() != send.SpanContext().SpanID() || proc.SpanContext().TraceID() != send.SpanContext().TraceID() {
		t.Fatal("consumer must be child of producer")
	}
	wantAttr(t, send, "messaging.system", "nats")
	wantAttr(t, send, "messaging.destination.name", "orders.created")
	wantAttr(t, send, "messaging.operation.type", "send")
	wantAttr(t, send, "messaging.message.id", "m1")
	wantAttr(t, proc, "messaging.operation.type", "process")
	wantIntAttr(t, proc, "messaging.nats.attempt", 1)
}

func TestErrorMarksSpanAndDuplicateDoesNot(t *testing.T) {
	_, sr, _, _, opts := setup(t)
	h := natsotel.Middleware(opts...)(func(context.Context, *nats.RawMsg) error { return errors.New("boom") })
	_ = h(context.Background(), &nats.RawMsg{Subject: "s"})
	d := natsotel.Middleware(opts...)(func(context.Context, *nats.RawMsg) error { return nats.Terminal(nats.ErrDuplicate) })
	_ = d(context.Background(), &nats.RawMsg{Subject: "d"})
	if s := spanNamed(t, sr, "process s"); s.Status().Code != codes.Error || attrString(s, "error.type") == "" {
		t.Fatal(s.Status())
	}
	if s := spanNamed(t, sr, "process d"); s.Status().Code == codes.Error || !attrBool(s, "messaging.nats.duplicate") {
		t.Fatal(s.Status())
	}
}

type codeError struct{}

func (codeError) Error() string { return "code" }

func TestErrorTypeSkipsFmtWrappers(t *testing.T) {
	_, sr, _, _, opts := setup(t)
	for i, c := range []struct {
		err  error
		want string
	}{
		{codeError{}, "natsotel_test.codeError"},
		{fmt.Errorf("a: %w", fmt.Errorf("b: %w", codeError{})), "natsotel_test.codeError"},
		{fmt.Errorf("a: %w", errors.New("x")), "*errors.errorString"},
		{fmt.Errorf("%w %w", codeError{}, errors.New("x")), "*fmt.wrapErrors"},
	} {
		subject := fmt.Sprint("e", i)
		h := natsotel.Middleware(opts...)(func(context.Context, *nats.RawMsg) error { return c.err })
		_ = h(context.Background(), &nats.RawMsg{Subject: subject})
		if got := attrString(spanNamed(t, sr, "process "+subject), "error.type"); got != c.want {
			t.Errorf("error.type of %v = %q, want %q", c.err, got, c.want)
		}
	}
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

func TestWithDestination(t *testing.T) {
	_, sr, _, _, opts := setup(t)
	opts = append(opts, natsotel.WithDestination(func(string) string { return "orders.*" }))
	_ = natsotel.Middleware(opts...)(func(context.Context, *nats.RawMsg) error { return nil })(context.Background(), &nats.RawMsg{Subject: "orders.123"})
	if s := spanNamed(t, sr, "process orders.*"); attrString(s, "messaging.destination.name") != "orders.*" {
		t.Fatal("destination not overridden")
	}
}

func TestNoHeaders(t *testing.T) {
	_, sr, _, _, opts := setup(t)
	if err := natsotel.Middleware(opts...)(func(context.Context, *nats.RawMsg) error { return nil })(context.Background(), &nats.RawMsg{Subject: "bare"}); err != nil {
		t.Fatal(err)
	}
	if s := spanNamed(t, sr, "process bare"); s.Parent().IsValid() {
		t.Fatal("must be a root span")
	}
	m := &nats.RawMsg{Subject: "out"}
	_ = natsotel.PublishMiddleware(opts...)(func(context.Context, *nats.RawMsg) error { return nil })(context.Background(), m)
	if m.Header.Get("traceparent") == "" {
		t.Fatal("traceparent must be injected into a nil header")
	}
}
