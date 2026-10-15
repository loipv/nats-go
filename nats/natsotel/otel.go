package natsotel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/loipv/nats-go/nats"
)

const scope = "github.com/loipv/nats-go/nats/natsotel"

// Attribute keys, following the OpenTelemetry messaging semantic conventions.
const (
	keySystem        = "messaging.system"
	keyDestination   = "messaging.destination.name"
	keyOperationType = "messaging.operation.type"
	keyOperationName = "messaging.operation.name"
	keyBodySize      = "messaging.message.body.size"
	keyMessageID     = "messaging.message.id"
	keyAttempt       = "messaging.nats.attempt"
	keyDuplicate     = "messaging.nats.duplicate"
	keyErrorType     = "error.type"
)

// Option configures Middleware and PublishMiddleware.
type Option func(*options)

type options struct {
	tp          trace.TracerProvider
	mp          metric.MeterProvider
	propagator  propagation.TextMapPropagator
	destination func(string) string
}

// WithTracerProvider sets the tracer provider (default otel.GetTracerProvider).
func WithTracerProvider(tp trace.TracerProvider) Option { return func(o *options) { o.tp = tp } }

// WithMeterProvider sets the meter provider (default otel.GetMeterProvider).
func WithMeterProvider(mp metric.MeterProvider) Option { return func(o *options) { o.mp = mp } }

// WithPropagator sets the propagator (default otel.GetTextMapPropagator).
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.propagator = p }
}

// WithDestination maps a message subject to the value used for the
// messaging.destination.name attribute and the span name. Use it to keep
// cardinality low when subjects carry identifiers.
func WithDestination(fn func(subject string) string) Option {
	return func(o *options) { o.destination = fn }
}

type config struct {
	tracer      trace.Tracer
	propagator  propagation.TextMapPropagator
	destination func(string) string
	inst        *instruments
}

func newConfig(opts []Option) *config {
	o := options{
		tp:          otel.GetTracerProvider(),
		mp:          otel.GetMeterProvider(),
		propagator:  otel.GetTextMapPropagator(),
		destination: func(s string) string { return s },
	}
	for _, opt := range opts {
		opt(&o)
	}
	return &config{
		tracer:      o.tp.Tracer(scope),
		propagator:  o.propagator,
		destination: o.destination,
		inst:        newInstruments(o.mp.Meter(scope)),
	}
}

// errorType returns the Go type name of err, or "" for nil.
func errorType(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%T", err)
}

// PublishMiddleware traces and measures each outgoing message and injects the
// trace context into its headers.
func PublishMiddleware(opts ...Option) nats.PublishMiddleware {
	c := newConfig(opts)
	return func(next nats.PublishFunc) nats.PublishFunc {
		return func(ctx context.Context, m *nats.RawMsg) error {
			dest := c.destination(m.Subject)
			ctx, span := c.tracer.Start(ctx, "send "+dest,
				trace.WithSpanKind(trace.SpanKindProducer),
				trace.WithAttributes(c.attrs(m, dest, "send")...))
			if m.Header == nil {
				m.Header = nats.Header{}
			}
			c.propagator.Inject(ctx, headerCarrier(m.Header))

			begin := time.Now()
			err := next(ctx, m)
			c.finish(ctx, span, err, dest, "send", time.Since(begin), c.inst.operationDuration, c.inst.sent)
			return err
		}
	}
}

// Middleware traces and measures each received message; its span is a child of
// the producer span carried in the message headers.
func Middleware(opts ...Option) nats.Middleware {
	c := newConfig(opts)
	return func(next nats.Handler) nats.Handler {
		return func(ctx context.Context, m *nats.RawMsg) error {
			dest := c.destination(m.Subject)
			if m.Header != nil {
				ctx = c.propagator.Extract(ctx, headerCarrier(m.Header))
			}
			attrs := append(c.attrs(m, dest, "process"), attribute.Int(keyAttempt, nats.Attempt(ctx)))
			ctx, span := c.tracer.Start(ctx, "process "+dest,
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(attrs...))

			begin := time.Now()
			err := next(ctx, m)
			observed := err
			if errors.Is(err, nats.ErrDuplicate) {
				span.SetAttributes(attribute.Bool(keyDuplicate, true))
				observed = nil // a skipped duplicate is not a failure
			}
			c.finish(ctx, span, observed, dest, "process", time.Since(begin), c.inst.processDuration, c.inst.consumed)
			return err
		}
	}
}

func (c *config) attrs(m *nats.RawMsg, dest, op string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String(keySystem, "nats"),
		attribute.String(keyDestination, dest),
		attribute.String(keyOperationType, op),
		attribute.Int(keyBodySize, len(m.Data)),
	}
	if id := m.Header.Get(jetstream.MsgIDHeader); id != "" {
		attrs = append(attrs, attribute.String(keyMessageID, id))
	}
	return attrs
}

// finish ends span, records err on it, and records the duration and count
// metrics for operation op.
func (c *config) finish(ctx context.Context, span trace.Span, err error, dest, op string,
	d time.Duration, dur metric.Float64Histogram, count metric.Int64Counter) {
	attrs := []attribute.KeyValue{
		attribute.String(keySystem, "nats"),
		attribute.String(keyDestination, dest),
		attribute.String(keyOperationName, op),
	}
	if err != nil {
		et := errorType(err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.String(keyErrorType, et))
		attrs = append(attrs, attribute.String(keyErrorType, et))
	}
	span.End()
	o := metric.WithAttributes(attrs...)
	dur.Record(ctx, d.Seconds(), o)
	count.Add(ctx, 1, o)
}
