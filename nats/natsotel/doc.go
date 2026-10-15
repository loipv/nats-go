// Package natsotel adds OpenTelemetry tracing and metrics to a nats.App.
//
// Register Middleware with App.Use and PublishMiddleware with
// App.UsePublish:
//
//	app.Use(natsotel.Middleware())
//	app.UsePublish(natsotel.PublishMiddleware())
//
// By default the global tracer provider, meter provider and propagator are
// used; set them with otel.SetTracerProvider, otel.SetMeterProvider and
// otel.SetTextMapPropagator (the propagator is a no-op until set, for example
// to propagation.TraceContext{}), or pass WithTracerProvider,
// WithMeterProvider and WithPropagator.
//
// The trace context travels in the message headers, so the "process" span of
// a handler is a child of the "send" span of the publisher. Every attempt of
// a message gets its own span: retries inside the process and JetStream
// redeliveries alike, with the attempt in messaging.nats.attempt. A message
// skipped by Idempotent is marked messaging.nats.duplicate and is not an
// error.
//
// Not covered: ConsumeBatch handlers (they have no middleware) and responses
// sent by Reply workers or micro endpoints (they bypass PublishMiddleware).
//
// Cardinality: the destination name, by default the message subject, is an
// attribute of every span and metric. When subjects carry identifiers such as
// "orders.42.created", metrics get one series per identifier. Set
// WithDestination to map the subject to a low-cardinality value, for example
// the subscription pattern "orders.*.created".
package natsotel
