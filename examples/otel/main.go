// Command otel shows tracing across a publish and a handler. The trace
// context travels in the message headers, so the producer span ("send") and
// the consumer span ("process") print with the same trace ID, the second as a
// child of the first. Spans go to stdout.
//
//	nats-server -js &
//	go run ./examples/otel
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/loipv/nats-go/nats"
	"github.com/loipv/nats-go/nats/natsotel"
)

type Order struct {
	ID string `json:"id"`
}

func main() {
	ctx := context.Background()

	exp, err := stdouttrace.New()
	if err != nil {
		log.Fatal(err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() {
		if err := tp.Shutdown(ctx); err != nil {
			log.Println("shutdown:", err)
		}
	}()
	// natsotel uses the global provider and propagator by default.
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	app, err := nats.New(nats.WithURL(natsURL()))
	if err != nil {
		log.Fatal(err)
	}
	app.Use(natsotel.Middleware())
	app.UsePublish(natsotel.PublishMiddleware())

	handled := make(chan struct{})
	app.Subscribe("otel.orders.created", nats.Typed(func(_ context.Context, m *nats.Msg[Order]) error {
		fmt.Println("handled order", m.Data.ID)
		close(handled)
		return nil
	}))

	if err := app.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := app.Shutdown(ctx); err != nil {
			log.Println("shutdown:", err)
		}
	}()

	if err := app.Publish(ctx, "otel.orders.created", Order{ID: "42"}); err != nil {
		log.Fatal(err)
	}
	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		log.Fatal("timed out waiting for the handler")
	}
	// The process span ends after the handler returns; give it a moment.
	time.Sleep(200 * time.Millisecond)
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://127.0.0.1:4222"
}
