// Command cloudevents publishes a CloudEvent and handles it. The attributes
// travel in ce-* headers (binary content mode) and the payload is the body.
//
//	nats-server -js &
//	go run ./examples/cloudevents
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/loipv/nats-go/nats"
	"github.com/loipv/nats-go/nats/cloudevents"
)

type OrderCreated struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

func main() {
	app, err := nats.New(nats.WithURL(natsURL()))
	if err != nil {
		log.Fatal(err)
	}
	// Events are published and handled on "events.<type>" by default.
	ce := cloudevents.New(app, "urn:example:shop")

	got := make(chan struct{})
	ce.Handle("order.created", cloudevents.Typed(func(_ context.Context, ev cloudevents.Event, m *nats.Msg[OrderCreated]) error {
		fmt.Printf("event %s of type %s from %s\n", ev.ID, ev.Type, ev.Source)
		fmt.Printf("  content type %s, region extension %q\n", ev.DataContentType, ev.Extensions["region"])
		fmt.Printf("  order %s total %d\n", m.Data.ID, m.Data.Total)
		close(got)
		return nil
	}))

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := app.Shutdown(ctx); err != nil {
			log.Println("shutdown:", err)
		}
	}()

	err = ce.Publish(ctx, "order.created", OrderCreated{ID: "42", Total: 1999},
		cloudevents.WithExtension("region", "eu"),
		cloudevents.WithSubject("orders/42"))
	if err != nil {
		log.Fatal(err)
	}
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		log.Fatal("timed out waiting for the event")
	}
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://127.0.0.1:4222"
}
