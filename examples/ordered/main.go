// Command ordered reads a stream in order with an ephemeral ordered
// consumer, filtered to a subset of subjects.
//
//	nats-server -js &
//	go run ./examples/ordered
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/loipv/nats-go/nats"
	"github.com/nats-io/nats.go/jetstream"
)

type Event struct {
	Name string `json:"name"`
}

func main() {
	app, err := nats.New(nats.WithURL(natsURL()))
	if err != nil {
		log.Fatal(err)
	}
	app.Stream(jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"events.>"}})

	var wg sync.WaitGroup
	wg.Add(2)
	// Ordered consumers never ack. DeliverNewPolicy skips history from
	// earlier runs; use DeliverAllPolicy to replay the whole stream.
	app.ConsumeOrdered("EVENTS", nats.Typed(func(_ context.Context, m *nats.Msg[Event]) error {
		defer wg.Done()
		fmt.Printf("%s: %s\n", m.Subject, m.Data.Name)
		return nil
	}), nats.WithFilterSubjects("events.eu.>"), nats.WithDeliverPolicy(jetstream.DeliverNewPolicy))

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := app.Shutdown(ctx); err != nil {
			log.Println("shutdown:", err)
		}
	}()

	for i, subject := range []string{"events.eu.signup", "events.us.signup", "events.eu.login"} {
		if _, err := app.PublishJS(ctx, subject, Event{Name: fmt.Sprintf("event-%d", i+1)}); err != nil {
			log.Fatal(err)
		}
	}
	// Prints event-1 then event-3; event-2 is filtered out.
	wg.Wait()
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://127.0.0.1:4222"
}
