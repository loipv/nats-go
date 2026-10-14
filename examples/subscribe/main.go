// Command subscribe shows core NATS subscriptions: wildcards, queue groups
// and concurrent handlers.
//
//	nats-server -js &
//	go run ./examples/subscribe
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/loipv/nats-go/nats"
)

type Order struct {
	ID string `json:"id"`
}

func main() {
	app, err := nats.New(nats.WithURL(natsURL()))
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	// "orders.*" matches exactly one token after "orders.". Every instance
	// started with the same Queue group shares the load: each message goes
	// to one of them. Concurrency(4) runs up to 4 handler calls in parallel.
	nats.Subscribe(app, "orders.*", func(_ context.Context, m *nats.Msg[Order]) error {
		defer wg.Done()
		fmt.Printf("received %s: order %s\n", m.Subject, m.Data.ID)
		return nil
	}, nats.Queue("workers"), nats.Concurrency(4))

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer app.Shutdown(ctx)

	for _, subject := range []string{"orders.created", "orders.shipped", "orders.eu.created"} {
		if err := app.Publish(ctx, subject, Order{ID: "42"}); err != nil {
			log.Fatal(err)
		}
	}
	// "orders.eu.created" has two tokens after "orders." and is not delivered.
	wg.Wait()
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://127.0.0.1:4222"
}
