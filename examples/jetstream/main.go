// Command jetstream declares a stream and two durable consumers in code,
// publishes with deduplication and consumes with automatic and manual ack.
//
//	nats-server -js &
//	go run ./examples/jetstream
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/loipv/nats-go/nats"
	"github.com/nats-io/nats.go/jetstream"
)

type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

func main() {
	app, err := nats.New(nats.WithURL(natsURL()))
	if err != nil {
		log.Fatal(err)
	}

	// Created or updated on Start, never deleted.
	app.Stream(jetstream.StreamConfig{
		Name:       "ORDERS",
		Subjects:   []string{"orders.>"},
		Duplicates: 2 * time.Minute, // deduplication window for MsgID
	})
	app.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "billing", AckPolicy: jetstream.AckExplicitPolicy})
	app.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "audit", AckPolicy: jetstream.AckExplicitPolicy})

	var wg sync.WaitGroup
	wg.Add(2)

	// Automatic ack: nil acks, nats.Terminal(err) terms, other errors nak.
	app.Consume("ORDERS", "billing", nats.Typed(func(_ context.Context, m *nats.Msg[Order]) error {
		defer wg.Done()
		fmt.Printf("billing: charged %d for order %s\n", m.Data.Amount, m.Data.ID)
		return nil
	}))

	// Manual ack: the handler decides.
	app.Consume("ORDERS", "audit", nats.Typed(func(_ context.Context, m *nats.Msg[Order]) error {
		defer wg.Done()
		fmt.Printf("audit: recorded order %s\n", m.Data.ID)
		return m.Ack()
	}), nats.ManualAck())

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer app.Shutdown(ctx)

	// Publishing twice with the same MsgID stores the message once.
	order := Order{ID: fmt.Sprintf("o-%d", time.Now().UnixNano()), Amount: 100}
	for range 2 {
		ack, err := app.PublishJS(ctx, "orders.created", order, nats.MsgID(order.ID))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("published seq=%d duplicate=%v\n", ack.Sequence, ack.Duplicate)
	}
	wg.Wait()
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://127.0.0.1:4222"
}
