// Command idempotency processes an order once even though it is published
// three times. Idempotent claims a key in a JetStream key-value bucket before
// the handler runs and remembers it afterwards, so duplicates are acked
// without running the handler. KVStore needs nats-server 2.11 or later.
//
// The messages carry the key in an X-Order-Id header and no Nats-Msg-Id,
// because the stream would drop those duplicates before the handler saw them.
//
//	nats-server -js &
//	go run ./examples/idempotency
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/loipv/nats-go/nats"
	"github.com/nats-io/nats.go/jetstream"
)

type Order struct {
	ID string `json:"id"`
}

func main() {
	app, err := nats.New(nats.WithURL(natsURL()))
	if err != nil {
		log.Fatal(err)
	}

	app.Stream(jetstream.StreamConfig{Name: "INVOICES", Subjects: []string{"invoices.>"}})
	app.Consumer("INVOICES", jetstream.ConsumerConfig{Durable: "mailer", AckPolicy: jetstream.AckExplicitPolicy})

	// The bucket is created or updated on Start. Use NewMemoryStore instead
	// when a single instance consumes the messages.
	store := nats.NewKVStore(app, "IDEMPOTENCY")
	app.Use(
		nats.Recover(),
		nats.Idempotent(store,
			nats.IdempotencyKey(func(m *nats.RawMsg) string { return m.Header.Get("X-Order-Id") }),
			// Must exceed the longest the handler can run.
			nats.ClaimTTL(time.Minute),
			nats.DoneTTL(time.Hour),
		),
	)

	var handled atomic.Int32
	nats.Consume(app, "INVOICES", "mailer", func(_ context.Context, m *nats.Msg[Order]) error {
		fmt.Printf("sending invoice for order %s\n", m.Data.ID)
		handled.Add(1)
		return nil
	})

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer app.Shutdown(ctx)

	order := Order{ID: fmt.Sprintf("o-%d", time.Now().UnixNano())}
	var last uint64
	for range 3 {
		ack, err := app.PublishJS(ctx, "invoices.created", order,
			nats.WithHeader(nats.Header{"X-Order-Id": {order.ID}}))
		if err != nil {
			log.Fatal(err)
		}
		last = ack.Sequence
	}

	// Wait until the consumer has acked all three deliveries.
	cons, err := app.JetStream().Consumer(ctx, "INVOICES", "mailer")
	if err != nil {
		log.Fatal(err)
	}
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		info, err := cons.Info(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if info.AckFloor.Stream >= last {
			break
		}
		if time.Now().After(deadline) {
			log.Fatal("timed out waiting for the consumer")
		}
	}
	if n := handled.Load(); n == 1 {
		fmt.Println("processed once")
	} else {
		fmt.Printf("processed %d times\n", n)
	}
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://127.0.0.1:4222"
}
