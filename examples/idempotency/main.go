// Command idempotency processes an order once per handler even though it is
// published three times. WithIdempotency claims a key in a JetStream
// key-value bucket before the handler runs and remembers it afterwards, so
// duplicates are acked without running the handler. Keys are scoped per
// handler: the mailer and the archiver share the bucket and each handle the
// order once. KVStore needs nats-server 2.11 or later.
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
	for _, name := range []string{"mailer", "archiver"} {
		app.Consumer("INVOICES", jetstream.ConsumerConfig{Durable: name, AckPolicy: jetstream.AckExplicitPolicy})
	}

	// The bucket is created or updated on Start. Use NewMemoryStore instead
	// when a single instance consumes the messages.
	store := nats.NewKVStore(app, "IDEMPOTENCY")
	app.Use(nats.Recover())
	// App-wide settings; each handler opts in with WithIdempotency.
	app.Idempotency(store,
		nats.WithIdempotencyKey(func(m *nats.RawMsg) string { return m.Header.Get("X-Order-Id") }),
		// Must exceed the longest the handler can run.
		nats.WithClaimTTL(time.Minute),
		nats.WithDoneTTL(time.Hour),
	)

	var mailed, archived atomic.Int32
	app.Consume("INVOICES", "mailer", nats.Typed(func(_ context.Context, m *nats.Msg[Order]) error {
		fmt.Printf("sending invoice for order %s\n", m.Data.ID)
		mailed.Add(1)
		return nil
	}), nats.WithIdempotency()) // scope "INVOICES/mailer"
	app.Consume("INVOICES", "archiver", nats.Typed(func(_ context.Context, m *nats.Msg[Order]) error {
		fmt.Printf("archiving invoice for order %s\n", m.Data.ID)
		archived.Add(1)
		return nil
	}), nats.WithIdempotency()) // scope "INVOICES/archiver"

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := app.Shutdown(ctx); err != nil {
			log.Println("shutdown:", err)
		}
	}()

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

	// Wait until both consumers have acked all three deliveries.
	for _, name := range []string{"mailer", "archiver"} {
		waitAcked(ctx, app, name, last)
	}
	fmt.Printf("mailer ran %d time(s), archiver %d time(s)\n", mailed.Load(), archived.Load())
}

func waitAcked(ctx context.Context, app *nats.App, consumer string, seq uint64) {
	cons, err := app.JetStream().Consumer(ctx, "INVOICES", consumer)
	if err != nil {
		log.Fatal(err)
	}
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		info, err := cons.Info(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if info.AckFloor.Stream >= seq {
			return
		}
		if time.Now().After(deadline) {
			log.Fatalf("timed out waiting for %s", consumer)
		}
	}
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://127.0.0.1:4222"
}
