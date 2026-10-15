// Command retry-dlq retries failing messages with exponential backoff and
// sends the ones that run out of attempts to a dead-letter queue. It covers a
// JetStream consumer (the server redelivers after the backoff) and a core
// subscription (the library retries in process), then reads the DLQ stream
// and prints the Nats-DLQ-* headers.
//
//	nats-server -js &
//	go run ./examples/retry-dlq
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/loipv/nats-go/nats"
	"github.com/nats-io/nats.go/jetstream"
)

type Payment struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
	// FailUntil is the attempt that finally succeeds; 0 means never.
	FailUntil int `json:"fail_until"`
}

func main() {
	app, err := nats.New(nats.WithURL(natsURL()))
	if err != nil {
		log.Fatal(err)
	}
	// The handlers report here, only for this run's payments (the durable
	// consumer may also redeliver ones left by an interrupted earlier run);
	// main waits on them one step at a time.
	run := time.Now().UnixNano()
	mine := func(id string) bool { return strings.HasSuffix(id, fmt.Sprintf("-%d", run)) }
	charged := make(chan string, 1)
	deadLettered := make(chan string, 2)

	// The DLQ is an ordinary stream that you declare: Start fails if no
	// stream captures the DLQ subject.
	app.Stream(jetstream.StreamConfig{Name: "PAYMENTS", Subjects: []string{"payments.>"}})
	app.Stream(jetstream.StreamConfig{Name: "DLQ", Subjects: []string{"dlq.>"}})
	app.Consumer("PAYMENTS", jetstream.ConsumerConfig{Durable: "charger", AckPolicy: jetstream.AckExplicitPolicy})

	// JetStream: up to 3 deliveries, 200ms then 400ms between them (with
	// jitter), then the message goes to dlq.payments and is termed.
	app.Consume("PAYMENTS", "charger", nats.Typed(func(_ context.Context, m *nats.Msg[Payment]) error {
		fmt.Printf("charger: %s attempt %d\n", m.Data.ID, m.Attempt())
		if m.Data.FailUntil > 0 && m.Attempt() >= m.Data.FailUntil {
			fmt.Printf("charger: %s charged\n", m.Data.ID)
			if mine(m.Data.ID) {
				charged <- m.Data.ID
			}
			return nil
		}
		return errors.New("card gateway unavailable")
	}),
		nats.WithMaxAttempts(3),
		nats.WithBackoff(nats.ExponentialBackoff(200*time.Millisecond, 2*time.Second)),
		nats.WithDLQ("dlq.payments"),
	)

	// Core NATS never redelivers, so the library retries in the worker:
	// 2 attempts, then the message goes to dlq.notify.
	app.Subscribe("notify.send", nats.Typed(func(_ context.Context, m *nats.Msg[Payment]) error {
		fmt.Printf("notifier: %s attempt %d\n", m.Data.ID, m.Attempt())
		return errors.New("mail server down")
	}),
		nats.WithMaxAttempts(2),
		nats.WithBackoff(nats.ExponentialBackoff(200*time.Millisecond, 2*time.Second)),
		nats.WithDLQ("dlq.notify"),
	)

	// Read the DLQ: only messages dead-lettered during this run.
	app.ConsumeOrdered("DLQ", nats.Typed(func(_ context.Context, m *nats.Msg[Payment]) error {
		var b strings.Builder
		fmt.Fprintf(&b, "dlq: %s\n", m.Data.ID)
		var keys []string
		for k := range m.Header {
			if strings.HasPrefix(k, "Nats-DLQ-") {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "  %s: %s\n", k, m.Header.Get(k))
		}
		if mine(m.Data.ID) {
			deadLettered <- b.String()
		}
		return nil
	}), nats.WithDeliverPolicy(jetstream.DeliverNewPolicy))

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := app.Shutdown(ctx); err != nil {
			log.Println("shutdown:", err)
		}
	}()

	flaky := Payment{ID: fmt.Sprintf("flaky-%d", run), Amount: 10, FailUntil: 2}
	broken := Payment{ID: fmt.Sprintf("broken-%d", run), Amount: 20}
	mail := Payment{ID: fmt.Sprintf("mail-%d", run), Amount: 30}
	// One scenario at a time, so the output reads in order.
	wait := func(ch <-chan string) string {
		select {
		case v := <-ch:
			return v
		case <-time.After(20 * time.Second):
			log.Fatal("timed out")
			return ""
		}
	}
	for _, p := range []Payment{flaky, broken} {
		if _, err := app.PublishJS(ctx, "payments.charge", p, nats.WithMsgID(p.ID)); err != nil {
			log.Fatal(err)
		}
		if p.FailUntil > 0 {
			wait(charged)
		} else {
			fmt.Print(wait(deadLettered))
		}
	}
	if err := app.Publish(ctx, "notify.send", mail); err != nil {
		log.Fatal(err)
	}
	fmt.Print(wait(deadLettered))
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://127.0.0.1:4222"
}
