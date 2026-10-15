// Command hooks-events shows App.Hook and the WithOn* event options. A handler
// that always fails is retried once and then dead-lettered; the program prints
// each hook and event as it happens, then shuts down.
//
//	nats-server -js &
//	go run ./examples/hooks-events
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/loipv/nats-go/nats"
	"github.com/nats-io/nats.go/jetstream"
)

type Job struct {
	ID string `json:"id"`
}

func main() {
	deadLettered := make(chan struct{})
	app, err := nats.New(
		nats.WithURL(natsURL()),
		nats.WithOnRetry(func(e nats.MsgEvent) {
			fmt.Printf("event: retry %s attempt %d: %v\n", e.Subject, e.Attempt, e.Err)
		}),
		nats.WithOnDeadLettered(func(e nats.MsgEvent) {
			fmt.Printf("event: dead-lettered %s after attempt %d: %v\n", e.Subject, e.Attempt, e.Err)
			close(deadLettered)
		}),
		nats.WithOnDisconnected(func(err error) {
			fmt.Printf("event: disconnected: %v\n", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	app.Hook(nats.Hook{
		OnStart: func(context.Context) error { fmt.Println("hook: start"); return nil },
		OnStop:  func(context.Context) error { fmt.Println("hook: stop"); return nil },
	})

	app.Stream(jetstream.StreamConfig{Name: "HOOKDEMO", Subjects: []string{"hookdemo.>"}})
	app.Stream(jetstream.StreamConfig{Name: "DLQ", Subjects: []string{"dlq.>"}})
	app.Consumer("HOOKDEMO", jetstream.ConsumerConfig{Durable: "worker", AckPolicy: jetstream.AckExplicitPolicy})

	// Always fails: 2 attempts with a short backoff, then dlq.hookdemo.
	app.Consume("HOOKDEMO", "worker", nats.Typed(func(_ context.Context, m *nats.Msg[Job]) error {
		return errors.New("job failed")
	}),
		nats.WithMaxAttempts(2),
		nats.WithBackoff(nats.ExponentialBackoff(100*time.Millisecond, time.Second)),
		nats.WithDLQ("dlq.hookdemo"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := app.Start(ctx); err != nil {
		log.Fatal(err)
	}

	id := fmt.Sprintf("job-%d", time.Now().UnixNano())
	if _, err := app.PublishJS(ctx, "hookdemo.run", Job{ID: id}, nats.WithMsgID(id)); err != nil {
		log.Fatal(err)
	}

	select {
	case <-deadLettered:
	case <-ctx.Done():
		log.Fatal("timed out waiting for the dead-letter event")
	}

	if err := app.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://127.0.0.1:4222"
}
