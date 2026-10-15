// Command batch-publish publishes several messages in one call. PublishJSBatch
// is not atomic: each message is acked on its own. PublishJSAtomicBatch stores
// the whole batch or nothing: a batch with one subject outside the stream
// fails and leaves the stream unchanged. Atomic batches need nats-server 2.12
// or later and a stream with AllowAtomicPublish.
//
//	nats-server -js &
//	go run ./examples/batch-publish
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/loipv/nats-go/nats"
)

type Item struct {
	N int `json:"n"`
}

func main() {
	app, err := nats.New(nats.WithURL(natsURL()))
	if err != nil {
		log.Fatal(err)
	}
	app.Stream(jetstream.StreamConfig{
		Name:               "BATCHDEMO",
		Subjects:           []string{"batchdemo.>"},
		AllowAtomicPublish: true,
	})

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer app.Shutdown(ctx)

	stream, err := app.JetStream().Stream(ctx, "BATCHDEMO")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("messages in the stream at the start:", count(ctx, stream))

	// Not atomic: every message has its own ack, or an entry in Failed.
	msgs := make([]nats.BatchMsg, 5)
	for i := range msgs {
		msgs[i] = nats.BatchMsg{Subject: "batchdemo.plain", Data: Item{N: i}}
	}
	acks, err := app.PublishJSBatch(ctx, msgs)
	if err != nil {
		log.Fatal(err)
	}
	for i, ack := range acks {
		fmt.Printf("batch message %d stored at sequence %d\n", i, ack.Sequence)
	}
	fmt.Println("messages in the stream after PublishJSBatch:", count(ctx, stream))

	// Atomic: all three or none.
	ok := make([]nats.BatchMsg, 3)
	for i := range ok {
		ok[i] = nats.BatchMsg{Subject: "batchdemo.atomic", Data: Item{N: i}}
	}
	ack, err := app.PublishJSAtomicBatch(ctx, ok)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("atomic batch %s committed: %d messages, last sequence %d\n", ack.BatchID, ack.BatchSize, ack.Sequence)
	fmt.Println("messages in the stream after the atomic batch:", count(ctx, stream))

	// One subject belongs to no stream, so the server rejects the whole batch.
	bad := []nats.BatchMsg{
		{Subject: "batchdemo.atomic", Data: Item{N: 100}},
		{Subject: "elsewhere.atomic", Data: Item{N: 101}},
		{Subject: "batchdemo.atomic", Data: Item{N: 102}},
	}
	if _, err := app.PublishJSAtomicBatch(ctx, bad); err != nil {
		fmt.Println("atomic batch with a foreign subject failed:", err)
	}
	fmt.Println("messages in the stream after the failed batch:", count(ctx, stream))
}

func count(ctx context.Context, s jetstream.Stream) uint64 {
	info, err := s.Info(ctx)
	if err != nil {
		log.Fatal(err)
	}
	return info.State.Msgs
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://127.0.0.1:4222"
}
