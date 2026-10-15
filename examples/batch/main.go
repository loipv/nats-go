// Command batch handles messages in batches. Orders with an odd ID are
// reported as failed with a *BatchError: they are retried once more and then
// sent to a dead-letter queue, while the rest of each batch is acked.
//
//	nats-server -js &
//	go run ./examples/batch
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
	ID int `json:"id"`
}

const total = 25

func main() {
	app, err := nats.New(nats.WithURL(natsURL()))
	if err != nil {
		log.Fatal(err)
	}

	app.Stream(jetstream.StreamConfig{Name: "ORDERS_BATCH", Subjects: []string{"ordersbatch.>"}})
	app.Stream(jetstream.StreamConfig{Name: "ORDERS_BATCH_DLQ", Subjects: []string{"batchdlq.>"}})
	app.Consumer("ORDERS_BATCH", jetstream.ConsumerConfig{Durable: "loader", AckPolicy: jetstream.AckExplicitPolicy})

	var batches atomic.Int32
	nats.ConsumeBatch(app, "ORDERS_BATCH", "loader", func(_ context.Context, msgs []*nats.Msg[Order]) error {
		fmt.Printf("batch %d: %d messages\n", batches.Add(1), len(msgs))
		failed := map[int]error{}
		for i, m := range msgs {
			if m.Data.ID%2 == 1 {
				failed[i] = fmt.Errorf("order %d rejected", m.Data.ID)
			}
		}
		if len(failed) > 0 {
			return &nats.BatchError{Failed: failed}
		}
		return nil
	},
		nats.BatchSize(10),
		nats.BatchWait(500*time.Millisecond),
		nats.MaxAttempts(2),
		nats.WithBackoff(nats.ExponentialBackoff(200*time.Millisecond, time.Second)),
		nats.DLQ("batchdlq.orders"),
	)

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer app.Shutdown(ctx)

	js := app.JetStream()
	dlq, err := js.Stream(ctx, "ORDERS_BATCH_DLQ")
	if err != nil {
		log.Fatal(err)
	}
	before := dlqCount(ctx, dlq)

	var last uint64
	for i := range total {
		ack, err := app.PublishJS(ctx, "ordersbatch.created", Order{ID: i})
		if err != nil {
			log.Fatal(err)
		}
		last = ack.Sequence
	}

	// Done when every message is acked or dead-lettered: the odd IDs
	// (12 of 25) come back once, then move to the DLQ.
	const wantDead = total / 2
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		cons, err := js.Consumer(ctx, "ORDERS_BATCH", "loader")
		if err != nil {
			log.Fatal(err)
		}
		info, err := cons.Info(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if info.AckFloor.Stream >= last && dlqCount(ctx, dlq)-before >= wantDead {
			break
		}
		if time.Now().After(deadline) {
			log.Fatal("timed out waiting for the batches")
		}
	}
	fmt.Printf("%d messages in the DLQ\n", dlqCount(ctx, dlq)-before)
}

func dlqCount(ctx context.Context, s jetstream.Stream) uint64 {
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
