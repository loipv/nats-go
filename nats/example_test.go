package nats_test

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/loipv/nats-go/nats"
)

type Order struct {
	ID string `json:"id"`
}

func ExampleNew() {
	app, err := nats.New(nats.WithURL("nats://127.0.0.1:4222"), nats.WithName("orders"))
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// Run blocks until ctx is cancelled, then shuts down gracefully.
	if err := app.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func ExampleTyped() {
	app, err := nats.New()
	if err != nil {
		log.Fatal(err)
	}
	app.Subscribe("orders.created", nats.Typed(func(ctx context.Context, m *nats.Msg[Order]) error {
		log.Println("order", m.Data.ID)
		return nil
	}), nats.WithConcurrency(4))
}

func ExampleApp_Hook() {
	app, err := nats.New()
	if err != nil {
		log.Fatal(err)
	}
	app.Hook(nats.Hook{
		// OnStart runs before any handler sees a message, and may publish.
		OnStart: func(ctx context.Context) error {
			return app.Publish(ctx, "orders.service.up", Order{})
		},
		// OnStop runs after the handlers stopped, while publishing still works.
		OnStop: func(ctx context.Context) error {
			return app.Publish(ctx, "orders.service.down", Order{})
		},
	})
}
