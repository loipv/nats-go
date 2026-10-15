// Command middleware shows inbound middleware (app-wide and per handler),
// panic recovery and outbound publish middleware.
//
//	nats-server -js &
//	go run ./examples/middleware
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/loipv/nats-go/nats"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	app, err := nats.New(nats.WithURL(natsURL()), nats.WithLogger(logger))
	if err != nil {
		log.Fatal(err)
	}

	// App middleware wraps every handler, first one outermost.
	app.Use(nats.Recover(), nats.Logger(logger))

	// Publish middleware runs on every outgoing message.
	app.UsePublish(func(next nats.PublishFunc) nats.PublishFunc {
		return func(ctx context.Context, m *nats.RawMsg) error {
			if m.Header == nil {
				m.Header = nats.Header{}
			}
			m.Header.Set("X-Request-Id", fmt.Sprintf("req-%d", time.Now().UnixNano()))
			return next(ctx, m)
		}
	})

	// Handler middleware runs inside the app middleware.
	timing := func(next nats.Handler) nats.Handler {
		return func(ctx context.Context, m *nats.RawMsg) error {
			begin := time.Now()
			err := next(ctx, m)
			fmt.Printf("timing: %s took %s\n", m.Subject, time.Since(begin))
			return err
		}
	}

	greeted, crashed := make(chan struct{}), make(chan struct{})
	app.Subscribe("greet", nats.Typed(func(_ context.Context, m *nats.Msg[string]) error {
		defer close(greeted)
		fmt.Printf("hello %s (request id %s)\n", m.Data, m.Header.Get("X-Request-Id"))
		return nil
	}), nats.WithMiddleware(timing))

	// Recover turns this panic into an error; the subscription keeps running.
	app.Subscribe("crash", nats.Typed(func(context.Context, *nats.Msg[[]byte]) error {
		defer close(crashed)
		panic("something went wrong")
	}))

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer app.Shutdown(ctx)

	_ = app.Publish(ctx, "greet", "gopher")
	_ = app.Publish(ctx, "crash", []byte("x"))
	<-greeted
	<-crashed
	time.Sleep(50 * time.Millisecond) // let the logger print the recovered panic
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://127.0.0.1:4222"
}
