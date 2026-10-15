// Command service is a long-running service: it serves requests until
// SIGINT/SIGTERM, exposes a health endpoint and shuts down gracefully.
//
//	nats-server -js &
//	go run ./examples/service
//	nats request api.orders.get '{"id":"42"}'   # with the nats CLI
//	curl localhost:8080/healthz
//
// Set NATS_CREDS to a .creds file to authenticate with a JWT.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/loipv/nats-go/nats"
)

type GetOrder struct {
	ID string `json:"id"`
}

// Validate is called automatically after decoding. A failure is answered
// with code 400 without calling the handler.
func (r GetOrder) Validate() error {
	if r.ID == "" {
		return errors.New("id is required")
	}
	return nil
}

type Order struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts := []nats.Option{
		nats.WithURL(natsURL()),
		nats.Name("orders-service"),
		nats.WithShutdownTimeout(10 * time.Second),
	}
	if creds := os.Getenv("NATS_CREDS"); creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}
	app, err := nats.New(opts...)
	if err != nil {
		log.Fatal(err)
	}
	app.Use(nats.Recover())

	app.Reply("api.orders.get", nats.TypedReply(func(_ context.Context, m *nats.Msg[GetOrder]) (Order, error) {
		return Order{ID: m.Data.ID, Status: "shipped"}, nil
	}), nats.Queue("orders-service"))

	mux := http.NewServeMux()
	mux.Handle("/healthz", app.HealthHandler())
	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	// Run blocks until ctx is cancelled, then drains within the shutdown timeout.
	runErr := app.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	if runErr != nil {
		log.Fatal(runErr)
	}
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://127.0.0.1:4222"
}
