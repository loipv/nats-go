// Command micro-service runs a NATS micro service named "inventory" with two
// endpoints, one of them in a group, until SIGINT/SIGTERM. Requests are
// validated and answered like Reply handlers, and the service shows up in the
// nats CLI:
//
//	nats-server -js &
//	go run ./examples/micro-service
//	nats micro info inventory                      # with the nats CLI
//	nats request inventory.get '{"sku":"apple"}'
//	nats request inventory.admin.reset '{}'
//	nats micro stats inventory
//
// Endpoints are handled one request at a time per instance; run more
// instances (they share a queue group) to scale.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/nats-io/nats.go/micro"

	"github.com/loipv/nats-go/nats"
)

type GetStock struct {
	SKU string `json:"sku"`
}

// Validate is called after decoding; a failure is answered with code 400.
func (r GetStock) Validate() error {
	if r.SKU == "" {
		return errors.New("sku is required")
	}
	return nil
}

type Stock struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type Reset struct{}

type ResetResult struct {
	Items int `json:"items"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app, err := nats.New(nats.WithURL(natsURL()), nats.Name("inventory"))
	if err != nil {
		log.Fatal(err)
	}
	app.Use(nats.Recover())

	var mu sync.Mutex
	stock := map[string]int{"apple": 10, "pear": 4}

	svc := app.Service(micro.Config{
		Name:        "inventory",
		Version:     "1.0.0",
		Description: "Stock levels",
	})

	// Subject "inventory.get".
	inv := svc.Group("inventory")
	inv.Endpoint("get", nats.TypedReply(func(_ context.Context, m *nats.Msg[GetStock]) (Stock, error) {
		mu.Lock()
		defer mu.Unlock()
		q, ok := stock[m.Data.SKU]
		if !ok {
			// Terminal errors reach the caller with code 400, others with 500.
			return Stock{}, nats.Terminal(errors.New("unknown sku"))
		}
		return Stock{SKU: m.Data.SKU, Quantity: q}, nil
	}), nats.EndpointMetadata(map[string]string{"owner": "warehouse"}))

	// Subject "inventory.admin.reset".
	admin := inv.Group("admin")
	admin.Endpoint("reset", nats.TypedReply(func(_ context.Context, _ *nats.Msg[Reset]) (ResetResult, error) {
		mu.Lock()
		defer mu.Unlock()
		n := len(stock)
		clear(stock)
		return ResetResult{Items: n}, nil
	}))

	log.Println("inventory 1.0.0 is up; try: nats micro info inventory")
	if err := app.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://127.0.0.1:4222"
}
