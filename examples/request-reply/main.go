// Command request-reply shows Reply and Request with typed payloads and
// error propagation through ServiceError.
//
//	nats-server -js &
//	go run ./examples/request-reply
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/loipv/nats-go/nats"
)

func main() {
	app, err := nats.New(nats.WithURL(natsURL()))
	if err != nil {
		log.Fatal(err)
	}

	nats.Reply(app, "math.double", func(_ context.Context, m *nats.Msg[int]) (int, error) {
		if m.Data < 0 {
			// Terminal errors reach the caller with code 400, others with 500.
			return 0, nats.Terminal(errors.New("negative input"))
		}
		return m.Data * 2, nil
	})

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer app.Shutdown(ctx)

	// The request timeout comes from the context.
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := nats.Request[int](reqCtx, app, "math.double", 21)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("double(21) =", resp.Data)

	_, err = nats.Request[int](reqCtx, app, "math.double", -1)
	var se *nats.ServiceError
	if errors.As(err, &se) {
		fmt.Printf("double(-1) failed: code %d: %s\n", se.Code, se.Description)
	}
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://127.0.0.1:4222"
}
