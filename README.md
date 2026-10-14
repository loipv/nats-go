# nats-go

A Go framework for building [NATS](https://nats.io) and JetStream services on top of the official [nats.go](https://github.com/nats-io/nats.go) client.

- **Typed handlers** with generics: `func(ctx context.Context, m *nats.Msg[Order]) error`
- **Core NATS**: wildcards, queue groups, request/reply with error propagation
- **JetStream**: pull, push and ordered consumers, automatic or manual ack
- **Publishing**: fire-and-forget, typed request/reply, JetStream with ack and deduplication
- **Streams and consumers declared in code**, created or updated on startup
- **Middleware** on the inbound and outbound paths
- **Fail-fast startup**, **graceful shutdown** with a deadline, **health** endpoint
- Every auth method and TLS supported by nats.go

## Install

```sh
go get github.com/loipv/nats-go
```

Requires Go 1.26+ and nats-server 2.10+.

## Quick start

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/loipv/nats-go/nats"
	"github.com/nats-io/nats.go/jetstream"
)

type Order struct {
	ID string `json:"id"`
}

func main() {
	app, err := nats.New(nats.WithURL("nats://127.0.0.1:4222"))
	if err != nil {
		log.Fatal(err)
	}

	app.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	app.Consumer("ORDERS", jetstream.ConsumerConfig{Durable: "billing", AckPolicy: jetstream.AckExplicitPolicy})

	nats.Consume(app, "ORDERS", "billing", func(ctx context.Context, m *nats.Msg[Order]) error {
		log.Printf("billing order %s", m.Data.ID)
		return nil // ack; return an error to nak, nats.Terminal(err) to term
	})

	// Accept checkout requests from core NATS and store them in the stream.
	nats.Subscribe(app, "checkout.*", func(ctx context.Context, m *nats.Msg[Order]) error {
		_, err := app.PublishJS(ctx, "orders.created", m.Data, nats.MsgID(m.Data.ID))
		return err
	}, nats.Queue("workers"))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := app.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

## Examples

Start a server with `nats-server -js`, then run any example with `go run ./examples/<name>`. Set `NATS_URL` to use another server.

| Example | Shows |
|---|---|
| [subscribe](examples/subscribe) | Wildcards, queue groups, concurrent handlers |
| [request-reply](examples/request-reply) | `Reply`, typed `Request`, `ServiceError` |
| [jetstream](examples/jetstream) | Declared stream and consumers, `PublishJS` with `MsgID`, automatic and manual ack |
| [ordered](examples/ordered) | `ConsumeOrdered` with subject filters |
| [middleware](examples/middleware) | `Use`, `With`, `Recover`, `Logger`, `UsePublish` |
| [service](examples/service) | `Run` with signals, validation, health endpoint, credentials |

## How handler errors are handled

| Handler returns | JetStream message | Reply |
|---|---|---|
| `nil` | `Ack` | encoded response |
| any other error | `Nak` (redelivered) | `Nats-Service-Error`, code 500 |
| `nats.Terminal(err)`, decode or validation failure | `Term` (never redelivered) | `Nats-Service-Error`, code 400 |

With `nats.ManualAck()` the handler acks itself via `m.Ack()`, `m.Nak()`, `m.NakWithDelay(d)`, `m.Term()` or `m.InProgress()`.

A payload type with a `Validate() error` method is validated after decoding.

## Lifecycle

- `Start(ctx)` connects, creates or updates declared streams and consumers (never deletes them), checks that consumed consumers exist and starts every handler. The first failure stops everything and is returned. Registration mistakes are returned here too.
- `Shutdown(ctx)` stops intake, waits for in-flight handlers and drains the connection within the context deadline. Unacked JetStream messages are redelivered by the server.
- `Run(ctx)` is `Start`, wait for `ctx`, then `Shutdown` bounded by `WithShutdownTimeout` (30s by default).
- `Health()` and `HealthHandler()` report the connection and every handler; mount the handler on your own HTTP server.

## Authentication and TLS

```go
nats.New(
	nats.WithURL("tls://nats.example.com:4222"),
	nats.UserCredentials("service.creds"), // or UserInfo, Token, NKeyFromSeed, UserJWTAndSeed
	nats.RootCAs("ca.pem"),
	nats.ClientCert("client.pem", "client-key.pem"),
)
```

Any other nats.go option can be passed with `nats.WithNATSOptions`. The package is named `nats` like nats.go, so import nats.go under an alias when you need it:

```go
import natsio "github.com/nats-io/nats.go"

nats.WithNATSOptions(natsio.MaxReconnects(-1))
```

## Roadmap

| Milestone | Status | Scope |
|---|---|---|
| M1 Core | available | Everything above |
| M2 Reliability | planned | Retry with backoff, dead-letter queue, idempotency (in-memory or KV), batch handlers, handler auto-restart |
| M3 Integrations | planned | NATS micro services, OpenTelemetry, CloudEvents, batch and atomic batch publish |

The API may change until v1.0.0.

## License

[MIT](LICENSE)
