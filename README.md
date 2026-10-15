# nats-go

A Go framework for building [NATS](https://nats.io) and JetStream services on top of the official [nats.go](https://github.com/nats-io/nats.go) client.

- **Typed handlers** with generics: `func(ctx context.Context, m *nats.Msg[Order]) error`
- **Core NATS**: wildcards, queue groups, request/reply with error propagation
- **JetStream**: pull, push and ordered consumers, automatic or manual ack
- **Publishing**: fire-and-forget, typed request/reply, JetStream with ack and deduplication
- **Reliability**: retry with backoff, dead-letter queue, idempotency (in-memory or KV), batch handlers, automatic restart of consumers, panic recovery
- **Streams and consumers declared in code**, created or updated on startup
- **Middleware** on the inbound and outbound paths
- **Fail-fast startup**, **graceful shutdown** with a deadline, **health** endpoint
- Every auth method and TLS supported by nats.go

## Install

```sh
go get github.com/loipv/nats-go
```

Requires Go 1.26+ and nats-server 2.10+. `KVStore` requires nats-server 2.11+.

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
| [retry-dlq](examples/retry-dlq) | `MaxAttempts`, `WithBackoff`, `DLQ` on a JetStream consumer and a core subscription, `Nats-DLQ-*` headers |
| [idempotency](examples/idempotency) | `Idempotent` with a `KVStore`, a custom key, one execution for three deliveries |
| [batch](examples/batch) | `ConsumeBatch`, `BatchError`, retry and DLQ per message |

## How handler errors are handled

| Handler returns | JetStream message | Retry | DLQ | Reply |
|---|---|---|---|---|
| `nil` | `Ack` | - | - | encoded response |
| any other error | `NakWithDelay` by the backoff, until `MaxAttempts` is used up; then `Term` | yes, unless `MaxAttempts` is 1 | after the last attempt | `Nats-Service-Error`, code 500 |
| `nats.Terminal(err)`, decode or validation failure | `Term` (never redelivered) | no | immediately | `Nats-Service-Error`, code 400 |
| panic | handled as any other error | as above | as above | as above |

Retry and DLQ apply when `DLQ(subject)` is set; without it a message that ran out of attempts is logged and termed. `Consume` and `ConsumeBatch` retry forever by default; `Subscribe`, `Reply` and `ConsumeOrdered` do not retry unless `MaxAttempts(n)` is set.

With `nats.ManualAck()` the handler acks itself via `m.Ack()`, `m.Nak()`, `m.NakWithDelay(d)`, `m.Term()` or `m.InProgress()`.

A payload type with a `Validate() error` method is validated after decoding.

## Reliability

### Retry and DLQ

```go
app.Stream(jetstream.StreamConfig{Name: "DLQ", Subjects: []string{"dlq.>"}}) // you declare the DLQ stream

nats.Consume(app, "PAYMENTS", "charger", charge,
	nats.MaxAttempts(5),
	nats.WithBackoff(nats.ExponentialBackoff(time.Second, time.Minute)), // the default
	nats.DLQ("dlq.payments"),
)
```

- With `Consume` and `ConsumeBatch` the server redelivers: a failed message is naked with a delay, and `m.Attempt()` is the delivery count. `MaxDeliver` on the consumer must be `-1` or greater than `MaxAttempts`, or `Start` fails.
- With `Subscribe`, `Reply` and `ConsumeOrdered` there is no redelivery, so `MaxAttempts(n)` makes the library retry in the worker, waiting for the backoff between attempts. `Unlimited` can stall the subscription while failures last, a `Reply` must finish its retries within the requester's timeout, and `ConsumeOrdered` stalls the whole stream while it retries (which keeps the order).
- `DLQ(subject)` publishes the original payload and headers plus `Nats-DLQ-Error`, `Nats-DLQ-Subject`, `Nats-DLQ-Deliveries` and, for JetStream consumers, `Nats-DLQ-Stream`, `Nats-DLQ-Consumer`, `Nats-DLQ-Sequence` and `Nats-DLQ-Msg-Id`. `Start` fails if no stream captures the subject.
- `MaxAttempts`, `WithBackoff` and `DLQ` cannot be combined with `ManualAck`.
- `AutoInProgress()` extends the ack wait of a `Consume` or `ConsumeBatch` message while the handler runs; use it with `Timeout(d)`.

### Idempotency

```go
store := nats.NewKVStore(app, "IDEMPOTENCY") // or nats.NewMemoryStore() for a single instance
app.Use(nats.Recover(), nats.Idempotent(store,
	nats.IdempotencyKey(func(m *nats.RawMsg) string { return m.Header.Get("X-Order-Id") }), // default: Nats-Msg-Id
	nats.ClaimTTL(time.Minute),
	nats.DoneTTL(24*time.Hour),
))
```

A key is claimed before the handler runs and remembered once it succeeds; a duplicate is acked without running the handler. A failing or panicking handler releases the claim so the retry can run. `ClaimTTL` must exceed the worst-case handler duration, because claims are released by key without an ownership token. `KVStore` is correct across instances, needs nats-server 2.11+ and rounds every TTL up to a whole second. `MemoryStore` is only correct for a single instance. Idempotency does not apply to `ConsumeBatch`.

### Batch handlers

```go
nats.ConsumeBatch(app, "ORDERS", "loader", func(ctx context.Context, msgs []*nats.Msg[Order]) error {
	failed := map[int]error{}
	// ... record the index of every message that failed
	if len(failed) > 0 {
		return &nats.BatchError{Failed: failed} // only these are retried; the rest are acked
	}
	return nil // ack all
}, nats.BatchSize(100), nats.BatchWait(time.Second))
```

Any other error fails the whole batch. `ConsumeBatch` needs a pull consumer, and middleware does not apply to it.

### Supervisor

`Consume` and `ConsumeBatch` handlers restart when their consumer or stream is deleted: the handler reports `restarting` (and `Health` reports `degraded`), recreates the stream and consumer if they were declared with `app.Stream` and `app.Consumer`, and resumes, waiting `WithRestartBackoff` (default `ExponentialBackoff(1s, 30s)`) between attempts. `HandlerStatus.Restarts` counts restarts. Set `IdleHeartbeat` on push consumers: without it a deleted push consumer cannot be detected.

### Panics

A panic in a handler is recovered by the worker, logged with its stack and handled like any other error. `nats.Recover()` is still useful to make middleware around the handler see the panic as an error.

## Lifecycle

- `Start(ctx)` connects, creates or updates declared streams and consumers (never deletes them), checks that consumed consumers exist and starts every handler. The first failure stops everything and is returned. Registration mistakes are returned here too.
- `Shutdown(ctx)` stops intake, waits for in-flight handlers and drains the connection within the context deadline. Unacked JetStream messages are redelivered by the server.
- `Run(ctx)` is `Start`, wait for `ctx`, then `Shutdown` bounded by `WithShutdownTimeout` (30s by default).
- The connection reconnects forever by default (`nats.WithNATSOptions(natsio.MaxReconnects(n))` to change it); while reconnecting, health reports `degraded`.
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

## Behaviour changes in M2

Three things behave differently from M1:

1. A failed `Consume` message is naked with a delay from the default backoff (`ExponentialBackoff(1s, 1m)`, retried without limit) instead of immediately. Pass `WithBackoff` or `MaxAttempts` to change it.
2. A handler panic no longer crashes the process: the worker recovers it and treats it as an error.
3. A deleted consumer no longer leaves its handler `stopped` for good: it goes to `restarting` and recovers.

## Roadmap

| Milestone | Status | Scope |
|---|---|---|
| M1 Core | available | Typed handlers, core and JetStream consumers, publishing, middleware, lifecycle, health |
| M2 Reliability | available | Retry with backoff, dead-letter queue, idempotency (in-memory or KV), batch handlers, handler auto-restart |
| M3 Integrations | planned | NATS micro services, OpenTelemetry, CloudEvents, batch and atomic batch publish |

The API may change until v1.0.0.

## License

[MIT](LICENSE)
