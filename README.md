# nats-go

A Go framework for building [NATS](https://nats.io) and JetStream services on top of the official [nats.go](https://github.com/nats-io/nats.go) client.

- **Typed handlers** with generics: `app.Subscribe("orders.*", nats.Typed(func(ctx context.Context, m *nats.Msg[Order]) error { ... }))`
- **Core NATS**: wildcards, queue groups, request/reply with error propagation
- **JetStream**: pull, push and ordered consumers, automatic or manual ack
- **Publishing**: fire-and-forget, typed request/reply, JetStream with ack and deduplication
- **Reliability**: retry with backoff, dead-letter queue, idempotency (in-memory or KV), batch handlers, automatic restart of consumers, panic recovery
- **Integrations**: NATS micro services, batch and atomic batch publishing, OpenTelemetry tracing and metrics, CloudEvents
- **Streams and consumers declared in code**, created or updated on startup
- **Middleware** on the inbound and outbound paths
- **Fail-fast startup**, **graceful shutdown** with a deadline, **health** endpoint
- Every auth method and TLS supported by nats.go

## Install

```sh
go get github.com/loipv/nats-go
```

Requires Go 1.26+ and nats-server 2.10+. `KVStore` requires nats-server 2.11+, and atomic batch publishing (`atomicbatch`) requires 2.12+ and a stream with `AllowAtomicPublish`.

The core `nats` package has no dependency beyond nats.go. OpenTelemetry (`natsotel`) and atomic batches (`atomicbatch`) live in subpackages, so you only pull in their dependencies when you import them.

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

	app.Consume("ORDERS", "billing", nats.Typed(func(ctx context.Context, m *nats.Msg[Order]) error {
		log.Printf("billing order %s", m.Data.ID)
		return nil // ack; return an error to nak, nats.Terminal(err) to term
	}))

	// Accept checkout requests from core NATS and store them in the stream.
	app.Subscribe("checkout.*", nats.Typed(func(ctx context.Context, m *nats.Msg[Order]) error {
		_, err := app.PublishJS(ctx, "orders.created", m.Data, nats.MsgID(m.Data.ID))
		return err
	}), nats.Queue("workers"))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := app.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

Handlers are registered with `App` methods and wrapped in `nats.Typed` (`nats.TypedReply` for `Reply` and service endpoints, `nats.TypedBatch` for `ConsumeBatch`), because Go methods cannot have type parameters. The payload type is inferred from the handler, and passing a handler of the wrong kind does not compile.

## Examples

Start a server with `nats-server -js`, then run any example with `go run ./examples/<name>`. Set `NATS_URL` to use another server.

| Example | Shows |
|---|---|
| [subscribe](examples/subscribe) | Wildcards, queue groups, concurrent handlers |
| [request-reply](examples/request-reply) | `Reply`, `Request` decoding into a value, `ServiceError` |
| [jetstream](examples/jetstream) | Declared stream and consumers, `PublishJS` with `MsgID`, automatic and manual ack |
| [ordered](examples/ordered) | `ConsumeOrdered` with subject filters |
| [middleware](examples/middleware) | `Use`, `With`, `Recover`, `Logger`, `UsePublish` |
| [service](examples/service) | `Run` with signals, validation, health endpoint, credentials |
| [retry-dlq](examples/retry-dlq) | `MaxAttempts`, `WithBackoff`, `DLQ` on a JetStream consumer and a core subscription, `Nats-DLQ-*` headers |
| [idempotency](examples/idempotency) | `Idempotent` with a `KVStore`, a custom key, one execution for three deliveries |
| [batch](examples/batch) | `ConsumeBatch`, `BatchError`, retry and DLQ per message |
| [micro-service](examples/micro-service) | `Service`, `Group` and `Endpoint`; found by `nats micro info inventory` |
| [otel](examples/otel) | `natsotel` middleware: producer and consumer spans in one trace, printed to stdout |
| [cloudevents](examples/cloudevents) | `cloudevents.New`, `Handle` and `Publish` with an extension |
| [batch-publish](examples/batch-publish) | `PublishJSBatch`, and `atomicbatch.Publish` rolling back a batch with a foreign subject |

## How handler errors are handled

| Handler returns | JetStream message | Retry | DLQ | Reply |
|---|---|---|---|---|
| `nil` | `Ack` | - | - | encoded response |
| any other error | `NakWithDelay` by the backoff, until `MaxAttempts` is used up; then `Term` | yes, unless `MaxAttempts` is 1 | after the last attempt, if `DLQ(subject)` is set | `Nats-Service-Error`, code 500 |
| `nats.Terminal(err)`, decode or validation failure | `Term` (never redelivered) | no | immediately, if `DLQ(subject)` is set | `Nats-Service-Error`, code 400 |
| panic | handled as any other error | as above | as above | as above |

Retry and DLQ apply when `DLQ(subject)` is set; without it a message that ran out of attempts is logged and termed. `Consume` and `ConsumeBatch` retry forever by default; `Subscribe`, `Reply` and `ConsumeOrdered` do not retry unless `MaxAttempts(n)` is set.

With `nats.ManualAck()` the handler acks itself via `m.Ack()`, `m.Nak()`, `m.NakWithDelay(d)`, `m.Term()` or `m.InProgress()`.

A payload type with a `Validate() error` method is validated after decoding.

## Reliability

### Retry and DLQ

```go
app.Stream(jetstream.StreamConfig{Name: "DLQ", Subjects: []string{"dlq.>"}}) // you declare the DLQ stream

app.Consume("PAYMENTS", "charger", nats.Typed(charge),
	nats.MaxAttempts(5),
	nats.WithBackoff(nats.ExponentialBackoff(time.Second, time.Minute)), // the default
	nats.DLQ("dlq.payments"),
)
```

- With `Consume` and `ConsumeBatch` the server redelivers: a failed message is naked with a delay, and `m.Attempt()` is the delivery count. `MaxDeliver` on the consumer must be `-1` or greater than `MaxAttempts`, or `Start` fails.
- With `Subscribe`, `Reply` and `ConsumeOrdered` there is no redelivery, so `MaxAttempts(n)` makes the library retry in the worker, waiting for the backoff between attempts. `Unlimited` can stall the subscription while failures last, a `Reply` must finish its retries within the requester's timeout, and `ConsumeOrdered` stalls the whole stream while it retries (which keeps the order).
- `DLQ(subject)` publishes the original payload and headers plus `Nats-DLQ-Error`, `Nats-DLQ-Subject`, `Nats-DLQ-Deliveries` and, for JetStream consumers, `Nats-DLQ-Stream`, `Nats-DLQ-Consumer`, `Nats-DLQ-Sequence` and `Nats-DLQ-Msg-Id`. `Start` fails if no stream captures the subject, if it is the handler's source stream, or if a `Subscribe` or `Reply` handler's own subject matches it.
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

A key is claimed before the handler runs and remembered once it succeeds; duplicates are acked without calling the handler, and a duplicate request gets a 400; a duplicate is never retried or sent to the DLQ. A failing or panicking handler releases the claim so the retry can run. `ClaimTTL` must exceed the worst-case handler duration, because claims are released by key without an ownership token. `KVStore` is correct across instances, needs nats-server 2.11+ and rounds every TTL up to a whole second. `MemoryStore` is only correct for a single instance. Idempotency does not apply to `ConsumeBatch`.

### Batch handlers

```go
app.ConsumeBatch("ORDERS", "loader", nats.TypedBatch(func(ctx context.Context, msgs []*nats.Msg[Order]) error {
	failed := map[int]error{}
	// ... record the index of every message that failed
	if len(failed) > 0 {
		return &nats.BatchError{Failed: failed} // only these are retried; the rest are acked
	}
	return nil // ack all
}), nats.BatchSize(100), nats.BatchWait(time.Second))
```

Any other error fails the whole batch. `ConsumeBatch` needs a pull consumer, and middleware does not apply to it.

### Supervisor

`Consume` and `ConsumeBatch` handlers restart when their consumer or stream is deleted: the handler reports `restarting` (and `Health` reports `degraded`), recreates the stream and consumer if they were declared with `app.Stream` and `app.Consumer`, and resumes, waiting `WithRestartBackoff` (default `ExponentialBackoff(1s, 30s)`) between attempts. `HandlerStatus.Restarts` counts restarts. Set `IdleHeartbeat` on push consumers: without it a deleted push consumer cannot be detected.

### Panics

A panic in a handler is recovered by the worker, logged with its stack and handled like any other error. `nats.Recover()` is still useful to make middleware around the handler see the panic as an error.

## Integrations

### Micro services

```go
svc := app.Service(micro.Config{Name: "inventory", Version: "1.0.0"}) // micro is github.com/nats-io/nats.go/micro
inv := svc.Group("inventory")

inv.Endpoint("get", nats.TypedReply(func(ctx context.Context, m *nats.Msg[GetStock]) (Stock, error) {
	return Stock{SKU: m.Data.SKU, Quantity: 10}, nil
})) // subject inventory.get

inv.Group("admin").Endpoint("reset", nats.TypedReply(reset), nats.EndpointMetadata(map[string]string{"owner": "ops"})) // inventory.admin.reset
```

`nats micro ls`, `info` and `stats` find the service once the app starts. An endpoint works like `Reply`: the returned value is encoded and sent back, `Terminal(err)` and validation failures answer with code 400, any other error with 500, and the service `STATS` count the errors. `Timeout`, `With`, `MaxAttempts`, `WithBackoff` and `DLQ` apply; `EndpointSubject` and `EndpointMetadata` are specific to endpoints. `cfg` is passed to `micro.AddService` as is, and `Health` reports one `endpoint` handler per endpoint.

- An endpoint handles one request at a time per instance. Scale with more instances: they share a queue group.
- `MaxAttempts`, `WithBackoff` and `Unlimited` stall the endpoint while a request is retried, and the whole retry time must fit the requester's timeout.
- On shutdown, requests already delivered are drained and answered. A handler that publishes through the `App` during the drain fails, because publishing is closed.
- A middleware that does not call `next` makes the endpoint send an empty response.

### Batch publishing

```go
acks, err := app.PublishJSBatch(ctx, []nats.BatchMsg{
	{Subject: "orders.created", Data: o1, Opts: []nats.PubOption{nats.MsgID("o1")}},
	{Subject: "orders.created", Data: o2},
})
var be *nats.PublishBatchError
if errors.As(err, &be) {
	for i, e := range be.Failed { /* message i failed */ }
}
```

`PublishJSBatch` sends the messages in order without waiting for each ack, then waits for all of them. It is **not atomic**: `acks[i]` is non-nil for every message known to be stored, and `PublishBatchError.Failed` lists the others by index. A `Failed` entry whose error is the bare context error means the outcome is unknown: the message may have been stored. Once `ctx` is cancelled or expires no further message is sent, and those entries read `nats: batch message N not sent: ...`. A message that cannot be encoded fails the whole call before anything is sent. Beyond about 4000 unacknowledged messages in flight a send stalls for up to 200ms and then fails with `jetstream.ErrTooManyStalledMsgs` (definitely not stored), so split bigger batches; an ack that never arrives fails with `jetstream.ErrAsyncPublishTimeout` after 30s. Storage order is not strictly guaranteed: jetstream resends a message after a no-responders reply, which can land it behind later ones.

For all-or-nothing publishing use `atomicbatch`:

```go
import "github.com/loipv/nats-go/nats/atomicbatch"

ack, err := atomicbatch.Publish(ctx, app, []nats.BatchMsg{
	{Subject: "orders.created", Data: order},
	{Subject: "orders.items", Data: items},
})
```

The server stores every message or none. It needs nats-server 2.12+ and a stream with `AllowAtomicPublish: true` that captures all the subjects. Messages go through `PublishMiddleware` before the batch is sent, so a middleware span ends before the actual send. If `ctx` expires during the commit the outcome is unknown. An `ExpectLastSeq` option only makes sense on the first message of the batch.

### OpenTelemetry

```go
import "github.com/loipv/nats-go/nats/natsotel"

otel.SetTextMapPropagator(propagation.TraceContext{}) // plus your tracer and meter providers
app.Use(natsotel.Middleware())
app.UsePublish(natsotel.PublishMiddleware())
```

The trace context travels in the message headers: a `send <subject>` producer span is the parent of the `process <subject>` consumer span, across services. Each attempt is its own span, with `messaging.nats.attempt`. The middleware records `messaging.client.operation.duration`, `messaging.client.sent.messages`, `messaging.process.duration` and `messaging.client.consumed.messages`. A message skipped by `Idempotent` is tagged `messaging.nats.duplicate` and is not counted as an error. The global providers and propagator are used unless you pass `WithTracerProvider`, `WithMeterProvider` or `WithPropagator`.

- **Cardinality**: the destination name is the message subject by default, and it is a metric attribute. Subjects that contain identifiers (`orders.42.created`) create one series each. Use `natsotel.WithDestination(func(subject string) string { ... })` to map them to something bounded such as `orders.*.created`.
- **Not covered**: `ConsumeBatch` handlers (they have no middleware) and responses sent by `Reply` workers or micro endpoints (they do not go through `PublishMiddleware`). The `send` span of a `PublishJSBatch` message ends when the message is handed to the client and does not see its ack result.

### CloudEvents

```go
import "github.com/loipv/nats-go/nats/cloudevents"

ce := cloudevents.New(app, "urn:example:shop")

ce.Handle("order.created", cloudevents.Typed(func(ctx context.Context, ev cloudevents.Event, m *nats.Msg[Order]) error {
	log.Printf("event %s from %s: order %s", ev.ID, ev.Source, m.Data.ID)
	return nil
}))

err := ce.Publish(ctx, "order.created", order, cloudevents.WithExtension("region", "eu"))
```

CloudEvents v1.0 in binary content mode: attributes travel in `ce-*` headers and the payload is the body. An event of type `T` is published on `events.T` (change it with `cloudevents.SubjectFunc`). `ce.PublishJS` sends to a stream and uses the event id as `Nats-Msg-Id`, so a repeated id is dropped within the stream's `Duplicates` window. Extra headers go through `cloudevents.WithHeader`; `WithPubOptions` is for core options that are not headers, and must not carry `nats.WithHeader`, which would replace the CloudEvents headers.

- `Handle` is one-way and never replies; use `app.Reply` for request/reply.
- An event with invalid attributes (missing `ce-id`, `ce-source`, `ce-type` or `ce-specversion`, a version other than `1.0`, a bad `ce-time`) fails with `nats.Terminal` and the handler is not called; with the `DLQ` option it is dead-lettered.
- For a JetStream durable consumer, use `app.Consume` with `cloudevents.FromHeader`.
- Structured and batched modes are not supported.

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
| M3 Integrations | available | NATS micro services, OpenTelemetry (subpackage `natsotel`), CloudEvents (`cloudevents`), batch and atomic batch publish (`atomicbatch`) |

The API may change until v1.0.0.

## License

[MIT](LICENSE)
