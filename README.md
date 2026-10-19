# nats-go

[![Go Reference](https://pkg.go.dev/badge/github.com/loipv/nats-go/nats.svg)](https://pkg.go.dev/github.com/loipv/nats-go/nats)
[![CI](https://github.com/loipv/nats-go/actions/workflows/ci.yml/badge.svg)](https://github.com/loipv/nats-go/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/loipv/nats-go)](https://goreportcard.com/report/github.com/loipv/nats-go)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A Go framework for building [NATS](https://nats.io) and JetStream services on top of the official [nats.go](https://github.com/nats-io/nats.go) client.

- **Typed handlers** with generics: `app.Subscribe("orders.*", nats.Typed(func(ctx context.Context, m *nats.Msg[Order]) error { ... }))`
- **Core NATS**: wildcards, queue groups, request/reply with error propagation
- **JetStream**: pull, push and ordered consumers, automatic or manual ack, batch handlers
- **Publishing**: fire-and-forget, typed request/reply, JetStream with ack and deduplication, batch and atomic batch
- **Reliability**: retry with backoff, dead-letter queue, idempotency (in-memory or KV), automatic restart of consumers, panic recovery
- **Streams and consumers declared in code**, created, updated or only verified on startup
- **Middleware** on the inbound and outbound paths, lifecycle hooks and event callbacks for your own extensions
- **Integrations**: NATS micro services, OpenTelemetry tracing and metrics, CloudEvents
- **Fail-fast startup**, **graceful shutdown** with a deadline, **health** endpoint
- Every auth method and TLS supported by nats.go

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Examples](#examples)
- [Handlers](#handlers)
- [Publishing](#publishing)
- [Middleware and extensions](#middleware-and-extensions)
- [Streams and consumers](#streams-and-consumers)
- [Reliability](#reliability)
- [Integrations](#integrations)
- [Lifecycle](#lifecycle)
- [Authentication and TLS](#authentication-and-tls)
- [Development](#development)

## Install

```sh
go get github.com/loipv/nats-go
```

| Requirement | Version |
|---|---|
| Go | 1.26+ |
| nats-server | 2.10+ |
| `KVStore` (per-key TTL) | nats-server 2.11+ |
| `PublishJSAtomicBatch` | nats-server 2.12+ and a stream with `AllowAtomicPublish` |

The core `nats` package depends only on nats.go and orbit.go's small `jetstreamext` (for atomic batches). OpenTelemetry (`natsotel`) and CloudEvents (`cloudevents`) live in subpackages, so you only pull in their dependencies when you import them.

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
		_, err := app.PublishJS(ctx, "orders.created", m.Data, nats.WithMsgID(m.Data.ID))
		return err
	}), nats.WithQueue("workers"))

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
| [request-reply](examples/request-reply) | `SubscribeReply`, `Request` decoding into a value, `ServiceError` |
| [jetstream](examples/jetstream) | Declared stream and consumers, `PublishJS` with `WithMsgID`, automatic and manual ack |
| [ordered](examples/ordered) | `ConsumeOrdered` with subject filters |
| [middleware](examples/middleware) | `Use`, `WithMiddleware`, `Recover`, `Logger`, `UsePublish` |
| [service](examples/service) | `Run` with signals, validation, health endpoint, credentials |
| [retry-dlq](examples/retry-dlq) | `WithMaxAttempts`, `WithBackoff`, `WithDLQ` on a JetStream consumer and a core subscription, `Nats-DLQ-*` headers |
| [idempotency](examples/idempotency) | `WithIdempotency` with a `KVStore` and a custom key: two consumers of one stream each run once for three deliveries |
| [batch](examples/batch) | `ConsumeBatch`, `BatchError`, retry and DLQ per message |
| [micro-service](examples/micro-service) | `Service`, `Group` and `Endpoint`; found by `nats micro info inventory` |
| [otel](examples/otel) | `natsotel` middleware: producer and consumer spans in one trace, printed to stdout |
| [cloudevents](examples/cloudevents) | `cloudevents.New`, `Handle` and `Publish` with an extension |
| [batch-publish](examples/batch-publish) | `PublishJSBatch`, and `PublishJSAtomicBatch` rolling back a batch with a foreign subject |
| [hooks-events](examples/hooks-events) | `Hook`, `WithOnRetry`, `WithOnDeadLettered`, `WithOnDisconnected` |

## Handlers

Handlers are registered with `App` methods before `Start` and wrapped in an adapter that carries the payload type, because Go methods cannot have type parameters. The type is inferred from the handler, and passing a handler of the wrong kind does not compile.

| Method | Receives | Adapter |
|---|---|---|
| `Subscribe(subject, h, opts...)` | core NATS messages, with wildcards and queue groups | `nats.Typed` |
| `SubscribeReply(subject, h, opts...)` | requests; the returned value is the response | `nats.TypedReply` |
| `Consume(stream, consumer, h, opts...)` | a durable JetStream consumer, pull or push (detected at `Start`) | `nats.Typed` |
| `ConsumeOrdered(stream, h, opts...)` | a stream in order, through an ephemeral ordered consumer | `nats.Typed` |
| `ConsumeBatch(stream, consumer, h, opts...)` | slices of messages from a pull consumer | `nats.TypedBatch` |
| `Service(cfg).Endpoint(name, h, opts...)` | NATS micro endpoint requests | `nats.TypedReply` |

Payloads are decoded with the App's `Codec` (`JSONCodec` by default, `nats.WithCodec` to change it); `[]byte` bypasses the codec in both directions. A payload type with a `Validate() error` method is validated after decoding. `m.Data` is the payload, `m.Header` the headers, `m.Attempt()` the delivery count.

`SubscribeReply` covers the usual one request, one response. For anything else, such as answering only some messages of a subject, a `Subscribe` handler can answer by hand with `m.Respond(v)` or `m.RespondError(err)`; the requester sees the same response and `*ServiceError` as from `SubscribeReply`. Both return `nats.ErrNotCore` on a JetStream message (its reply subject is the ack subject) and an error inside `SubscribeReply` or an endpoint, whose return value is the response.

### Handler options

An option used on a handler kind it does not apply to makes `Start` fail.

| Option | Applies to | Effect |
|---|---|---|
| `WithQueue(group)` | `Subscribe`, `SubscribeReply` | join a queue group |
| `WithConcurrency(n)` | all but `ConsumeBatch` and endpoints | up to `n` calls in parallel (default 1, which keeps order; `ConsumeOrdered` accepts only 1) |
| `WithTimeout(d)` | all | deadline on the handler's context |
| `WithMiddleware(mw...)` | all but `ConsumeBatch` | middleware for this handler only |
| `WithManualAck()` | `Consume`, `ConsumeBatch` | the handler settles messages with `m.Ack()`, `m.Nak()`, `m.NakWithDelay(d)`, `m.Term()`, `m.InProgress()` |
| `WithMaxAttempts(n)`, `WithBackoff(b)`, `WithDLQ(subject)` | all | [retry and dead-lettering](#retry-and-dlq) |
| `WithAutoInProgress()` | `Consume`, `ConsumeBatch` | keep the ack wait from expiring while the handler runs |
| `WithPullMaxMessages(n)` | `Consume` on a pull consumer, `ConsumeOrdered` | messages buffered by the client |
| `WithFilterSubjects(s...)`, `WithDeliverPolicy(p)` | `ConsumeOrdered` | subjects and start position |
| `WithBatchSize(n)`, `WithBatchWait(d)` | `ConsumeBatch` | batch size (default 100) and fill wait (default 1s) |
| `WithEndpointSubject(s)`, `WithEndpointMetadata(m)` | endpoints | endpoint subject and metadata |

### Errors

| Handler returns | JetStream message | Retry | DLQ | Reply |
|---|---|---|---|---|
| `nil` | `Ack` | - | - | encoded response |
| any other error | `NakWithDelay` by the backoff, until `WithMaxAttempts` is used up; then `Term` | yes, unless `WithMaxAttempts` is 1 | after the last attempt, if `WithDLQ(subject)` is set | `Nats-Service-Error`, code 500 |
| `nats.Terminal(err)`, decode or validation failure | `Term` (never redelivered) | no | immediately, if `WithDLQ(subject)` is set | `Nats-Service-Error`, code 400 |
| panic | handled as any other error | as above | as above | `Nats-Service-Error: internal error`, code 500; the panic and stack are only logged |

Every `Term` carries the first line of the error as its reason, reported by the server in the `$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED` advisory. Retry runs whether or not `WithDLQ` is set; `WithDLQ(subject)` only decides where a message goes once its attempts are used up (the DLQ, or, without it, a log line and `Term`). `Consume` and `ConsumeBatch` retry forever by default; `Subscribe`, `SubscribeReply` and `ConsumeOrdered` do not retry unless `WithMaxAttempts(n)` is set.

## Publishing

```go
err := app.Publish(ctx, "orders.created", order)                          // fire-and-forget
var inv Invoice
_, err = app.Request(ctx, "billing.get", GetInvoice{ID: "42"}, &inv)       // waits for the reply and decodes it
ack, err := app.PublishJS(ctx, "orders.created", order, nats.WithMsgID(order.ID)) // waits for the stream's ack
```

A responder that failed comes back from `Request` as `*nats.ServiceError` with its code. Publishing works from `Start` until every handler has stopped during `Shutdown`, and returns `nats.ErrNotRunning` outside that window.

| Option | Effect |
|---|---|
| `WithHeader(h)` | headers of the message |
| `WithMsgID(id)` | `Nats-Msg-Id`: the stream drops an ID it has seen within its `Duplicates` window; on `Publish` and `Request` it only sets the header, the default key of `WithIdempotency` |
| `WithExpectStream(name)`, `WithExpectLastSeq(seq)`, `WithExpectLastSubjectSeq(seq)` | optimistic concurrency checks done by the server |
| `WithJSPublishOpts(opts...)` | any other `jetstream.PublishOpt`, such as `jetstream.WithMsgTTL`; `PublishJS` only |

The JetStream options other than `WithMsgID` are rejected by `Publish` and `Request`.

### Batch publishing

```go
acks, err := app.PublishJSBatch(ctx, []nats.BatchMsg{
	{Subject: "orders.created", Data: o1, Opts: []nats.PubOption{nats.WithMsgID("o1")}},
	{Subject: "orders.created", Data: o2},
})
var be *nats.PublishBatchError
if errors.As(err, &be) {
	for i, e := range be.Failed { /* message i failed */ }
}
```

`PublishJSBatch` sends the messages in order without waiting for each ack, then waits for all of them. It is **not atomic**: `acks[i]` is non-nil for every message known to be stored, and `PublishBatchError.Failed` lists the others by index. A `Failed` entry whose error is the bare context error means the outcome is unknown: the message may have been stored. Once `ctx` is cancelled or expires no further message is started, and those entries read `nats: batch message N not sent: ...`. Other errors read `nats: batch message N: ...`. A publish middleware that calls `next` and then returns an error marks that message failed with its error, although it was sent. A message that cannot be encoded fails the whole call before anything is sent. Beyond about 4000 unacknowledged messages in flight a send stalls for up to 200ms and then fails with `jetstream.ErrTooManyStalledMsgs` (definitely not stored), so split bigger batches; an ack that never arrives fails with `jetstream.ErrAsyncPublishTimeout` after 30s. Storage order is not strictly guaranteed: jetstream resends a message after a no-responders reply, which can land it behind later ones.

For all-or-nothing publishing use `PublishJSAtomicBatch`:

```go
ack, err := app.PublishJSAtomicBatch(ctx, []nats.BatchMsg{
	{Subject: "orders.created", Data: order},
	{Subject: "orders.items", Data: items},
})
```

The server stores every message or none. It needs nats-server 2.12+ and a stream with `AllowAtomicPublish: true` that captures all the subjects. Messages go through `PublishMiddleware` before the batch is sent, so a middleware span ends before the actual send. If `ctx` expires during the commit the outcome is unknown. A `WithExpectLastSeq` option only makes sense on the first message of the batch.

Both batch calls reject `nats.WithJSPublishOpts`, which passes any other `jetstream.PublishOpt` (such as `jetstream.WithMsgTTL`) to `PublishJS` only: those options are not headers, so they cannot travel with a built message.

## Middleware and extensions

A middleware wraps a handler; a publish middleware wraps every outgoing message:

```go
// package nats
type Handler func(ctx context.Context, m *RawMsg) error
type Middleware func(Handler) Handler

type PublishFunc func(ctx context.Context, m *RawMsg) error
type PublishMiddleware func(PublishFunc) PublishFunc
```

| Register with | Applies to |
|---|---|
| `app.Use(mw...)` | every handler |
| `nats.WithMiddleware(mw...)` | one handler, inside the app middleware |
| `app.UsePublish(mw...)` | every `Publish`, `Request`, `PublishJS` and each message of a batch |

The first middleware registered is the outermost: it runs first on the way in and last on the way out. Built in: `nats.Recover()` and `nats.Logger(l)`; `natsotel` and `cloudevents` are built on the same API.

```go
func RequireTenant() nats.Middleware {
	return func(next nats.Handler) nats.Handler {
		return func(ctx context.Context, m *nats.RawMsg) error {
			t := m.Header.Get("X-Tenant")
			if t == "" {
				return nats.Terminal(errors.New("missing tenant")) // never retried
			}
			return next(context.WithValue(ctx, tenantKey{}, t), m)
		}
	}
}

app.Use(nats.Recover(), RequireTenant())
```

An error returned by a middleware is handled like a handler error. Middleware does not apply to `ConsumeBatch`, and `Use`, `UsePublish` and `Hook` called after `Start` are ignored and logged.

A reusable extension is a function that takes the `*nats.App` and registers what it needs: middleware, a [hook](#hooks) for resources tied to the App's lifetime, and [event callbacks](#events) passed to `New`. These interfaces can also be replaced:

| Extension point | Default | Replace with |
|---|---|---|
| `Codec` (`Marshal`, `Unmarshal`) via `WithCodec` | `JSONCodec` | protobuf, msgpack, ... |
| `IdempotencyStore` (`Begin`, `Complete`, `Abort`) | `MemoryStore`, `KVStore` | Redis, a database, ... |
| `*slog.Logger` via `WithLogger` | `slog.Default()` | any `slog.Handler` |
| `app.Conn()`, `app.JetStream()` | | direct nats.go access |

## Streams and consumers

`app.Stream` and `app.Consumer` are synced with the server at `Start`, according to a `ProvisionMode`:

| Mode | Missing | Exists, differs |
|---|---|---|
| `nats.Upsert` (default) | created | updated; a change the server rejects fails `Start` |
| `nats.CreateOnly` | created | left as is, logged at Warn (`nats: config drift`) |
| `nats.Verify` | `Start` fails | `Start` fails with `nats.ErrConfigDrift` |

Set the default with `nats.WithProvision` and override it per declaration. When several services declare one stream, let its owner `Upsert` and the others `Verify`, so they cannot overwrite each other:

```go
app, _ := nats.New(nats.WithProvision(nats.Verify)) // other services' streams are only checked
// This service owns ORDERS.
app.Stream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}}, nats.Upsert)
// PAYMENTS belongs to another service: Start fails if it is missing or differs.
app.Stream(jetstream.StreamConfig{Name: "PAYMENTS", Subjects: []string{"payments.>"}})
```

`CreateOnly` and `Verify` compare only the fields set in the declaration, since the server fills defaults into the rest; `Metadata` only needs the declared keys. So a declaration cannot require a field to be zero, including the zero-valued defaults `FileStorage`, `LimitsPolicy`, `DiscardOld`, `AckExplicitPolicy` and `DeliverAllPolicy`. Fields the server overrides are skipped (a consumer's `AckWait` when `BackOff` is set, `StreamSource.Domain`). Nothing is ever deleted, and the `KVStore` bucket is always created or updated.

## Reliability

### Retry and DLQ

```go
app.Stream(jetstream.StreamConfig{Name: "DLQ", Subjects: []string{"dlq.>"}}) // you declare the DLQ stream

app.Consume("PAYMENTS", "charger", nats.Typed(charge),
	nats.WithMaxAttempts(5),
	nats.WithBackoff(nats.ExponentialBackoff(time.Second, time.Minute)), // the default
	nats.WithDLQ("dlq.payments"),
)
```

- With `Consume` and `ConsumeBatch` the server redelivers: a failed message is naked with a delay, and `m.Attempt()` is the delivery count. `MaxDeliver` on the consumer must be `-1` or greater than `WithMaxAttempts`, or `Start` fails.
- With `Subscribe`, `SubscribeReply` and `ConsumeOrdered` there is no redelivery, so `WithMaxAttempts(n)` makes the library retry in the worker, waiting for the backoff between attempts. `Unlimited` can stall the subscription while failures last, a `SubscribeReply` must finish its retries within the requester's timeout, and `ConsumeOrdered` stalls the whole stream while it retries (which keeps the order).
- `WithDLQ(subject)` publishes the original payload and headers plus `Nats-DLQ-Error`, `Nats-DLQ-Subject`, `Nats-DLQ-Deliveries` and, for JetStream consumers, `Nats-DLQ-Stream`, `Nats-DLQ-Consumer`, `Nats-DLQ-Sequence` and `Nats-DLQ-Msg-Id`. The subject must be a valid subject without a `*` or `>` token. `Start` fails if no stream captures the subject, if it is the handler's source stream, or if a `Subscribe` or `SubscribeReply` handler's own subject matches it.
- `WithMaxAttempts`, `WithBackoff` and `WithDLQ` cannot be combined with `WithManualAck`.
- `WithAutoInProgress()` extends the ack wait of a `Consume` or `ConsumeBatch` message while the handler runs; use it with `WithTimeout(d)`.

### Idempotency

```go
store := nats.NewKVStore(app, "IDEMPOTENCY") // or nats.NewMemoryStore() for a single instance
app.Idempotency(store, // App-wide settings; enables nothing on its own
	nats.WithIdempotencyKey(func(m *nats.RawMsg) string { return m.Header.Get("X-Order-Id") }), // default: Nats-Msg-Id
	nats.WithClaimTTL(time.Minute),
	nats.WithDoneTTL(24*time.Hour),
)

app.Consume("ORDERS", "billing", billing, nats.WithIdempotency())   // scope "ORDERS/billing"
app.Consume("ORDERS", "shipping", shipping, nats.WithIdempotency()) // scope "ORDERS/shipping"
app.Subscribe("audit.>", audit, nats.WithIdempotency(
	nats.WithIdempotencyStore(nats.NewMemoryStore()), // overrides the App's store only
), nats.WithIdempotencyScope("audit-svc"))
```

`WithIdempotency` turns it on for one handler. Its options override `app.Idempotency` field by field, which overrides the defaults (key `Nats-Msg-Id`, claim TTL 1 minute, done TTL 24 hours). `app.Idempotency` can be called before or after the handlers are registered.

Keys are scoped per handler: handlers that receive the same message each process it once, and the replicas of one handler share their keys.

| Handler | Default scope |
|---|---|
| `Consume` | `STREAM/consumer` |
| `Endpoint` | `service.group.endpoint` |
| `Subscribe`, `SubscribeReply` with `WithQueue(q)` | `subject@q` |
| `Subscribe`, `SubscribeReply` without a queue, `ConsumeOrdered` | none: `WithIdempotencyScope` is required |

A key is claimed before the handler runs and remembered once it succeeds; duplicates are acked without calling the handler, and a duplicate request gets a 400; a duplicate is never retried or sent to the DLQ. Idempotency runs inside every middleware, so a middleware sees a duplicate as `ErrDuplicate`. A failing or panicking handler releases the claim so the retry can run. `KVStore` is correct across instances, needs nats-server 2.11+ and rounds every TTL up to a whole second. `MemoryStore` is only correct for a single instance.

`Start` fails when a handler with `WithIdempotency` has no scope or no store, when `WithIdempotencyScope` is used without `WithIdempotency`, on `ConsumeBatch`, and on a nil store, nil key function or non-positive TTL.

Limits:

- It is not exactly-once. A retry runs the side effects of the failed attempt again; a key whose `Complete` fails is only logged and reported, so a later duplicate is admitted; a duplicate arriving after `WithDoneTTL` is processed again.
- `WithClaimTTL` must exceed the worst-case handler duration, because claims are released by key without an ownership token.
- A copy whose twin is still being handled fails with `ErrInProgress` and is retried. On JetStream each of those retries counts toward `WithMaxAttempts`, so with a limit and a DLQ the copy can be dead-lettered even though its twin succeeds.
- A duplicate request gets a 400, not the original response.
- `ConsumeBatch` has no idempotency.
- `WithMsgID` on `Publish` or `Request` only sets the header; there is no server-side deduplication outside JetStream.

### Batch handlers

```go
app.ConsumeBatch("ORDERS", "loader", nats.TypedBatch(func(ctx context.Context, msgs []*nats.Msg[Order]) error {
	failed := map[int]error{}
	// ... record the index of every message that failed
	if len(failed) > 0 {
		return &nats.BatchError{Failed: failed} // only these are retried; the rest are acked
	}
	return nil // ack all
}), nats.WithBatchSize(100), nats.WithBatchWait(time.Second))
```

Any other error fails the whole batch. A non-nil `*BatchError` is found even inside `errors.Join` or a wrapper, and then fails only its listed indexes while the rest are acked. Only a typed-nil `*BatchError` returned directly counts as no error. `ConsumeBatch` needs a pull consumer, and middleware does not apply to it. With `nats.WithManualAck()` the handler settles each message itself (`m.Ack()` and friends) and a returned error is only logged; messages that fail to decode are still settled by the library.

### Supervisor

`Consume` and `ConsumeBatch` handlers restart when their consumer or stream is deleted: the handler reports `restarting` (and `Health` reports `degraded`), recreates the stream and consumer if they were declared with `app.Stream` and `app.Consumer` (except under `Verify`, where it waits for their owner), and resumes, waiting `WithRestartBackoff` (default `ExponentialBackoff(1s, 30s)`) between attempts. `HandlerStatus.Restarts` counts restarts. Set `IdleHeartbeat` on push consumers: without it a deleted push consumer cannot be detected.

### Panics

A panic in a handler is recovered by the worker, logged with its stack and handled like any other error. The requester of a `SubscribeReply` handler or micro endpoint that panicked gets `Nats-Service-Error: internal error` with code 500, never the panic text. `nats.Recover()` is still useful to make middleware around the handler see the panic as an error.

## Integrations

### Micro services

```go
svc := app.Service(micro.Config{Name: "inventory", Version: "1.0.0"}) // micro is github.com/nats-io/nats.go/micro
inv := svc.Group("inventory")

inv.Endpoint("get", nats.TypedReply(func(ctx context.Context, m *nats.Msg[GetStock]) (Stock, error) {
	return Stock{SKU: m.Data.SKU, Quantity: 10}, nil
})) // subject inventory.get

inv.Group("admin").Endpoint("reset", nats.TypedReply(reset), nats.WithEndpointMetadata(map[string]string{"owner": "ops"})) // inventory.admin.reset
```

`nats micro ls`, `info` and `stats` find the service once the app starts. An endpoint works like `SubscribeReply`: the returned value is encoded and sent back, `Terminal(err)` and validation failures answer with code 400, any other error with 500, and the service `STATS` count the errors. `WithTimeout`, `WithMiddleware`, `WithMaxAttempts`, `WithBackoff` and `WithDLQ` apply; `WithEndpointSubject` and `WithEndpointMetadata` are specific to endpoints. `cfg` is passed to `micro.AddService` as is, and `Health` reports one `endpoint` handler per endpoint, named after the service, the group prefixes and the endpoint joined with `.` (`orders.v2.get`). A request without a reply subject is skipped with a warning.

- An endpoint handles one request at a time per instance. Scale with more instances: they share a queue group.
- `WithMaxAttempts`, `WithBackoff` and `Unlimited` stall the endpoint while a request is retried, and the whole retry time must fit the requester's timeout.
- On shutdown, every request delivered to an endpoint is still answered. The one being handled when `Shutdown` begins can publish through the `App`; requests queued behind it, or delivered while micro drains its subscriptions, cannot, because publishing closes once no request is in flight.
- A middleware that does not call `next` makes the endpoint send an empty response.

### OpenTelemetry

```go
import "github.com/loipv/nats-go/nats/natsotel"

otel.SetTextMapPropagator(propagation.TraceContext{}) // plus your tracer and meter providers
app.Use(natsotel.Middleware())
app.UsePublish(natsotel.PublishMiddleware())
```

The trace context travels in the message headers: a `send <subject>` producer span is the parent of the `process <subject>` consumer span, across services. Each attempt is its own span, with `messaging.nats.attempt`. The middleware records `messaging.client.operation.duration`, `messaging.client.sent.messages`, `messaging.process.duration` and `messaging.client.consumed.messages`. A duplicate skipped by `WithIdempotency` is tagged `messaging.nats.duplicate` and is not counted as an error. The global providers and propagator are used unless you pass `WithTracerProvider`, `WithMeterProvider` or `WithPropagator`.

- **Cardinality**: the destination name is the message subject by default, and it is a metric attribute. Subjects that contain identifiers (`orders.42.created`) create one series each. Use `natsotel.WithDestination(func(subject string) string { ... })` to map them to something bounded such as `orders.*.created`.
- **Not covered**: `ConsumeBatch` handlers (they have no middleware) and responses sent by `SubscribeReply` workers or micro endpoints (they do not go through `PublishMiddleware`). The `send` span of a `PublishJSBatch` message ends when the message is handed to the client and does not see its ack result.

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

CloudEvents v1.0 in binary content mode: attributes travel in `ce-*` headers and the payload is the body. An event of type `T` is published on `events.T` (change it with `cloudevents.WithSubjectFunc`). `ce.PublishJS` sends to a stream and uses the event id as `Nats-Msg-Id`, so a repeated id is dropped within the stream's `Duplicates` window. Extra headers go through `cloudevents.WithHeader`; `WithPubOptions` is for core options that are not headers, and must not carry `nats.WithHeader`, which would replace the CloudEvents headers.

- `Handle` is one-way and never replies; use `app.SubscribeReply` for request/reply.
- An event with invalid attributes (missing `ce-id`, `ce-source`, `ce-type` or `ce-specversion`, a version other than `1.0`, a bad `ce-time`) fails with `nats.Terminal` and the handler is not called; with the `WithDLQ` option it is dead-lettered.
- For a JetStream durable consumer, use `app.Consume` with `cloudevents.FromHeader`.
- Structured and batched modes are not supported.

## Lifecycle

- `Start(ctx)` connects, syncs declared streams and consumers according to their [provision mode](#streams-and-consumers) (never deletes them), checks that consumed consumers exist and starts every handler. The first failure stops everything and is returned, and `Start` can then be called again. Registration mistakes (including `WithPullMaxMessages(n)` with `n < 1`, a negative `WithTimeout` and an invalid `WithDLQ` subject) are returned here too. `Start` does not hold a lock while it connects, so `Health()` answers meanwhile with `ready` false, and `Conn()` and `JetStream()` return nil unless the app is started or starting.
- `Shutdown(ctx)` stops intake, waits for in-flight handlers and drains the connection within the context deadline. Unacked JetStream messages are redelivered by the server. A second `Shutdown` that starts while the first is running waits for it and returns its error; a `Shutdown` during `Start` waits for `Start`, or cancels it when its `ctx` ends first.
- `Run(ctx)` is `Start`, wait for `ctx` or for the connection to be closed for good, then `Shutdown` bounded by `WithShutdownTimeout` (30s by default). If that `Shutdown` fails, `Run` returns its error; otherwise, when the connection closed for good, `Run` returns an error wrapping nats.go's `ErrConnectionClosed`.
- The connection reconnects forever by default (`nats.WithNATSOptions(natsio.MaxReconnects(n))` to change it); while reconnecting, health reports `degraded`.
- `Health()` returns a `Report`: `Status` (`ok`, `degraded` while reconnecting or while a handler is restarting or has stopped, `down` before `Start`, after `Shutdown` or once the connection is closed), the connection state, `Ready`, and one `HandlerStatus` per handler. `HealthHandler()` serves it as JSON with 200 when `ok` and 503 otherwise; mount it on your own HTTP server.

### Hooks

```go
app.Hook(nats.Hook{
	OnStart: func(ctx context.Context) error { return warmCache(ctx) },
	OnStop:  func(ctx context.Context) error { return flushMetrics(ctx) },
})
```

Hooks start in registration order, after publishing opens and before any handler starts, and stop in reverse order after the handlers have stopped (or `Shutdown`'s deadline passed). If a hook or a handler fails to start, the hooks already started are stopped and `Start` returns the error, with any `OnStop` errors joined into it (each wrapped `nats: stop hook i: <err>`). `OnStop` receives the `Shutdown` context, even when it has expired, and its errors are joined into the `Shutdown` error. Panics in hooks are not recovered.

### Events

The library logs what happens to messages, handlers and the connection. The `WithOn*` options also hand those moments to your code, for metrics or alerts:

| Group | Option | Runs when |
|---|---|---|
| Message | `WithOnRetry` | a failed message will be tried again: nakked for redelivery, or about to be retried in process |
| | `WithOnFailed` | the library gives up on a message without a DLQ: terminated, a core handler out of attempts (every failed core handler with the default single attempt), or a response that cannot be encoded |
| | `WithOnDeadLettered` | a failed message was published to its DLQ |
| | `WithOnDeadLetterFailed` | publishing to the DLQ failed; the message is nakked (JetStream) or dropped (core) |
| Handler | `WithOnHandlerRestarting` | the consumer of a `Consume` or `ConsumeBatch` handler is gone and the supervisor restarts it |
| | `WithOnHandlerRestarted` | the restarted handler runs again |
| | `WithOnHandlerStopped` | a handler of any kind stopped, on `Shutdown` or a failed `Start` |
| Connection | `WithOnDisconnected` | the connection was lost, or closed by `Shutdown` |
| | `WithOnReconnected` | the connection is back |
| | `WithOnClosed` | the connection is closed for good, on `Shutdown` or a failed `Start` after the connection was made |
| Errors | `WithOnAsyncError` | an error the library could only log: a failed ack, nak, term, in-progress or response, a consume, fetch or restart failure, an idempotency claim that could not be completed or aborted, or a nats.go connection error. `Err` is prefixed with the operation (`ack: ...`) and still works with `errors.Is`. A failing `WithAutoInProgress` emits one event per failed tick |

```go
app, err := nats.New(
	nats.WithOnRetry(func(e nats.MsgEvent) { retries.Inc() }),
	nats.WithOnDeadLettered(func(e nats.MsgEvent) { log.Printf("dead letter %s: %v", e.Subject, e.Err) }),
	nats.WithOnDisconnected(func(err error) { log.Printf("disconnected: %v", err) }),
)
```

- Each option may be passed several times: callbacks of the same kind run in the order given. A nil function makes `New` return an error.
- Callbacks run synchronously on the goroutine where the event happens (a handler worker, the supervisor, a nats.go callback), so they must return quickly and must not block. Callbacks may run concurrently and must be safe for concurrent use. A panic in a callback is recovered and logged, and the next callback still runs. The library logs first, then runs the callbacks.
- In-process retries (`Subscribe`, `SubscribeReply`, `ConsumeOrdered`, endpoints) fire `OnRetry` before the backoff wait, so a retry cut short by `Shutdown` has already been reported.
- Handlers with `WithManualAck` emit no message event, because the handler decides the message's fate. The exception is a `ConsumeBatch` message that fails to decode: the library terms it, so `OnFailed` fires.
- `OnDisconnected` and `OnClosed` also fire when `Shutdown` closes the connection. `Shutdown` returns after `OnClosed` ran when it drains within its deadline. On a deadline, and after a failed `Start`, the connection events may arrive after the call returns.
- Callbacks you pass in `WithNATSOptions` (`DisconnectErrHandler`, `ReconnectHandler`, `ClosedHandler`, `ErrorHandler`) are chained, not replaced: yours runs first, then the library's. Without your own `ErrorHandler`, nats.go's async errors are logged through the App's logger.

## Authentication and TLS

```go
nats.New(
	nats.WithURL("tls://nats.example.com:4222"),
	nats.WithUserCredentials("service.creds"), // or WithUserInfo, WithToken, WithNKeyFromSeed, WithUserJWTAndSeed
	nats.WithRootCAs("ca.pem"),
	nats.WithClientCert("client.pem", "client-key.pem"),
)
```

Any other nats.go option can be passed with `nats.WithNATSOptions`. The package is named `nats` like nats.go, so import nats.go under an alias when you need it:

```go
import natsio "github.com/nats-io/nats.go"

nats.WithNATSOptions(natsio.MaxReconnects(-1))
```

## Development

The tests start an embedded nats-server, so no server is needed:

```sh
go test -race ./...                       # all tests
go test -race -run TestProvisionVerify ./nats/  # one test
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@latest ./...
```

CI runs the same commands on Go 1.26 and the latest stable release. The examples need a server: `nats-server -js`, then `go run ./examples/<name>`.

## Status

All features above are available. The API may change until v1.0.0.

## License

[MIT](LICENSE)
