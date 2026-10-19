# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Tests start an embedded nats-server (`nats-server/v2/test`), so none needs to be running.

```sh
go test -race ./...                              # everything (what CI runs)
go test -race -run TestProvisionVerify ./nats/   # one test
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@latest ./...
golangci-lint run ./...                          # kept at 0 issues locally; not in CI
gofmt -l .
```

Examples need a real server: `nats-server -js`, then `go run ./examples/<name>` (`NATS_URL` overrides the URL).

## Layout

- `nats/`: the core package, named `nats` like nats.go. Inside it, import nats.go as `natsio`.
- `nats/natsotel`, `nats/cloudevents`: built **only on the public API** (middleware, `Subscribe`, `Publish`). Keep them that way; they double as proof that third-party extensions work. Their tests are external (`package x_test`) and share helpers in `nats/internal/testutil`.
- `examples/<name>/main.go`: one runnable program per feature. Each is listed in the README's Examples table.
- `nats/doc.go` is the godoc overview and mirrors the README. A behaviour change usually needs both updated.
- `docs/superpowers/` and `.superpowers/` are git-ignored working notes (specs, plans, review ledgers).

## Architecture

**App lifecycle** (`app.go`). Every registration (`Subscribe`, `Consume`, `Stream`, `Use`, `Hook`, …) only appends to the App under `a.mu`. A registration made after Start is ignored and logged (`lateLocked`). A registration mistake is collected (`addErr`) and returned by `Start`.

`Start` runs these steps in order, and the first failure rolls back the ones already done (including `OnStop` of started hooks, in reverse):

1. connect
2. `provision` (streams, then consumers)
3. resolve each consumer (`jsReg.prepare`: pull or push detection, MaxDeliver vs `WithMaxAttempts` check, DLQ stream check)
4. open publishing
5. run `OnStart` hooks in order
6. start the runners

`Shutdown` reverses this:

1. stop intake and wait for in-flight handlers
2. run `OnStop` hooks in reverse
3. close publishing (`pubOpen`)
4. drain the connection

**Connection state** lives in `App.conn atomic.Pointer[connState]` (`nc`, `js`, `base` ctx), read through `a.nc()`, `a.js()` and the `conn.Load()` accessors. The publish and handler paths must never take `a.mu`: a logger that publishes through the App while `a.mu` is held deadlocks. `TestLoggerPublishingThroughApp` guards this.

**Handler pipeline.** A `Typed`, `TypedReply` or `TypedBatch` adapter decodes the payload with the `Codec` and calls `Validate()` (`msg.go`). Around it sits the middleware chain: app `Use` first, then per-handler `WithMiddleware`, wrapped with `slices.Backward`, so the first registered is outermost. Below that:

- `handler.go`: a worker pool runs the handler.
- `retry.go` (`dispose`): turns the result into ack, nak with backoff, term, or DLQ (`dlq.go`).
- `nats.Terminal(err)`: makes an error non-retryable.

Core subscriptions retry in-process; JetStream consumers let the server redeliver. `ConsumeBatch` (`consume_batch.go`) bypasses middleware.

**Handler options** (`handler_options.go`) are validated in `newHandlerConfig`: the `allowedKinds` table says which handler kinds accept each option. A new option must be added there and must record its name in `c.used`.

**Supervisor** (`supervisor.go`): a `Consume` or `ConsumeBatch` runner restarts when its consumer or stream is deleted. `recoverConsumer` recreates declarations through the same `syncStream`/`syncConsumer` used at Start, so it respects the provision mode: `Verify` declarations are never recreated.

**Provisioning** (`provision.go`): `ProvisionMode` `Upsert` (default), `CreateOnly` or `Verify`. Set the App default with `WithProvision`, or override it per declaration through the variadic argument of `Stream`/`Consumer`. `drift()` is a reflect-based comparison of only the non-zero declared fields:

- maps are subset checks
- `json:"-"` fields and empty pointers are skipped
- `AckWait` is ignored when `BackOff` is set

When the server normalises a field, extend this comparison instead of special-casing a caller.

**Events and logging** (`events.go`): a `sink` bundles the logger and the `WithOn*` callbacks. Call sites log first, then call the sink, whose `fire` runs the callbacks synchronously and recovers panics. Async errors carry an operation prefix (`ack: `, `consume: `, …). nats.go callbacks passed via `WithNATSOptions` are chained (user first), and `NoCallbacksAfterClientClose` is forced off.

**Publishing** (`publish.go`, `publish_batch.go`): `preparePub` encodes and applies `PubOption`s. JetStream-only options (`jsOption`) are rejected by `Publish` and `Request`. `send` runs the `UsePublish` chain. Batch errors are `PublishBatchError`/`BatchError` with per-index failures.

## Conventions

- Every error string starts with `nats: ` (sentinels in `errors.go`), and wrapping keeps `errors.Is`/`errors.As` working.
- Tests map 1:1 to source files (`foo.go` ↔ `foo_test.go`). The only extras are `helpers_test.go` (`runServer`, `newApp`, `start`, `waitFor`, `recv`, `expectNone`) and `example_test.go`. Test unexported code from inside the package; in the subpackages, test through the public API.
- A new behaviour gets a test that fails without the change.
- Wrap comments at about 100 columns. Docs, README and godoc are in English.
- Pre-1.0: API changes are allowed, but list user-visible changes for the release notes.
