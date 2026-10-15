// Package nats is a framework for building NATS and JetStream services on top
// of github.com/nats-io/nats.go.
//
// It adds typed handlers, middleware, declarative streams and consumers,
// fail-fast startup and bounded graceful shutdown, while keeping the
// underlying connection available through App.Conn and App.JetStream.
//
// This package shares its name with nats.go. Common connection, auth and TLS
// options are re-exported (WithUserCredentials, WithToken, WithRootCAs, WithClientCert, ...)
// so most programs never import nats.go directly. For anything else, import
// nats.go under an alias and use WithNATSOptions.
//
// # Lifecycle
//
// Create an App, register handlers, streams and middleware, then call Run:
//
//	app, err := nats.New(nats.WithURL("nats://127.0.0.1:4222"))
//	if err != nil { ... }
//	app.Subscribe("orders.*", nats.Typed(handleOrder))
//	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
//	defer stop()
//	if err := app.Run(ctx); err != nil { log.Fatal(err) }
//
// Start connects, creates or updates declared streams and consumers, checks
// that every consumed consumer exists and starts every handler, in that
// order; no handler receives a message before every check has passed. The first
// failure stops everything already started and is returned, and Start may then
// be called again. Registration mistakes (empty subject, an option used on the
// wrong kind of handler, an invalid option value, a duplicate consumer) are
// collected and returned by Start too.
//
// App.Hook registers a Hook with OnStart and OnStop callbacks. Hooks start in
// registration order after publishing opens and before any handler starts, and
// stop in reverse order after the handlers have stopped (or Shutdown's
// deadline passed). A failed Start stops the hooks it started and joins their
// OnStop errors into its error, each wrapped "nats: stop hook i: <err>";
// OnStop receives Shutdown's context, even when it has expired, and its errors
// are joined into Shutdown's. Panics are not recovered.
//
// Start does not hold the App lock while it connects, so Health and
// HealthHandler answer during startup with ready false; Conn and JetStream
// return nil unless the App is started or starting. Registering after Start
// began is rejected.
//
// Shutdown stops intake, waits for in-flight handlers and drains the
// connection, bounded by its context. A second Shutdown that starts while the
// first is running waits for it and returns the same error. A Shutdown during
// Start waits for Start, or cancels it when its context ends first. Run calls
// Start, waits for its context to be cancelled or for the connection to be
// closed for good, and calls Shutdown with the WithShutdownTimeout limit. If
// that Shutdown fails, Run returns its error; otherwise, when the connection
// closed for good, Run returns an error wrapping nats.go's
// ErrConnectionClosed. Programs that only publish can call Start and Shutdown
// directly.
//
// # Events
//
// The WithOn* options register callbacks for what the library would otherwise
// only log: WithOnRetry, WithOnFailed, WithOnDeadLettered and
// WithOnDeadLetterFailed for messages, WithOnHandlerRestarting,
// WithOnHandlerRestarted and WithOnHandlerStopped for handlers,
// WithOnDisconnected, WithOnReconnected and WithOnClosed for the connection,
// and WithOnAsyncError for failed acks, responses, restarts and connection
// errors. Each option may be given several times; callbacks of one kind run
// in the order given, and a nil function makes New return an error. Callbacks
// run synchronously on the goroutine where the event happens and must not
// block, and may run concurrently, so they must be safe for concurrent use; a
// panic in one is recovered and logged. Callbacks passed in WithNATSOptions
// are chained: the user's runs first, then the library's. Without a user
// ErrorHandler, nats.go's async errors are logged through the App's logger.
// Shutdown returns
// after OnClosed ran when it drains within its deadline; on a deadline, and
// after a failed Start, the connection events may arrive after the call
// returns.
//
// # Handlers
//
// Handlers receive a *Msg[T] whose Data is decoded with the App's Codec
// (JSON by default; []byte is passed through untouched). When T or *T has a
// Validate() error method it is called after decoding.
//
// Handlers are registered with App methods and wrapped in an adapter that
// carries their type, because Go methods cannot have type parameters:
// Typed for Subscribe, Consume and ConsumeOrdered, TypedReply for SubscribeReply and
// service endpoints, TypedBatch for ConsumeBatch. T is inferred from the
// handler:
//
//	app.SubscribeReply("billing.get", nats.TypedReply(func(ctx context.Context, m *nats.Msg[GetInvoice]) (Invoice, error) {
//		return load(ctx, m.Data.ID)
//	}))
//
// The registration methods:
//
//   - Subscribe handles core NATS messages, with wildcards and queue groups (WithQueue).
//   - SubscribeReply answers requests; the returned value is encoded and sent back.
//     A Subscribe handler can also answer by hand with Msg.Respond and
//     Msg.RespondError.
//   - Consume handles a durable JetStream consumer; pull and push consumers
//     are detected at Start.
//   - ConsumeOrdered reads a stream in order through an ephemeral ordered
//     consumer; it accepts only WithConcurrency(1) so order is kept.
//
// The error a handler returns decides what happens to the message:
//
//	returned error                  JetStream      Reply
//	nil                             Ack            encoded response
//	other error                     NakWithDelay   Nats-Service-Error, code 500
//	Terminal(err), decode/validate  Term           Nats-Service-Error, code 400
//
// Retries, the last attempt and the DLQ are covered under Reliability.
//
// Using WithManualAck, the handler acks itself through Msg.Ack, Nak, NakWithDelay,
// Term and InProgress, and the returned error is only logged; it emits no
// message event (see Events).
//
// Option values are checked at registration: WithPullMaxMessages needs n of at
// least 1 and WithTimeout a duration of 0 (no timeout) or more. Both are
// reported by Start, as is WithPullMaxMessages on a push consumer.
//
// # Reliability
//
// Failed messages are retried with a backoff and, when they run out of
// attempts, sent to a dead-letter queue. WithMaxAttempts, WithBackoff and WithDLQ
// configure this for every kind of handler; they cannot be combined with
// WithManualAck. Msg.Attempt returns the current attempt, starting at 1.
//
// For Consume and ConsumeBatch the server redelivers: a failed message is
// naked with a delay from the backoff, by default ExponentialBackoff(1s, 1m),
// and retried without limit. For Subscribe, SubscribeReply and ConsumeOrdered there is
// no redelivery, so a failed message is not retried unless WithMaxAttempts is set;
// the library then retries in the worker, waiting for the backoff between
// attempts. Unlimited is allowed but can stall the subscription while
// failures last, SubscribeReply must finish its retries within the requester's timeout,
// and ConsumeOrdered stalls the whole stream while it retries, which keeps
// the order.
//
// What happens to a message of a Consume or ConsumeBatch handler with
// automatic ack (attempt is the JetStream delivery count):
//
//	handler result                                no DLQ              DLQ
//	nil                                           Ack                 Ack
//	error, attempts left                          NakWithDelay        NakWithDelay
//	error, last attempt                           log, Term           publish to DLQ, Term
//	Terminal(err), decode or validation failure   Term                publish to DLQ, Term
//	delivered more than WithMaxAttempts times     Term, no handler    publish to DLQ, Term, no handler
//	DLQ publish fails                             -                   log, NakWithDelay
//
// A panic in a handler is recovered by the worker and handled like any other
// error; the panic value and stack are logged. A requester of a SubscribeReply handler
// or micro endpoint that panicked receives Nats-Service-Error "internal
// error" with code 500, never the panic text. Recover only turns the panic
// into an error earlier, for the middleware around it.
//
// The DLQ subject must be a valid subject without wildcard tokens (* or >),
// captured by an existing stream other than the handler's source stream, and
// must not match a Subscribe or SubscribeReply handler's own subject; Start fails
// otherwise. The dead-lettered message keeps its payload and headers and gains
// Nats-DLQ-Error, Nats-DLQ-Subject and Nats-DLQ-Deliveries, plus, for
// JetStream consumers, Nats-DLQ-Stream, Nats-DLQ-Consumer, Nats-DLQ-Sequence
// and Nats-DLQ-Msg-Id (the original Nats-Msg-Id). Publishing the same message
// again does not create a duplicate within the DLQ stream's Duplicates window.
// For Consume and ConsumeBatch with a finite WithMaxAttempts, the consumer's
// MaxDeliver must be -1 or greater than WithMaxAttempts, or Start fails.
//
// WithAutoInProgress sends in-progress signals every AckWait/2 while a Consume or
// ConsumeBatch handler runs, so a slow handler is not redelivered; combine it
// with WithTimeout.
//
// Idempotent is a middleware that skips messages whose key (by default the
// Nats-Msg-Id header, see WithIdempotencyKey) was already processed. It claims the
// key with an IdempotencyStore, runs the handler, then marks the key done, or
// releases the claim if the handler fails or panics. Duplicates are acked
// without calling the handler (with WithManualAck too), and a duplicate request
// gets a 400; a duplicate is never retried or sent to the DLQ.
// NewMemoryStore is correct for a single instance. NewKVStore keeps the keys in a JetStream
// key-value bucket and is correct across instances; it needs nats-server 2.11
// or later and rounds every TTL up to a whole second. WithClaimTTL must exceed
// the worst-case handler duration, because claims are released by key and
// carry no owner. Register it after Recover:
//
//	app.Use(nats.Recover(), nats.Idempotent(nats.NewKVStore(app, "IDEMPOTENCY")))
//
// ConsumeBatch passes up to WithBatchSize messages (waiting at most WithBatchWait) to
// one call on a pull consumer. Return nil to ack them all, a *BatchError to
// fail only the listed indexes, or any other error to fail them all. A
// non-nil *BatchError is found through errors.Join or a wrapper and fails only
// its listed indexes, acking the rest; only a typed-nil *BatchError returned
// directly counts as no error. Failed messages follow the table above. Middleware does not apply to batches, and
// Idempotent cannot be used with them.
//
// Consume and ConsumeBatch handlers are supervised: when the consumer or its
// stream is deleted, the handler goes to the "restarting" state, recreates
// whatever was declared with App.Stream and App.Consumer, and resumes, waiting
// WithRestartBackoff between attempts (default ExponentialBackoff(1s, 30s))
// and trying until it succeeds or the App shuts down. HandlerStatus.Restarts
// counts the restarts, and App.Health reports "degraded" meanwhile. A push
// consumer should set IdleHeartbeat: without it a deleted push consumer cannot
// be detected.
//
// # Publishing
//
// App.Publish sends fire-and-forget, App.Request waits for a reply and
// decodes it into out (a failed responder comes back as *ServiceError) and
// App.PublishJS waits for the stream's ack, with WithMsgID for deduplication.
// WithJSPublishOpts passes any other jetstream.PublishOpt (such as
// jetstream.WithMsgTTL) to PublishJS.
//
//	var inv Invoice
//	_, err := app.Request(ctx, "billing.get", GetInvoice{ID: "42"}, &inv)
//
// # Middleware
//
// Middleware wraps handlers on raw messages, before decoding: App.Use for
// every handler, WithMiddleware for one. Recover and Logger are provided. PublishMiddleware
// wraps every outgoing message, registered with App.UsePublish.
//
// # Integrations
//
// Service registers a NATS micro service, found by "nats micro ls|info|stats".
// Endpoint adds typed endpoints to it, or to a Group of it, and works like
// SubscribeReply: the returned value is encoded and sent back, and an error becomes a
// micro service error (code 400 for Terminal, 500 otherwise) that the service
// STATS count. A request without a reply subject is skipped with a warning.
// Health reports one handler per endpoint, named after the service, the
// group prefixes and the endpoint joined with "." (orders.v2.get). The WithTimeout, WithMiddleware, WithMaxAttempts, WithBackoff and WithDLQ options
// apply; WithEndpointSubject and WithEndpointMetadata are specific to endpoints.
//
//	svc := app.Service(micro.Config{Name: "inventory", Version: "1.0.0"})
//	svc.Group("inventory").Endpoint("get", nats.TypedReply(getStock)) // subject inventory.get
//
// An endpoint handles one request at a time per instance, in micro's
// goroutine; scale with more instances, which share a queue group.
// WithMaxAttempts, WithBackoff and Unlimited stall the endpoint while a request
// is retried, and the whole retry time must fit the requester's timeout. On
// Shutdown the requests already delivered are drained and answered, but a
// handler that publishes through the App then fails, because publishing is
// closed. A middleware that does not call next makes the endpoint send an
// empty response.
//
// App.PublishJSBatch publishes many messages to JetStream and waits for all
// the acks together. It is not atomic: acks[i] is non-nil for every message
// known to be stored, and the *PublishBatchError lists the others by index.
// An entry whose error is the bare context error means the outcome is
// unknown; once ctx is done the remaining messages are not sent and their
// entries say "not sent". Beyond about 4000 unacknowledged messages in flight
// a send stalls for up to 200ms and then fails with
// jetstream.ErrTooManyStalledMsgs (definitely not stored), and an ack that
// never arrives fails with jetstream.ErrAsyncPublishTimeout after 30s. Split
// bigger batches. App.PublishJSAtomicBatch publishes a batch that the server
// stores completely or not at all (nats-server 2.12 or later, and a stream
// with AllowAtomicPublish).
//
// Two subpackages hold the integrations, so this package imports neither:
//
//   - natsotel adds OpenTelemetry tracing and metrics as middleware.
//   - cloudevents publishes and handles CloudEvents v1.0 events.
//
// App.Message builds the message PublishJS would send, without sending it, and
// App.WrapPublish wraps a send function with the registered PublishMiddleware;
// the subpackages use them. Attempt returns the attempt of the message being
// handled, for middleware.
//
// # Health
//
// App.Health reports the connection and handler state; App.HealthHandler
// serves it as JSON with status 200 or 503.
//
// Runnable programs live in the examples directory of the repository.
package nats
