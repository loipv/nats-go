// Package nats is a framework for building NATS and JetStream services on top
// of github.com/nats-io/nats.go.
//
// It adds typed handlers, middleware, declarative streams and consumers,
// fail-fast startup and bounded graceful shutdown, while keeping the
// underlying connection available through App.Conn and App.JetStream.
//
// This package shares its name with nats.go. Common connection, auth and TLS
// options are re-exported (UserCredentials, Token, RootCAs, ClientCert, ...)
// so most programs never import nats.go directly. For anything else, import
// nats.go under an alias and use WithNATSOptions.
//
// # Lifecycle
//
// Create an App, register handlers, streams and middleware, then call Run:
//
//	app, err := nats.New(nats.WithURL("nats://127.0.0.1:4222"))
//	if err != nil { ... }
//	nats.Subscribe(app, "orders.*", handleOrder)
//	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
//	defer stop()
//	if err := app.Run(ctx); err != nil { log.Fatal(err) }
//
// Start connects, creates or updates declared streams and consumers, checks
// that every consumed consumer exists and starts every handler, in that
// order; no handler receives a message before every check has passed. The first failure stops everything already started and is returned.
// Registration mistakes (empty subject, an option used on the wrong kind of
// handler, a duplicate consumer) are collected and returned by Start too.
//
// Shutdown stops intake, waits for in-flight handlers and drains the
// connection, bounded by its context. Run calls Start, waits for its context
// to be cancelled and calls Shutdown with the WithShutdownTimeout limit.
// Programs that only publish can call Start and Shutdown directly.
//
// # Handlers
//
// Handlers receive a *Msg[T] whose Data is decoded with the App's Codec
// (JSON by default; []byte is passed through untouched). When T or *T has a
// Validate() error method it is called after decoding.
//
//   - Subscribe handles core NATS messages, with wildcards and Queue groups.
//   - Reply answers requests; the returned value is encoded and sent back.
//   - Consume handles a durable JetStream consumer; pull and push consumers
//     are detected at Start.
//   - ConsumeOrdered reads a stream in order through an ephemeral ordered
//     consumer; it accepts only Concurrency(1) so order is kept.
//
// The error a handler returns decides what happens to the message:
//
//	returned error                  JetStream    Reply
//	nil                             Ack          encoded response
//	other error                     Nak          Nats-Service-Error, code 500
//	Terminal(err), decode/validate  Term         Nats-Service-Error, code 400
//
// With ManualAck the handler acks itself through Msg.Ack, Nak, NakWithDelay,
// Term and InProgress, and the returned error is only logged.
//
// # Publishing
//
// App.Publish sends fire-and-forget, Request waits for a typed reply (a
// failed responder comes back as *ServiceError) and App.PublishJS waits for
// the stream's ack, with MsgID for deduplication.
//
// # Middleware
//
// Middleware wraps handlers on raw messages, before decoding: App.Use for
// every handler, With for one. Recover and Logger are provided. PublishMiddleware
// wraps every outgoing message, registered with App.UsePublish.
//
// # Health
//
// App.Health reports the connection and handler state; App.HealthHandler
// serves it as JSON with status 200 or 503.
//
// Runnable programs live in the examples directory of the repository.
package nats
