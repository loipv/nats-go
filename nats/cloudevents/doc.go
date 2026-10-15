// Package cloudevents publishes and handles CloudEvents v1.0 messages in the
// binary content mode of the NATS protocol binding: the attributes travel in
// ce-* headers and the payload is the message body.
//
//	ce := cloudevents.New(app, "urn:example:shop")
//	cloudevents.Handle(ce, "order.created",
//		func(ctx context.Context, ev cloudevents.Event, m *nats.Msg[Order]) error {
//			return process(ctx, ev.ID, m.Data)
//		})
//	err := ce.Publish(ctx, "order.created", order, cloudevents.WithExtension("region", "eu"))
//
// An event of type T is published on the subject "events.T" unless
// SubjectFunc says otherwise; Handle subscribes to the same subject.
// PublishJS sends to a stream and uses the event id as Nats-Msg-Id, so the
// stream drops a repeated id within its Duplicates window.
//
// Handle is one-way: it never replies to a request. Use nats.Reply for
// request/reply. A message whose attributes are invalid (missing ce-id,
// ce-source, ce-type or ce-specversion, an unsupported spec version or a bad
// ce-time) fails with nats.Terminal and fn is not called; with the DLQ option
// it is dead-lettered. Handle accepts the options of nats.Subscribe.
//
// Extra headers go through WithHeader; the ce-* and Content-Type headers set
// by the emitter win on a collision. WithPubOptions is for core options that
// are not headers, such as nats.ExpectStream: nats.WithHeader passed there
// would replace the CloudEvents headers.
//
// Structured and batched modes are not supported. For JetStream durable
// consumers, use nats.Consume with FromHeader and filter by type with the
// consumer's FilterSubject.
package cloudevents
