// Package atomicbatch publishes a batch of messages to JetStream so that the
// server stores all of them or none.
//
//	ack, err := atomicbatch.Publish(ctx, app, []nats.BatchMsg{
//		{Subject: "orders.created", Data: order},
//		{Subject: "orders.items", Data: items},
//	})
//
// Requirements: nats-server 2.12 or later, and a stream with
// AllowAtomicPublish that captures every subject of the batch. All messages
// must belong to the same stream. Without these the call fails and nothing is
// stored. Use App.PublishJSBatch when you do not need all-or-nothing: it
// works on any server but is not atomic.
//
// Messages are built like App.PublishJS builds them (codec, headers, WithMsgID)
// and run through the app's PublishMiddleware before the batch is sent. A
// middleware span therefore ends before the messages are actually published,
// and does not see the outcome of the batch.
//
// Publish returns when the server has committed or rejected the batch. If ctx
// expires while the commit is in flight, the outcome is unknown: the batch may
// have been stored. A message option such as WithExpectLastSeq only makes sense on
// the first message of the batch; the server checks the stream's expected
// last sequence once, when the batch starts.
package atomicbatch
