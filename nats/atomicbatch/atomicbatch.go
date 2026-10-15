// Package atomicbatch publishes a batch of messages to JetStream so that the
// server stores all of them or none (server 2.12 or later, stream with
// AllowAtomicPublish).
package atomicbatch

import (
	"context"
	"errors"
	"fmt"

	natsio "github.com/nats-io/nats.go"
	"github.com/synadia-io/orbit.go/jetstreamext"

	"github.com/loipv/nats-go/nats"
)

// Publish stores all msgs or none. All messages must belong to one stream that
// has AllowAtomicPublish. Each message is built like App.PublishJS builds it
// and runs through the app's PublishMiddleware, which therefore sees it before
// it is sent: a middleware span ends before the batch is actually published.
// It returns nats.ErrNotRunning when the app is not running.
func Publish(ctx context.Context, app *nats.App, msgs []nats.BatchMsg) (*jetstreamext.BatchAck, error) {
	if app.JetStream() == nil || app.Conn() == nil || app.Conn().IsClosed() {
		return nil, nats.ErrNotRunning
	}
	if len(msgs) == 0 {
		return nil, errors.New("nats: atomic batch: empty")
	}
	built := make([]*nats.RawMsg, len(msgs))
	for i, bm := range msgs {
		m, err := app.Message(bm.Subject, bm.Data, bm.Opts...)
		if err != nil {
			return nil, fmt.Errorf("nats: atomic batch: message %d: %w", i, err)
		}
		built[i] = m
	}
	collected := make([]*natsio.Msg, 0, len(msgs))
	for i, m := range built {
		sent := false
		collect := func(_ context.Context, m *nats.RawMsg) error {
			if m.Header == nil {
				m.Header = nats.Header{}
			}
			collected = append(collected, m)
			sent = true
			return nil
		}
		if err := app.WrapPublish(collect)(ctx, m); err != nil {
			return nil, fmt.Errorf("nats: atomic batch: message %d: %w", i, err)
		}
		if !sent {
			return nil, fmt.Errorf("nats: atomic batch: message %d: a publish middleware did not call next", i)
		}
	}
	ack, err := jetstreamext.PublishMsgBatch(ctx, app.JetStream(), collected)
	if err != nil {
		return nil, fmt.Errorf("nats: atomic batch: %w", err)
	}
	return ack, nil
}
