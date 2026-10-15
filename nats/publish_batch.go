package nats

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// BatchMsg is one message of PublishJSBatch.
type BatchMsg struct {
	Subject string
	Data    any
	Opts    []PubOption
}

// PublishBatchError reports the messages of a batch that were not stored,
// keyed by their index in the batch. The other messages may have been stored.
type PublishBatchError struct{ Failed map[int]error }

func (e *PublishBatchError) Error() string {
	if e == nil {
		return "nats: batch failed"
	}
	return fmt.Sprintf("nats: %d of batch failed", len(e.Failed))
}

// PublishJSBatch publishes msgs to JetStream in order without waiting for each
// ack, then waits for all of them or for ctx. It is not atomic. acks[i] is set
// for every stored message; if any failed (send, ack, or ctx expired before the
// ack) the error is a *PublishBatchError. A message that cannot be encoded
// fails the whole call before anything is sent.
func (a *App) PublishJSBatch(ctx context.Context, msgs []BatchMsg) ([]*jetstream.PubAck, error) {
	if !a.pubOpen.Load() {
		return nil, ErrNotRunning
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	built := make([]*RawMsg, len(msgs))
	for i, bm := range msgs {
		m, err := a.Message(bm.Subject, bm.Data, bm.Opts...)
		if err != nil {
			return nil, fmt.Errorf("nats: batch message %d: %w", i, err)
		}
		built[i] = m
	}
	futures := make([]jetstream.PubAckFuture, len(msgs))
	failed := map[int]error{}
	for i, m := range built {
		err := a.WrapPublish(func(_ context.Context, m *RawMsg) error {
			f, err := a.js.PublishMsgAsync(m)
			futures[i] = f
			return err
		})(ctx, m)
		if err != nil {
			failed[i] = err
			futures[i] = nil
		} else if futures[i] == nil {
			failed[i] = errors.New("nats: batch message not sent (a publish middleware did not call next)")
		}
	}
	acks := make([]*jetstream.PubAck, len(msgs))
wait:
	for i, f := range futures {
		if f == nil {
			continue
		}
		select {
		case ack := <-f.Ok():
			acks[i] = ack
		case err := <-f.Err():
			failed[i] = err
		case <-ctx.Done():
			for j := i; j < len(futures); j++ {
				if futures[j] != nil {
					failed[j] = ctx.Err()
				}
			}
			break wait
		}
	}
	if len(failed) > 0 {
		return acks, &PublishBatchError{Failed: failed}
	}
	return acks, nil
}
