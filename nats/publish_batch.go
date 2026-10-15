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

// PublishBatchError reports the messages of a batch that failed, keyed by their
// index in the batch. A message whose error is "nats: batch message N not
// sent: <ctx error>" was never sent. A message whose error is the bare
// context error was sent but its ack did not arrive in time: the outcome is
// unknown. The other messages may have been stored.
type PublishBatchError struct{ Failed map[int]error }

func (e *PublishBatchError) Error() string {
	if e == nil {
		return "nats: batch failed"
	}
	return fmt.Sprintf("nats: %d of batch failed", len(e.Failed))
}

// PublishJSBatch publishes msgs to JetStream in order without waiting for each
// ack, then waits for all of them or for ctx. It is not atomic. acks[i] is set
// for every message known to be stored; if any failed (send, ack, or ctx
// expired before the ack, in which case the outcome is unknown) the error is a
// *PublishBatchError. A message that cannot be encoded
// fails the whole call before anything is sent. Once ctx is done no further
// message is sent. Beyond about 4000 unacknowledged messages in flight a send
// stalls for up to 200ms and then fails with jetstream.ErrTooManyStalledMsgs
// (not stored). Storage order is not strictly guaranteed: jetstream resends a
// message after a no-responders reply, which can land it behind later ones.
// An ack that never arrives fails with jetstream.ErrAsyncPublishTimeout after
// 30s. A publish middleware sees each message when it is handed to the
// client, not its ack: a natsotel "send" span does not record the ack result.
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
		if err := ctx.Err(); err != nil {
			for j := i; j < len(built); j++ {
				failed[j] = fmt.Errorf("nats: batch message %d not sent: %w", j, err)
			}
			break
		}
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
	// settle records the result of future i if it is already there.
	settle := func(i int) bool {
		select {
		case ack := <-futures[i].Ok():
			acks[i] = ack
		case err := <-futures[i].Err():
			failed[i] = err
		default:
			return false
		}
		return true
	}
	expired := false
	for i, f := range futures {
		if f == nil || settle(i) {
			continue
		}
		if !expired {
			select {
			case ack := <-f.Ok():
				acks[i] = ack
				continue
			case err := <-f.Err():
				failed[i] = err
				continue
			case <-ctx.Done():
				expired = true
			}
		}
		// ctx expired: acks that already arrived still count, the rest is unknown.
		if !settle(i) {
			failed[i] = ctx.Err()
		}
	}
	if len(failed) > 0 {
		return acks, &PublishBatchError{Failed: failed}
	}
	return acks, nil
}
