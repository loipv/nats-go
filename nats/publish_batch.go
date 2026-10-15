package nats

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
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
// *PublishBatchError. A message that cannot be encoded fails the whole call
// before anything is sent. Once ctx is done no further message is started.
//
// Beyond about 4000 unacknowledged messages in flight a send stalls for up to
// 200ms and then fails with jetstream.ErrTooManyStalledMsgs (not stored).
// Storage order is not strictly guaranteed: jetstream resends a message after
// a no-responders reply, which can land it behind later ones. An ack that
// never arrives fails with jetstream.ErrAsyncPublishTimeout after 30s.
//
// A publish middleware sees each message when it is handed to the client, not
// its ack: a natsotel "send" span does not record the ack result. A middleware
// that calls next and then returns an error marks that message failed with
// its error although it was sent, and one returning a bare context error
// makes the outcome look unknown.
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

// BatchAck is the server's ack for PublishJSAtomicBatch.
type BatchAck struct {
	Stream string
	// Sequence is the stream sequence of the last message of the batch.
	Sequence uint64
	Domain   string
	// Value is the counter value, set only on a stream with AllowMsgCounter.
	Value     string
	BatchID   string
	BatchSize uint64
}

// PublishJSAtomicBatch publishes msgs to JetStream so that the server stores
// all of them or none. It needs nats-server 2.12 or later, and every message
// must belong to one stream that has AllowAtomicPublish; otherwise the call
// fails and nothing is stored. Use PublishJSBatch when you do not need
// all-or-nothing.
//
// Each message is built like PublishJS builds it and runs through the
// PublishMiddleware before the batch is sent: a middleware span ends before
// the batch is actually published and does not see its outcome. It returns
// when the server has committed or rejected the batch; if ctx expires while
// the commit is in flight, the outcome is unknown. WithExpectLastSeq only makes
// sense on the first message: the server checks it once, when the batch starts.
func (a *App) PublishJSAtomicBatch(ctx context.Context, msgs []BatchMsg) (*BatchAck, error) {
	if !a.pubOpen.Load() {
		return nil, ErrNotRunning
	}
	if len(msgs) == 0 {
		return nil, errors.New("nats: atomic batch: empty")
	}
	built := make([]*RawMsg, len(msgs))
	for i, bm := range msgs {
		m, err := a.Message(bm.Subject, bm.Data, bm.Opts...)
		if err != nil {
			return nil, fmt.Errorf("nats: atomic batch: message %d: %w", i, err)
		}
		built[i] = m
	}
	collected := make([]*RawMsg, 0, len(msgs))
	for i, m := range built {
		sent := false
		collect := func(_ context.Context, m *RawMsg) error {
			if m.Header == nil {
				m.Header = Header{}
			}
			collected = append(collected, m)
			sent = true
			return nil
		}
		if err := a.WrapPublish(collect)(ctx, m); err != nil {
			return nil, fmt.Errorf("nats: atomic batch: message %d: %w", i, err)
		}
		if !sent {
			return nil, fmt.Errorf("nats: atomic batch: message %d: a publish middleware did not call next", i)
		}
	}
	ack, err := jetstreamext.PublishMsgBatch(ctx, a.js, collected)
	if err != nil {
		return nil, fmt.Errorf("nats: atomic batch: %w", err)
	}
	return &BatchAck{
		Stream: ack.Stream, Sequence: ack.Sequence, Domain: ack.Domain,
		Value: ack.Value, BatchID: ack.BatchID, BatchSize: ack.BatchSize,
	}, nil
}
