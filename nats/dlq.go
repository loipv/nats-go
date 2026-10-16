package nats

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// dlqSource describes where a dead-lettered message came from.
type dlqSource struct {
	stream, consumer string
	seq              uint64
	deliveries       int
	// msgID is true when seq is known, so the DLQ copy gets
	// Nats-DLQ-Sequence and a stable Nats-Msg-Id.
	msgID bool
}

// publishDLQ publishes orig to subject with headers describing the failure.
// It bypasses publish middleware.
func (a *App) publishDLQ(ctx context.Context, subject string, orig *RawMsg, cause error, src dlqSource) error {
	h := Header{}
	for k, v := range orig.Header {
		// Dropped so the DLQ stream neither dedups nor rejects the copy.
		if k == jetstream.MsgIDHeader || strings.HasPrefix(k, "Nats-Expected-") {
			continue
		}
		h[k] = slices.Clone(v)
	}
	h.Set("Nats-DLQ-Error", firstLine(cause))
	h.Set("Nats-DLQ-Subject", orig.Subject)
	h.Set("Nats-DLQ-Deliveries", strconv.Itoa(src.deliveries))
	if src.stream != "" {
		h.Set("Nats-DLQ-Stream", src.stream)
	}
	if src.msgID { // the sequence is known
		h.Set("Nats-DLQ-Sequence", strconv.FormatUint(src.seq, 10))
	}
	if src.consumer != "" {
		h.Set("Nats-DLQ-Consumer", src.consumer)
	}
	if id := orig.Header.Get(jetstream.MsgIDHeader); id != "" {
		h.Set("Nats-DLQ-Msg-Id", id)
	}
	var opts []jetstream.PublishOpt
	if src.msgID {
		opts = append(opts, jetstream.WithMsgID(fmt.Sprintf("dlq:%s:%d", src.stream, src.seq)))
	}
	if _, err := a.js().PublishMsg(ctx, &RawMsg{Subject: subject, Header: h, Data: orig.Data}, opts...); err != nil {
		return fmt.Errorf("nats: publish DLQ %q: %w", subject, err)
	}
	return nil
}

// checkDLQ verifies that a stream captures the DLQ subject and that it is
// not the source stream, which would feed failures back to the handler.
func (a *App) checkDLQ(ctx context.Context, subject, sourceStream string) error {
	name, err := a.js().StreamNameBySubject(ctx, subject)
	if err != nil {
		return fmt.Errorf("nats: no stream for DLQ subject %q: %w", subject, err)
	}
	if name == sourceStream {
		return fmt.Errorf("nats: DLQ subject %q is captured by source stream %q", subject, sourceStream)
	}
	return nil
}

// firstLine returns the first line of err's message: headers cannot carry
// newlines, and multi-line errors (such as Recover's stack trace) must not
// leak to other services.
func firstLine(err error) string {
	s, _, _ := strings.Cut(err.Error(), "\n")
	return s
}
