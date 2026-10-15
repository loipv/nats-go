package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Header is the NATS message header type.
type Header = natsio.Header

// RawMsg is an undecoded NATS message, as seen by middleware.
type RawMsg = natsio.Msg

// Msg is a decoded message passed to typed handlers.
type Msg[T any] struct {
	Subject string
	Header  Header
	Data    T
	// Raw is the undecoded message.
	Raw *RawMsg

	js      jetstream.Msg
	attempt int
	codec   Codec
	// replyOwned is set in SubscribeReply and endpoint handlers, which respond
	// with their return value.
	replyOwned bool
}

// canRespond reports why m cannot be responded to by hand, if it cannot.
func (m *Msg[T]) canRespond() error {
	switch {
	case m.Raw == nil:
		return natsio.ErrMsgNotBound // a Msg built by hand, as in a unit test
	case m.js != nil:
		return ErrNotCore
	case m.replyOwned:
		return errReplyOwned
	}
	return nil
}

// Respond answers a request received by Subscribe: v is encoded with the app
// codec ([]byte as is) and sent to the reply subject. It does not go through
// publish middleware. It returns ErrNotCore on a JetStream message, an error
// in a SubscribeReply or endpoint handler (return the response instead), and
// nats.go's ErrMsgNoReply when the message has no reply subject. A handler
// retried by WithMaxAttempts responds again on every attempt that calls it;
// the requester only reads the first response.
func (m *Msg[T]) Respond(v any) error {
	if err := m.canRespond(); err != nil {
		return err
	}
	data, err := encode(m.codec, v)
	if err != nil {
		return fmt.Errorf("nats: encode response: %w", err)
	}
	return m.Raw.Respond(data)
}

// RespondError answers a request received by Subscribe with err, as
// SubscribeReply does: in the Nats-Service-Error headers, with code 400 for
// Terminal errors and 500 otherwise, and "internal error" for a panic. The
// requester's App.Request returns it as *ServiceError. It fails like Respond.
func (m *Msg[T]) RespondError(err error) error {
	if cerr := m.canRespond(); cerr != nil {
		return cerr
	}
	if err == nil {
		return errors.New("nats: RespondError: nil error")
	}
	return respondError(m.Raw, err)
}

// Attempt returns the 1-based attempt of the message: the JetStream delivery
// count for Consume, or the in-process attempt number for handlers retried
// by WithMaxAttempts, 1 otherwise.
func (m *Msg[T]) Attempt() int { return m.attempt }

func (m *Msg[T]) jsMsg() (jetstream.Msg, error) {
	if m.js == nil {
		return nil, ErrNotJetStream
	}
	return m.js, nil
}

// Ack acknowledges a JetStream message. Use it with WithManualAck.
func (m *Msg[T]) Ack() error {
	js, err := m.jsMsg()
	if err != nil {
		return err
	}
	return js.Ack()
}

// Nak asks the server to redeliver a JetStream message.
func (m *Msg[T]) Nak() error {
	js, err := m.jsMsg()
	if err != nil {
		return err
	}
	return js.Nak()
}

// NakWithDelay asks the server to redeliver a JetStream message after d.
func (m *Msg[T]) NakWithDelay(d time.Duration) error {
	js, err := m.jsMsg()
	if err != nil {
		return err
	}
	return js.NakWithDelay(d)
}

// Term tells the server never to redeliver a JetStream message.
func (m *Msg[T]) Term() error {
	js, err := m.jsMsg()
	if err != nil {
		return err
	}
	return js.Term()
}

// InProgress resets the ack wait timer of a JetStream message.
func (m *Msg[T]) InProgress() error {
	js, err := m.jsMsg()
	if err != nil {
		return err
	}
	return js.InProgress()
}

// decodeMsg decodes and validates raw into a Msg, taking the JetStream
// message and attempt from ctx. Decode and validation failures are terminal.
func decodeMsg[T any](c Codec, ctx context.Context, raw *RawMsg) (*Msg[T], error) {
	data, err := decode[T](c, raw.Data)
	if err != nil {
		return nil, Terminal(fmt.Errorf("nats: decode: %w", err))
	}
	if v, ok := any(&data).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return nil, Terminal(fmt.Errorf("nats: validate: %w", err))
		}
	}
	_, owned := ctx.Value(replyKey{}).(*reply)
	return &Msg[T]{Subject: raw.Subject, Header: raw.Header, Data: data, Raw: raw, js: jsMsgFrom(ctx), attempt: attemptFrom(ctx),
		codec: c, replyOwned: owned}, nil
}
