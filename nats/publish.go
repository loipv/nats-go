package nats

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"github.com/nats-io/nats.go/jetstream"
)

// PubOption configures one publish.
type PubOption func(*pubConfig)

type pubConfig struct {
	header Header
	jsOpts []jetstream.PublishOpt
	jsOnly []string
}

// WithHeader sets headers on the published message.
func WithHeader(h Header) PubOption {
	return func(c *pubConfig) { c.header = h }
}

func jsOption(name string, opt jetstream.PublishOpt) PubOption {
	return func(c *pubConfig) {
		c.jsOpts = append(c.jsOpts, opt)
		c.jsOnly = append(c.jsOnly, name)
	}
}

// MsgID sets Nats-Msg-Id: the stream drops messages with an ID it has seen
// within its Duplicates window. PublishJS only.
func MsgID(id string) PubOption { return jsOption("MsgID", jetstream.WithMsgID(id)) }

// ExpectStream fails the publish unless the subject belongs to stream. PublishJS only.
func ExpectStream(stream string) PubOption {
	return jsOption("ExpectStream", jetstream.WithExpectStream(stream))
}

// ExpectLastSeq fails the publish unless the stream's last sequence is seq. PublishJS only.
func ExpectLastSeq(seq uint64) PubOption {
	return jsOption("ExpectLastSeq", jetstream.WithExpectLastSequence(seq))
}

// ExpectLastSubjectSeq fails the publish unless the last sequence on the
// subject is seq. PublishJS only.
func ExpectLastSubjectSeq(seq uint64) PubOption {
	return jsOption("ExpectLastSubjectSeq", jetstream.WithExpectLastSequencePerSubject(seq))
}

func (a *App) prepare(subject string, v any, opts []PubOption, js bool) (*RawMsg, pubConfig, error) {
	var c pubConfig
	if !a.pubOpen.Load() {
		return nil, c, ErrNotRunning
	}
	for _, opt := range opts {
		opt(&c)
	}
	if !js && len(c.jsOnly) > 0 {
		return nil, c, fmt.Errorf("nats: option %s requires PublishJS", c.jsOnly[0])
	}
	data, err := encode(a.opts.codec, v)
	if err != nil {
		return nil, c, fmt.Errorf("nats: encode: %w", err)
	}
	return &RawMsg{Subject: subject, Data: data, Header: maps.Clone(c.header)}, c, nil
}

func (a *App) send(ctx context.Context, m *RawMsg, f PublishFunc) error {
	for _, mw := range slices.Backward(a.pubMws) {
		f = mw(f)
	}
	return f(ctx, m)
}

// Publish encodes v and sends it on subject without waiting for anyone.
func (a *App) Publish(ctx context.Context, subject string, v any, opts ...PubOption) error {
	m, _, err := a.prepare(subject, v, opts, false)
	if err != nil {
		return err
	}
	return a.send(ctx, m, func(_ context.Context, m *RawMsg) error { return a.nc.PublishMsg(m) })
}

// Request encodes v, sends it on subject and decodes the reply into Resp.
// The timeout comes from ctx. A reply carrying Nats-Service-Error is
// returned as *ServiceError.
func Request[Resp any](ctx context.Context, app *App, subject string, v any, opts ...PubOption) (*Msg[Resp], error) {
	m, _, err := app.prepare(subject, v, opts, false)
	if err != nil {
		return nil, err
	}
	var reply *RawMsg
	err = app.send(ctx, m, func(ctx context.Context, m *RawMsg) error {
		r, err := app.nc.RequestMsgWithContext(ctx, m)
		reply = r
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("nats: request %q: %w", subject, err)
	}
	if reply == nil {
		return nil, fmt.Errorf("nats: request %q: no reply (a publish middleware did not call next)", subject)
	}
	if desc := reply.Header.Get("Nats-Service-Error"); desc != "" {
		code, _ := strconv.Atoi(reply.Header.Get("Nats-Service-Error-Code"))
		return nil, &ServiceError{Code: code, Description: desc}
	}
	data, err := decode[Resp](app.opts.codec, reply.Data)
	if err != nil {
		return nil, fmt.Errorf("nats: decode reply: %w", err)
	}
	return &Msg[Resp]{Subject: reply.Subject, Header: reply.Header, Data: data, Raw: reply}, nil
}

// PublishJS encodes v, publishes it to JetStream and waits for the stream's ack.
func (a *App) PublishJS(ctx context.Context, subject string, v any, opts ...PubOption) (*jetstream.PubAck, error) {
	m, c, err := a.prepare(subject, v, opts, true)
	if err != nil {
		return nil, err
	}
	var ack *jetstream.PubAck
	err = a.send(ctx, m, func(ctx context.Context, m *RawMsg) error {
		var err error
		ack, err = a.js.PublishMsg(ctx, m, c.jsOpts...)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("nats: publish %q: %w", subject, err)
	}
	return ack, nil
}
