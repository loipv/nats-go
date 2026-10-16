package nats

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

// PubOption configures one publish.
type PubOption func(*pubConfig)

type pubConfig struct {
	header Header
	// jsHeaders carry the JetStream publish options as headers, so a built
	// message keeps them on any send path.
	jsHeaders map[string]string
	jsOnly    []string
	// jsPubOpts cannot be turned into headers, so only PublishJS applies them.
	jsPubOpts []jetstream.PublishOpt
}

// WithHeader sets headers on the published message.
func WithHeader(h Header) PubOption {
	return func(c *pubConfig) { c.header = h }
}

// jsOption sets a JetStream header. An empty value removes it, but the
// option still requires PublishJS.
func jsOption(name, header, value string) PubOption {
	return func(c *pubConfig) {
		c.jsOnly = append(c.jsOnly, name)
		if value == "" {
			delete(c.jsHeaders, header)
			return
		}
		if c.jsHeaders == nil {
			c.jsHeaders = map[string]string{}
		}
		c.jsHeaders[header] = value
	}
}

// WithMsgID sets Nats-Msg-Id: the stream drops messages with an ID it has seen
// within its Duplicates window. An empty id sets no header. PublishJS only.
func WithMsgID(id string) PubOption { return jsOption("WithMsgID", jetstream.MsgIDHeader, id) }

// WithExpectStream fails the publish unless the subject belongs to stream. An
// empty stream sets no expectation. PublishJS only.
func WithExpectStream(stream string) PubOption {
	return jsOption("WithExpectStream", jetstream.ExpectedStreamHeader, stream)
}

// WithExpectLastSeq fails the publish unless the stream's last sequence is seq. PublishJS only.
func WithExpectLastSeq(seq uint64) PubOption {
	return jsOption("WithExpectLastSeq", jetstream.ExpectedLastSeqHeader, strconv.FormatUint(seq, 10))
}

// WithExpectLastSubjectSeq fails the publish unless the last sequence on the
// subject is seq. PublishJS only.
func WithExpectLastSubjectSeq(seq uint64) PubOption {
	return jsOption("WithExpectLastSubjectSeq", jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(seq, 10))
}

// WithJSPublishOpts passes jetstream publish options straight to nats.go, for
// anything this package does not wrap (such as jetstream.WithMsgTTL or
// jetstream.WithExpectLastMsgID). PublishJS only: they are not headers, so
// Message, PublishJSBatch and atomic batches reject them too.
func WithJSPublishOpts(opts ...jetstream.PublishOpt) PubOption {
	return func(c *pubConfig) {
		c.jsPubOpts = append(c.jsPubOpts, opts...)
		c.jsOnly = append(c.jsOnly, "WithJSPublishOpts")
	}
}

// build encodes v and applies opts without checking the app state or the
// JetStream-only options.
func (a *App) build(subject string, v any, opts []PubOption) (*RawMsg, pubConfig, error) {
	var c pubConfig
	for _, opt := range opts {
		opt(&c)
	}
	data, err := encode(a.opts.codec, v)
	if err != nil {
		return nil, c, fmt.Errorf("nats: encode: %w", err)
	}
	h := maps.Clone(c.header)
	if h == nil && len(c.jsHeaders) > 0 {
		h = Header{}
	}
	for k, v := range c.jsHeaders {
		h.Set(k, v)
	}
	return &RawMsg{Subject: subject, Data: data, Header: h}, c, nil
}

// Message builds the message PublishJS would send, without sending it: v
// encoded by the codec, the headers from opts, and the JetStream options
// (WithMsgID, WithExpectStream, WithExpectLastSeq, WithExpectLastSubjectSeq)
// as headers. Publish rejects those JetStream options, so a message built with
// them is only for PublishJS or a batch. It needs no connection and is meant
// for integrations such as atomic batches. WithJSPublishOpts is rejected: its
// options cannot travel with the message.
func (a *App) Message(subject string, v any, opts ...PubOption) (*RawMsg, error) {
	m, c, err := a.build(subject, v, opts)
	if err == nil && len(c.jsPubOpts) > 0 {
		return nil, fmt.Errorf("nats: option WithJSPublishOpts requires PublishJS")
	}
	return m, err
}

func (a *App) preparePub(subject string, v any, opts []PubOption, js bool) (*RawMsg, pubConfig, error) {
	if !a.pubOpen.Load() {
		return nil, pubConfig{}, ErrNotRunning
	}
	m, c, err := a.build(subject, v, opts)
	if err != nil {
		return nil, c, err
	}
	if !js && len(c.jsOnly) > 0 {
		return nil, c, fmt.Errorf("nats: option %s requires PublishJS", c.jsOnly[0])
	}
	return m, c, nil
}

// WrapPublish wraps f with the publish middleware registered by UsePublish,
// the first registered being the outermost.
func (a *App) WrapPublish(f PublishFunc) PublishFunc {
	for _, mw := range slices.Backward(a.pubMws) {
		f = mw(f)
	}
	return f
}

func (a *App) send(ctx context.Context, m *RawMsg, f PublishFunc) error {
	return a.WrapPublish(f)(ctx, m)
}

// Publish encodes v and sends it on subject without waiting for anyone.
func (a *App) Publish(ctx context.Context, subject string, v any, opts ...PubOption) error {
	m, _, err := a.preparePub(subject, v, opts, false)
	if err != nil {
		return err
	}
	nc := a.nc()
	return a.send(ctx, m, func(_ context.Context, m *RawMsg) error { return nc.PublishMsg(m) })
}

// Request encodes v, sends it on subject and decodes the reply into out,
// following the json.Unmarshal convention: a nil out skips decoding, a
// *[]byte receives the payload unchanged, and any other out must be a
// non-nil pointer. The raw reply is returned so its headers can be read,
// also when decoding fails. The timeout comes from ctx. A reply carrying
// Nats-Service-Error is returned as *ServiceError.
func (a *App) Request(ctx context.Context, subject string, v, out any, opts ...PubOption) (*RawMsg, error) {
	if out != nil {
		if rv := reflect.ValueOf(out); rv.Kind() != reflect.Pointer || rv.IsNil() {
			return nil, fmt.Errorf("nats: request %q: out must be nil or a non-nil pointer, got %T", subject, out)
		}
	}
	m, _, err := a.preparePub(subject, v, opts, false)
	if err != nil {
		return nil, err
	}
	nc := a.nc()
	var reply *RawMsg
	err = a.send(ctx, m, func(ctx context.Context, m *RawMsg) error {
		r, err := nc.RequestMsgWithContext(ctx, m)
		reply = r
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("nats: request %q: %w", subject, err)
	}
	if reply == nil {
		return nil, fmt.Errorf("nats: request %q: no reply (a publish middleware did not call next)", subject)
	}
	if desc := reply.Header.Get(micro.ErrorHeader); desc != "" {
		code, _ := strconv.Atoi(reply.Header.Get(micro.ErrorCodeHeader))
		return nil, &ServiceError{Code: code, Description: desc}
	}
	switch p := out.(type) {
	case nil:
	case *[]byte:
		*p = reply.Data
	default:
		if err := a.opts.codec.Unmarshal(reply.Data, out); err != nil {
			return reply, fmt.Errorf("nats: decode reply: %w", err)
		}
	}
	return reply, nil
}

// PublishJS encodes v, publishes it to JetStream and waits for the stream's ack.
func (a *App) PublishJS(ctx context.Context, subject string, v any, opts ...PubOption) (*jetstream.PubAck, error) {
	m, c, err := a.preparePub(subject, v, opts, true)
	if err != nil {
		return nil, err
	}
	js := a.js()
	var ack *jetstream.PubAck
	err = a.send(ctx, m, func(ctx context.Context, m *RawMsg) error {
		var err error
		ack, err = js.PublishMsg(ctx, m, c.jsPubOpts...)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("nats: publish %q: %w", subject, err)
	}
	return ack, nil
}
