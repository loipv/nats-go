package cloudevents

import (
	"context"
	"crypto/rand"
	"fmt"
	"regexp"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/loipv/nats-go/nats"
)

var extName = regexp.MustCompile(`^[a-z0-9]{1,20}$`)

// Emitter publishes CloudEvents through an App.
type Emitter struct {
	app        *nats.App
	source     string
	subjectFor func(eventType string) string
}

// Option configures an Emitter.
type Option func(*Emitter)

// SubjectFunc maps an event type to the NATS subject it is published on and
// handled from. The default is "events." + eventType.
func SubjectFunc(fn func(eventType string) string) Option {
	return func(e *Emitter) { e.subjectFor = fn }
}

// New returns an Emitter that stamps source into ce-source. It panics when
// source is empty.
func New(app *nats.App, source string, opts ...Option) *Emitter {
	if source == "" {
		panic("nats: cloudevents: empty source")
	}
	e := &Emitter{app: app, source: source, subjectFor: func(t string) string { return "events." + t }}
	for _, o := range opts {
		o(e)
	}
	return e
}

// Subject returns the NATS subject for eventType.
func (e *Emitter) Subject(eventType string) string { return e.subjectFor(eventType) }

// PublishOption configures one Publish or PublishJS.
type PublishOption func(*publishOptions)

type publishOptions struct {
	id, subject, contentType, dataSchema string
	time                                 time.Time
	extensions                           [][2]string
	header                               nats.Header
	pub                                  []nats.PubOption
}

// WithID sets ce-id. The default is a random UUID v4.
func WithID(id string) PublishOption { return func(o *publishOptions) { o.id = id } }

// WithSubject sets ce-subject.
func WithSubject(s string) PublishOption { return func(o *publishOptions) { o.subject = s } }

// WithTime sets ce-time. The default is the time of the publish.
func WithTime(t time.Time) PublishOption { return func(o *publishOptions) { o.time = t } }

// WithContentType sets Content-Type. The default is application/octet-stream
// for []byte data and application/json otherwise.
func WithContentType(ct string) PublishOption { return func(o *publishOptions) { o.contentType = ct } }

// WithDataSchema sets ce-dataschema.
func WithDataSchema(uri string) PublishOption { return func(o *publishOptions) { o.dataSchema = uri } }

// WithExtension sets the extension attribute ce-<name>. The name must match
// ^[a-z0-9]{1,20}$ and must not be a standard attribute name; otherwise the
// publish fails.
func WithExtension(name, value string) PublishOption {
	return func(o *publishOptions) { o.extensions = append(o.extensions, [2]string{name, value}) }
}

// WithHeader adds headers to the message. The ce-* and Content-Type headers
// set by the emitter win when they collide.
func WithHeader(h nats.Header) PublishOption { return func(o *publishOptions) { o.header = h } }

// WithPubOptions passes core options that are not headers, such as
// nats.ExpectStream. Do not pass nats.WithHeader: it replaces the whole
// header, including the CloudEvents attributes. Use WithHeader instead.
func WithPubOptions(opts ...nats.PubOption) PublishOption {
	return func(o *publishOptions) { o.pub = append(o.pub, opts...) }
}

// Publish sends data as a CloudEvent of eventType with a core NATS publish.
func (e *Emitter) Publish(ctx context.Context, eventType string, data any, opts ...PublishOption) error {
	h, _, o, err := e.build(eventType, data, opts)
	if err != nil {
		return err
	}
	return e.app.Publish(ctx, e.Subject(eventType), data, append([]nats.PubOption{nats.WithHeader(h)}, o.pub...)...)
}

// PublishJS sends data as a CloudEvent of eventType to JetStream. The event
// id is the Nats-Msg-Id, so the stream drops a repeated id within its
// Duplicates window.
func (e *Emitter) PublishJS(ctx context.Context, eventType string, data any, opts ...PublishOption) (*jetstream.PubAck, error) {
	h, id, o, err := e.build(eventType, data, opts)
	if err != nil {
		return nil, err
	}
	return e.app.PublishJS(ctx, e.Subject(eventType), data, append([]nats.PubOption{nats.WithHeader(h), nats.MsgID(id)}, o.pub...)...)
}

func (e *Emitter) build(eventType string, data any, opts []PublishOption) (nats.Header, string, publishOptions, error) {
	var o publishOptions
	for _, f := range opts {
		f(&o)
	}
	h, id, err := e.header(eventType, data, o)
	return h, id, o, err
}

// header builds the message header: the user's headers first, then the
// ce-* and Content-Type headers over them.
func (e *Emitter) header(eventType string, data any, o publishOptions) (nats.Header, string, error) {
	if eventType == "" {
		return nil, "", fmt.Errorf("nats: cloudevents: empty event type")
	}
	id := o.id
	if id == "" {
		id = newID()
	}
	t := o.time
	if t.IsZero() {
		t = time.Now()
	}
	ct := o.contentType
	if ct == "" {
		ct = "application/json"
		if _, ok := data.([]byte); ok {
			ct = "application/octet-stream"
		}
	}
	h := nats.Header{}
	for k, v := range o.header {
		h[k] = append([]string(nil), v...)
	}
	h["ce-specversion"] = []string{specVersion}
	h["ce-id"] = []string{id}
	h["ce-source"] = []string{e.source}
	h["ce-type"] = []string{eventType}
	h["ce-time"] = []string{t.UTC().Format(time.RFC3339Nano)}
	h[contentType] = []string{ct}
	if o.subject != "" {
		h["ce-subject"] = []string{o.subject}
	}
	if o.dataSchema != "" {
		h["ce-dataschema"] = []string{o.dataSchema}
	}
	for _, x := range o.extensions {
		if !extName.MatchString(x[0]) || standard[x[0]] {
			return nil, "", fmt.Errorf("nats: cloudevents: invalid extension name %q", x[0])
		}
		h[prefix+x[0]] = []string{x[1]}
	}
	return h, id, nil
}

// newID returns a random UUID v4.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// Handle subscribes fn to the events of eventType. The attributes are read
// from the message headers; a message with invalid attributes fails with
// nats.Terminal without calling fn, so the core logs it and, with a DLQ
// option, dead-letters it. CloudEvents are one-way: Handle never replies; use
// nats.Reply for request/reply.
// An empty eventType, or one that Subject maps to "", is a registration error
// that Start returns.
func Handle[T any](e *Emitter, eventType string, fn func(context.Context, Event, *nats.Msg[T]) error, opts ...nats.HandlerOption) {
	subject := ""
	if eventType != "" {
		subject = e.Subject(eventType)
	}
	nats.Subscribe(e.app, subject, func(ctx context.Context, m *nats.Msg[T]) error {
		ev, err := FromHeader(m.Header)
		if err != nil {
			return nats.Terminal(err)
		}
		return fn(ctx, ev, m)
	}, opts...)
}
