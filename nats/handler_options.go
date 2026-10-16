package nats

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/nats-io/nats.go/jetstream"
)

type handlerKind string

const (
	kindSubscribe handlerKind = "subscribe"
	kindReply     handlerKind = "reply"
	kindConsume   handlerKind = "consume"
	kindOrdered   handlerKind = "ordered"
	kindBatch     handlerKind = "batch"
	kindEndpoint  handlerKind = "endpoint"
)

// HandlerOption configures a handler registered with Subscribe, SubscribeReply,
// Consume, ConsumeOrdered, a batch consumer or Endpoint. Using an option on a
// handler kind it does not apply to is a registration error returned by Start.
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	queue       string
	concurrency int
	timeout     time.Duration
	mws         []Middleware
	manualAck   bool
	pullMax     int
	filters     []string
	deliver     *jetstream.DeliverPolicy
	// maxAttempts is 0 until WithMaxAttempts sets it; retry resolves the default.
	maxAttempts    int
	backoff        Backoff
	dlq            string
	autoInProgress bool
	batchSize      int
	batchWait      time.Duration
	// endpointSubject and endpointMetadata are passed to micro by Endpoint.
	endpointSubject  string
	endpointMetadata map[string]string
	used             []string
}

// allowedKinds returns the handler kinds a restricted option applies to,
// or nil when the option applies to every kind.
func allowedKinds(option string) []handlerKind {
	switch option {
	case "WithQueue":
		return []handlerKind{kindSubscribe, kindReply}
	case "WithConcurrency":
		return []handlerKind{kindSubscribe, kindReply, kindConsume, kindOrdered}
	case "WithMiddleware":
		return []handlerKind{kindSubscribe, kindReply, kindConsume, kindOrdered, kindEndpoint}
	case "WithManualAck":
		return []handlerKind{kindConsume, kindBatch}
	case "WithPullMaxMessages":
		return []handlerKind{kindConsume, kindOrdered}
	case "WithFilterSubjects", "WithDeliverPolicy":
		return []handlerKind{kindOrdered}
	case "WithAutoInProgress":
		return []handlerKind{kindConsume, kindBatch}
	case "WithBatchSize", "WithBatchWait":
		return []handlerKind{kindBatch}
	case "WithEndpointSubject", "WithEndpointMetadata":
		return []handlerKind{kindEndpoint}
	}
	return nil
}

func newHandlerConfig(kind handlerKind, opts []HandlerOption) (handlerConfig, error) {
	c := handlerConfig{concurrency: 1, batchSize: 100, batchWait: time.Second}
	for _, opt := range opts {
		opt(&c)
	}
	for _, name := range c.used {
		if kinds := allowedKinds(name); kinds != nil && !slices.Contains(kinds, kind) {
			return c, fmt.Errorf("nats: option %s not allowed for %s", name, kind)
		}
	}
	if slices.Contains(c.used, "WithMaxAttempts") && c.maxAttempts != Unlimited && c.maxAttempts < 1 {
		return c, fmt.Errorf("nats: WithMaxAttempts must be at least 1 or Unlimited, got %d", c.maxAttempts)
	}
	if slices.Contains(c.used, "WithBackoff") && c.backoff == nil {
		return c, errors.New("nats: WithBackoff requires a non-nil Backoff")
	}
	if slices.Contains(c.used, "WithDLQ") && c.dlq == "" {
		return c, errors.New("nats: WithDLQ requires a subject")
	}
	if c.dlq != "" && !validPublishSubject(c.dlq) {
		return c, fmt.Errorf("nats: WithDLQ: invalid subject %q (no empty token, whitespace, * or >)", c.dlq)
	}
	if slices.Contains(c.used, "WithPullMaxMessages") && c.pullMax < 1 {
		return c, fmt.Errorf("nats: WithPullMaxMessages must be at least 1, got %d", c.pullMax)
	}
	if c.timeout < 0 {
		return c, fmt.Errorf("nats: WithTimeout must not be negative, got %s", c.timeout)
	}
	if c.batchSize < 1 {
		return c, fmt.Errorf("nats: WithBatchSize must be at least 1, got %d", c.batchSize)
	}
	if c.batchWait <= 0 {
		return c, fmt.Errorf("nats: WithBatchWait must be positive, got %s", c.batchWait)
	}
	if c.manualAck {
		// The handler owns the ack, so the library has nothing to retry.
		for _, name := range []string{"WithMaxAttempts", "WithBackoff", "WithDLQ"} {
			if slices.Contains(c.used, name) {
				return c, fmt.Errorf("nats: option %s not allowed with WithManualAck", name)
			}
		}
	}
	if kind == kindOrdered && c.concurrency > 1 {
		return c, fmt.Errorf("nats: concurrency must be 1 for ordered consumers to keep stream order, got %d", c.concurrency)
	}
	if c.concurrency < 1 {
		return c, fmt.Errorf("nats: concurrency must be at least 1, got %d", c.concurrency)
	}
	return c, nil
}

// validPublishSubject reports whether s is a subject that can be published to:
// no empty token, no wildcard token and no whitespace.
func validPublishSubject(s string) bool {
	for _, tok := range strings.Split(s, ".") {
		if tok == "" || tok == "*" || tok == ">" || strings.ContainsFunc(tok, unicode.IsSpace) {
			return false
		}
	}
	return true
}

// WithQueue joins the subscription to a queue group, so each message is
// delivered to one member of the group.
func WithQueue(group string) HandlerOption {
	return func(c *handlerConfig) { c.queue = group; c.used = append(c.used, "WithQueue") }
}

// WithConcurrency runs up to n handler calls in parallel. Defaults to 1, which
// preserves message order.
func WithConcurrency(n int) HandlerOption {
	return func(c *handlerConfig) { c.concurrency = n; c.used = append(c.used, "WithConcurrency") }
}

// WithTimeout sets a deadline on the context passed to each handler call. 0
// means no timeout; a negative d is a registration error reported by Start.
func WithTimeout(d time.Duration) HandlerOption {
	return func(c *handlerConfig) { c.timeout = d; c.used = append(c.used, "WithTimeout") }
}

// WithMiddleware adds middleware to this handler only, inside the app middleware.
func WithMiddleware(mw ...Middleware) HandlerOption {
	return func(c *handlerConfig) { c.mws = append(c.mws, mw...); c.used = append(c.used, "WithMiddleware") }
}

// WithManualAck disables automatic ack: the handler acks with Msg.Ack and friends.
func WithManualAck() HandlerOption {
	return func(c *handlerConfig) { c.manualAck = true; c.used = append(c.used, "WithManualAck") }
}

// WithPullMaxMessages sets how many messages a pull consumer buffers. An n
// below 1 is a registration error, and using it on a push consumer makes Start
// fail.
func WithPullMaxMessages(n int) HandlerOption {
	return func(c *handlerConfig) { c.pullMax = n; c.used = append(c.used, "WithPullMaxMessages") }
}

// WithFilterSubjects limits an ordered consumer to the given subjects.
func WithFilterSubjects(subjects ...string) HandlerOption {
	return func(c *handlerConfig) { c.filters = subjects; c.used = append(c.used, "WithFilterSubjects") }
}

// WithDeliverPolicy sets where an ordered consumer starts in the stream.
func WithDeliverPolicy(p jetstream.DeliverPolicy) HandlerOption {
	return func(c *handlerConfig) { c.deliver = &p; c.used = append(c.used, "WithDeliverPolicy") }
}

// WithMaxAttempts limits how many times a message is delivered to the handler
// before it is given up on (sent to the DLQ if one is set). Use Unlimited to
// retry forever.
func WithMaxAttempts(n int) HandlerOption {
	return func(c *handlerConfig) { c.maxAttempts = n; c.used = append(c.used, "WithMaxAttempts") }
}

// WithBackoff sets the delay before a failed message is retried.
func WithBackoff(b Backoff) HandlerOption {
	return func(c *handlerConfig) { c.backoff = b; c.used = append(c.used, "WithBackoff") }
}

// WithDLQ publishes messages that exhausted their attempts, or failed
// terminally, to subject. Another handler may consume the DLQ subject (a DLQ
// processor); it must not dead-letter back to this handler's subject, or
// failures loop between the two. A subject that is not a valid NATS subject or that
// contains a wildcard token (* or >) is a registration error reported by Start,
// as is an empty one.
func WithDLQ(subject string) HandlerOption {
	return func(c *handlerConfig) { c.dlq = subject; c.used = append(c.used, "WithDLQ") }
}

// WithAutoInProgress keeps the ack wait timer of a message being handled from
// expiring by sending periodic in-progress signals.
func WithAutoInProgress() HandlerOption {
	return func(c *handlerConfig) { c.autoInProgress = true; c.used = append(c.used, "WithAutoInProgress") }
}

// WithBatchSize sets the maximum number of messages passed to a batch handler.
// Defaults to 100.
func WithBatchSize(n int) HandlerOption {
	return func(c *handlerConfig) { c.batchSize = n; c.used = append(c.used, "WithBatchSize") }
}

// WithBatchWait sets how long a batch handler waits to fill a batch. Defaults
// to one second.
func WithBatchWait(d time.Duration) HandlerOption {
	return func(c *handlerConfig) { c.batchWait = d; c.used = append(c.used, "WithBatchWait") }
}
