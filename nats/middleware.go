package nats

import "context"

// Handler processes one raw message. Middleware wraps Handlers.
type Handler func(ctx context.Context, m *RawMsg) error

// Middleware wraps a Handler on the inbound path.
type Middleware func(Handler) Handler

// PublishFunc sends one message. PublishMiddleware wraps PublishFuncs.
type PublishFunc func(ctx context.Context, m *RawMsg) error

// PublishMiddleware wraps a PublishFunc on the outbound path.
type PublishMiddleware func(PublishFunc) PublishFunc
