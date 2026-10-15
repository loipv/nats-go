package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// BatchError reports which messages of a batch failed, by index into the
// slice passed to the batch handler. The other messages are acked.
type BatchError struct {
	Failed map[int]error
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("nats: %d of batch failed", len(e.Failed))
}

// ConsumeBatch registers fn on an existing durable pull consumer (or one
// declared with App.Consumer), passing it up to BatchSize messages at a
// time. Messages that fail to decode or validate, or that were delivered
// more than MaxAttempts times, are settled without reaching fn. With
// automatic ack a nil error acks the whole batch, a *BatchError settles
// only the listed messages as failed, and any other error fails every
// message. Middleware does not apply to batch handlers.
func ConsumeBatch[T any](app *App, stream, consumer string, fn func(context.Context, []*Msg[T]) error, opts ...HandlerOption) {
	cfg, err := newHandlerConfig(kindBatch, opts)
	if err == nil && (stream == "" || consumer == "") {
		err = errors.New("nats: consume batch: stream and consumer are required")
	}
	if err == nil && !app.claimConsumer(stream+"/"+consumer) {
		err = fmt.Errorf("nats: duplicate consumer %s/%s", stream, consumer)
	}
	if err != nil {
		app.addErr(err)
		return
	}
	app.register(&batchReg[T]{jsReg: jsReg{kind: kindBatch, stream: stream, consumer: consumer, cfg: cfg}, codec: app.opts.codec, fn: fn})
}

// batchReg reuses jsReg to resolve and check the consumer.
type batchReg[T any] struct {
	jsReg
	codec Codec
	fn    func(context.Context, []*Msg[T]) error
}

func (r *batchReg[T]) prepare(ctx context.Context, a *App) error {
	err := r.jsReg.prepare(ctx, a)
	if r.push != nil {
		return fmt.Errorf("nats: consume batch %s: push consumers are not supported", r.name())
	}
	return err
}

func (r *batchReg[T]) start(_ context.Context, a *App) (runner, error) {
	// The loop context only stops fetching; handlers get one derived from
	// baseCtx, which lives until the Shutdown deadline.
	ctx, cancel := context.WithCancel(a.baseCtx)
	rn := &batchRunner{name: r.name(), cancel: cancel, done: make(chan struct{})}
	log := a.opts.logger.With("handler", rn.name, "kind", r.kind)
	go func() {
		defer close(rn.done)
		r.loop(ctx, a, rn, log)
	}()
	return rn, nil
}

// loop fetches and handles batches one at a time until ctx is cancelled or
// the consumer is gone.
func (r *batchReg[T]) loop(ctx context.Context, a *App, rn *batchRunner, log *slog.Logger) {
	rc := r.cfg.retry(r.kind)
	for {
		msgs, err := r.fetch(ctx)
		if ctx.Err() != nil {
			// Stopping: the server redelivers fetched messages after AckWait.
			return
		}
		if len(msgs) > 0 {
			r.handle(a, msgs, rc, log)
		}
		switch {
		case err == nil:
		case errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound):
			rn.fail(err)
			return
		default:
			log.Warn("nats: fetch", "error", err)
			select {
			case <-time.After(r.cfg.batchWait):
			case <-ctx.Done():
				return
			}
		}
	}
}

// fetch waits up to BatchWait for up to BatchSize messages.
func (r *batchReg[T]) fetch(ctx context.Context) ([]jetstream.Msg, error) {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.batchWait)
	defer cancel()
	b, err := r.pull.Fetch(r.cfg.batchSize, jetstream.FetchContext(ctx))
	if err != nil {
		return nil, err
	}
	var msgs []jetstream.Msg
	for m := range b.Messages() {
		msgs = append(msgs, m)
	}
	if err := b.Error(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return msgs, err
	}
	return msgs, nil
}

// handle drops messages past MaxAttempts or failing to decode, calls fn on
// the rest and settles them by its result.
func (r *batchReg[T]) handle(a *App, msgs []jetstream.Msg, rc retryConfig, log *slog.Logger) {
	ctx := withLogger(a.baseCtx, log)
	var ms []*Msg[T]
	var srcs []dlqSource
	for _, m := range msgs {
		raw, src := toRaw(m), r.source(m)
		if rc.exceeded(src.deliveries) {
			a.dispose(ctx, m, raw, errMaxAttempts, rc, src, log)
			continue
		}
		dm, err := decodeMsg[T](r.codec, withJSMsg(ctx, m), raw)
		if err != nil {
			a.dispose(ctx, m, raw, err, rc, src, log)
			continue
		}
		ms, srcs = append(ms, dm), append(srcs, src)
	}
	if len(ms) == 0 {
		return
	}

	err := r.call(ctx, ms, log)
	if r.cfg.manualAck {
		if err != nil {
			log.Error("nats: batch handler failed", "size", len(ms), "error", err)
		}
		return
	}
	var be *BatchError
	if errors.As(err, &be) {
		for i := range be.Failed {
			if i < 0 || i >= len(ms) {
				log.Warn("nats: batch error index out of range", "index", i, "size", len(ms))
			}
		}
	}
	for i, m := range ms {
		merr := err
		if be != nil {
			merr = be.Failed[i]
		}
		a.dispose(ctx, m.js, m.Raw, merr, rc, srcs[i], log)
	}
}

// call runs fn with the handler timeout, extending AckWait meanwhile if
// asked to, and turns a panic into an error that fails the whole batch.
func (r *batchReg[T]) call(ctx context.Context, ms []*Msg[T], log *slog.Logger) (err error) {
	if r.cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.cfg.timeout)
		defer cancel()
	}
	if r.cfg.autoInProgress {
		js := make([]jetstream.Msg, len(ms))
		for i, m := range ms {
			js[i] = m.js
		}
		defer keepAlive(js, r.ackWait/2, log)()
	}
	defer func() {
		if p := recover(); p != nil {
			log.Error("nats: handler panicked", "size", len(ms), "panic", p, "stack", string(debug.Stack()))
			err = fmt.Errorf("nats: panic: %v", p)
		}
	}()
	return r.fn(ctx, ms)
}

type batchRunner struct {
	name   string
	cancel context.CancelFunc
	// done is closed when the loop exits.
	done chan struct{}

	mu      sync.Mutex
	stopped bool
	lastErr error
}

func (r *batchRunner) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped, r.lastErr = true, err
}

// stop stops fetching and waits for the batch in flight or ctx expiry.
func (r *batchRunner) stop(ctx context.Context) error {
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()
	r.cancel()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *batchRunner) status() HandlerStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := HandlerStatus{Name: r.name, Kind: string(kindBatch), State: "running"}
	if r.stopped {
		st.State = "stopped"
	}
	if r.lastErr != nil {
		st.LastError = r.lastErr.Error()
	}
	return st
}
