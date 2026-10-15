package nats

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// BatchError reports which messages of a batch failed, by index into the
// slice passed to the batch handler. The other messages are acked, also when
// the *BatchError is wrapped (it is found with errors.As); a nil *BatchError
// returned directly acks them all.
type BatchError struct {
	Failed map[int]error
}

func (e *BatchError) Error() string {
	n := 0
	if e != nil {
		n = len(e.Failed)
	}
	return fmt.Sprintf("nats: %d of batch failed", n)
}

// ConsumeBatch registers h on an existing durable pull consumer (or one
// declared with App.Consumer), passing it up to WithBatchSize messages at a
// time. Messages that fail to decode or validate, or that were delivered
// more than WithMaxAttempts times, are settled without reaching h. With
// automatic ack a nil error acks the whole batch, a *BatchError settles
// only the listed messages as failed (also when found inside errors.Join or a
// wrapper), and any other error fails every message. With WithManualAck the
// handler settles each message itself and a returned error is only logged.
// Middleware does not apply to batch handlers.
func (a *App) ConsumeBatch(stream, consumer string, h BatchHandler, opts ...HandlerOption) {
	cfg, err := newHandlerConfig(kindBatch, opts)
	if err == nil && (stream == "" || consumer == "") {
		err = errors.New("nats: consume batch: stream and consumer are required")
	}
	if err == nil && h.reg == nil {
		err = fmt.Errorf("nats: consume batch %s/%s: nil handler", stream, consumer)
	}
	if err == nil && !a.claimConsumer(stream+"/"+consumer) {
		err = fmt.Errorf("nats: duplicate consumer %s/%s", stream, consumer)
	}
	if err != nil {
		a.addErr(err)
		return
	}
	a.register(h.reg(jsReg{kind: kindBatch, stream: stream, consumer: consumer, cfg: cfg}, a.opts.codec))
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
	log := a.sink(r.name(), r.kind)
	rn := &batchRunner{runnerState: newRunnerState(log), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(rn.done)
		r.loop(ctx, a, rn, log)
	}()
	return rn, nil
}

// loop fetches and handles batches one at a time until ctx is cancelled,
// restarting when the consumer is deleted.
func (r *batchReg[T]) loop(ctx context.Context, a *App, rn *batchRunner, log *sink) {
	rc := r.cfg.retry(r.kind)
	reconnect := func(ctx context.Context) error {
		if err := a.recoverConsumer(ctx, r.stream, r.consumer); err != nil {
			return err
		}
		return r.prepare(ctx, a)
	}
	for {
		if ctx.Err() != nil {
			return
		}
		msgs, err := r.fetch(ctx)
		if ctx.Err() != nil {
			// Stopping: hand the fetched messages back now rather than
			// after AckWait.
			for _, m := range msgs {
				if err := m.Nak(); err != nil {
					log.Debug("nats: nak on stop failed", "subject", m.Subject(), "error", err)
				}
			}
			return
		}
		if len(msgs) > 0 {
			r.handle(a, msgs, rc, log)
		}
		switch {
		case err == nil:
		case r.gone(ctx, a, err):
			rn.setRestarting(err)
			if !a.superviseLoop(ctx, reconnect, log) {
				return
			}
			rn.setRestarted()
		default:
			log.Warn("nats: fetch", "error", err)
			log.asyncErr("", fmt.Errorf("fetch: %w", err))
			select {
			case <-time.After(r.cfg.batchWait):
			case <-ctx.Done():
				return
			}
		}
	}
}

// fetch waits up to WithBatchWait for up to WithBatchSize messages.
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

// handle drops messages past WithMaxAttempts or failing to decode, calls fn on
// the rest and settles them by its result.
func (r *batchReg[T]) handle(a *App, msgs []jetstream.Msg, rc retryConfig, log *sink) {
	ctx := withSink(a.baseCtx, log)
	var ms []*Msg[T]
	var srcs []dlqSource
	for _, m := range msgs {
		raw, src := toRaw(m), r.source(m)
		if rc.exceeded(src.deliveries) {
			a.dispose(ctx, m, raw, errMaxAttempts, rc, src, log)
			continue
		}
		dm, err := decodeMsg[T](withJSMsg(ctx, m), r.codec, raw)
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
	// errors.As finds a non-nil *BatchError at any depth, so it settles only
	// its listed indexes and acks the rest. It also reports a typed-nil one as
	// be == nil, which would fail the whole batch; the check on the top-level
	// value below is what lets a directly returned typed-nil mean success.
	var be *BatchError
	errors.As(err, &be)
	if top, ok := err.(*BatchError); ok && top == nil {
		err = nil // a typed-nil *BatchError lists no failures
	}
	if r.cfg.manualAck {
		if err != nil {
			log.Error("nats: batch handler failed", "size", len(ms), "error", err)
		}
		return
	}
	if be != nil {
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
func (r *batchReg[T]) call(ctx context.Context, ms []*Msg[T], log *sink) (err error) {
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
	runnerState
	cancel context.CancelFunc
	// done is closed when the loop exits.
	done chan struct{}
}

// stop stops fetching, or restarting, and waits for the batch in flight or
// ctx expiry.
func (r *batchRunner) stop(ctx context.Context) error {
	r.setStopped()
	r.cancel()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
