package nats

import (
	"testing"
	"time"
)

func TestExponentialBackoffBounds(t *testing.T) {
	b := ExponentialBackoff(100*time.Millisecond, time.Second)
	for range 200 {
		if d := b(1); d < 80*time.Millisecond || d > 120*time.Millisecond {
			t.Fatal("attempt 1", d)
		}
		if d := b(3); d < 320*time.Millisecond || d > 480*time.Millisecond {
			t.Fatal("attempt 3", d)
		}
		if d := b(1000); d <= 0 || d > time.Second {
			t.Fatal("attempt 1000", d)
		}
		if d := b(0); d < 80*time.Millisecond || d > 120*time.Millisecond {
			t.Fatal("attempt 0", d)
		}
	}
}

func TestExponentialBackoffPanicsOnInvalid(t *testing.T) {
	for _, c := range [][2]time.Duration{{0, time.Second}, {time.Second, time.Millisecond}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("want panic", c)
				}
			}()
			ExponentialBackoff(c[0], c[1])
		}()
	}
}

func TestRetryOptionRules(t *testing.T) {
	bad := []struct {
		kind handlerKind
		opts []HandlerOption
	}{
		{kindSubscribe, []HandlerOption{MaxAttempts(0)}},
		{kindSubscribe, []HandlerOption{WithBackoff(nil)}},
		{kindSubscribe, []HandlerOption{DLQ("")}},
		{kindConsume, []HandlerOption{ManualAck(), MaxAttempts(3)}},
		{kindConsume, []HandlerOption{BatchSize(10)}},
		{kindBatch, []HandlerOption{Concurrency(2)}},
		{kindBatch, []HandlerOption{With(Recover())}},
		{kindBatch, []HandlerOption{BatchSize(0)}},
		{kindBatch, []HandlerOption{BatchWait(0)}},
		{kindSubscribe, []HandlerOption{AutoInProgress()}},
	}
	for _, c := range bad {
		if _, err := newHandlerConfig(c.kind, c.opts); err == nil {
			t.Fatalf("%s %d opts: want error", c.kind, len(c.opts))
		}
	}
	for _, kind := range []handlerKind{kindSubscribe, kindReply, kindConsume, kindOrdered, kindBatch} {
		if _, err := newHandlerConfig(kind, []HandlerOption{MaxAttempts(Unlimited), DLQ("dlq.x"), WithBackoff(defaultBackoff())}); err != nil {
			t.Fatal(kind, err)
		}
	}
	c, _ := newHandlerConfig(kindBatch, nil)
	if c.batchSize != 100 || c.batchWait != time.Second {
		t.Fatal(c.batchSize, c.batchWait)
	}
}

func TestRetryDefaultsByKind(t *testing.T) {
	for kind, want := range map[handlerKind]int{kindConsume: Unlimited, kindBatch: Unlimited, kindSubscribe: 1, kindReply: 1, kindOrdered: 1} {
		c, _ := newHandlerConfig(kind, nil)
		if r := c.retry(kind); r.maxAttempts != want || r.backoff == nil {
			t.Fatal(kind, r.maxAttempts)
		}
	}
	r := retryConfig{maxAttempts: 3}
	if r.exhausted(2) || !r.exhausted(3) || r.exceeded(3) || !r.exceeded(4) {
		t.Fatal("exhausted/exceeded")
	}
	if (retryConfig{maxAttempts: Unlimited}).exhausted(1 << 30) {
		t.Fatal("unlimited never exhausted")
	}
}
