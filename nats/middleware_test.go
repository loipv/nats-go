package nats

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

func TestChainOrder(t *testing.T) {
	var trace []string
	mw := func(name string) Middleware {
		return func(next Handler) Handler {
			return func(ctx context.Context, m *RawMsg) error {
				trace = append(trace, name)
				return next(ctx, m)
			}
		}
	}
	a, _ := New()
	a.Use(mw("app1"), mw("app2"))
	c, _ := newHandlerConfig(kindSubscribe, []HandlerOption{WithMiddleware(mw("h1"))})
	_ = a.chain(c, func(context.Context, *RawMsg) error { trace = append(trace, "fn"); return nil })(context.Background(), &RawMsg{})
	if strings.Join(trace, ",") != "app1,app2,h1,fn" {
		t.Fatal(trace)
	}
}

func TestRecover(t *testing.T) {
	err := Recover()(func(context.Context, *RawMsg) error { panic("boom") })(context.Background(), &RawMsg{})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatal(err)
	}
}

// lockedBuffer is a bytes.Buffer safe to read while a worker writes to it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestLoggerDuplicateAtDebug(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	var buf lockedBuffer
	l := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	done := make(chan struct{})
	a.Subscribe("dup", Typed(func(context.Context, *Msg[int]) error { defer close(done); return Terminal(ErrDuplicate) }), WithMiddleware(Logger(l)))
	start(t, a)
	_ = a.Publish(context.Background(), "dup", 1)
	recv(t, done)
	waitFor(t, func() bool { return strings.Contains(buf.String(), "nats: duplicate skipped") })
	if out := buf.String(); !strings.Contains(out, "level=DEBUG") || strings.Contains(out, "level=ERROR") {
		t.Fatal(out)
	}
}
