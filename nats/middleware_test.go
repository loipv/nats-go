package nats

import (
	"bytes"
	"context"
	"errors"
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
	if out := buf.String(); !strings.Contains(out, "level=DEBUG") || strings.Contains(out, "level=ERROR") || strings.Contains(out, "nats: handled") {
		t.Fatal(out)
	}
}

// recorded is one log record and the context it was logged with.
type recorded struct {
	ctx context.Context
	rec slog.Record
}

// recorder is a slog.Handler that keeps every record at every level.
type recorder struct {
	mu   sync.Mutex
	recs []recorded
}

func (r *recorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *recorder) WithAttrs([]slog.Attr) slog.Handler       { return r }
func (r *recorder) WithGroup(string) slog.Handler            { return r }

func (r *recorder) Handle(ctx context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, recorded{ctx: ctx, rec: rec.Clone()})
	return nil
}

func (r *recorder) all() []recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recorded(nil), r.recs...)
}

func attrsOf(rec slog.Record) map[string]slog.Value {
	m := map[string]slog.Value{}
	rec.Attrs(func(a slog.Attr) bool { m[a.Key] = a.Value; return true })
	return m
}

type markerKey struct{}

func TestLoggerLogsHandledMessage(t *testing.T) {
	var r recorder
	h := Logger(slog.New(&r))(func(context.Context, *RawMsg) error { return nil })
	ctx := context.WithValue(context.Background(), markerKey{}, "x")
	if err := h(ctx, &RawMsg{Subject: "orders.1"}); err != nil {
		t.Fatal(err)
	}
	recs := r.all()
	if len(recs) != 1 {
		t.Fatalf("%d records, want 1", len(recs))
	}
	got, attrs := recs[0], attrsOf(recs[0].rec)
	if got.rec.Level != slog.LevelDebug || got.rec.Message != "nats: handled" {
		t.Fatalf("record = %v %q, want DEBUG nats: handled", got.rec.Level, got.rec.Message)
	}
	if attrs["subject"].String() != "orders.1" || attrs["duration"].Kind() != slog.KindDuration {
		t.Fatalf("attrs = %v, want subject and duration", attrs)
	}
	if _, ok := attrs["error"]; ok {
		t.Fatal("a handled message must not carry an error attribute")
	}
	if got.ctx.Value(markerKey{}) != "x" {
		t.Fatal("the record must be logged with the handler's context")
	}
}

func TestLoggerLogsFailureAtDebug(t *testing.T) {
	var r recorder
	boom := errors.New("boom")
	h := Logger(slog.New(&r))(func(context.Context, *RawMsg) error { return boom })
	if err := h(context.Background(), &RawMsg{Subject: "orders.1"}); err != boom {
		t.Fatalf("returned %v, want the handler error", err)
	}
	recs := r.all()
	if len(recs) != 1 {
		t.Fatalf("%d records, want 1 (the App logs the failure at Error itself)", len(recs))
	}
	got, attrs := recs[0], attrsOf(recs[0].rec)
	if got.rec.Level != slog.LevelDebug || got.rec.Message != "nats: handled" {
		t.Fatalf("record = %v %q, want DEBUG nats: handled", got.rec.Level, got.rec.Message)
	}
	if e, ok := attrs["error"].Any().(error); !ok || e != boom {
		t.Fatalf("error attribute = %v, want the handler error", attrs["error"])
	}
	if attrs["duration"].Kind() != slog.KindDuration {
		t.Fatal("a failure must carry the duration too")
	}
}
