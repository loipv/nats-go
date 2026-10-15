package nats

import (
	"context"
	"strings"
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
