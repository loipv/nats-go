package nats

import (
	"context"
	"strings"
	"testing"
)

func TestMessageEncodesAndSetsJSHeaders(t *testing.T) {
	a, _ := New() // not started: Message needs no connection
	m, err := a.Message("orders.x", order{ID: "1"}, WithHeader(Header{"X-A": []string{"1"}}), WithMsgID("m-1"), WithExpectLastSeq(7), WithExpectStream("ORDERS"), WithExpectLastSubjectSeq(3))
	if err != nil {
		t.Fatal(err)
	}
	if string(m.Data) != `{"id":"1"}` || m.Subject != "orders.x" {
		t.Fatal(m.Subject, string(m.Data))
	}
	for k, v := range map[string]string{"X-A": "1", "Nats-Msg-Id": "m-1", "Nats-Expected-Last-Sequence": "7", "Nats-Expected-Stream": "ORDERS", "Nats-Expected-Last-Subject-Sequence": "3"} {
		if m.Header.Get(k) != v {
			t.Fatalf("%s=%q", k, m.Header.Get(k))
		}
	}
	if _, err := a.Message("x", make(chan int)); err == nil {
		t.Fatal("encode error expected")
	}
}

func TestPublishJSExpectHeaderEnforced(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	start(t, a)
	if _, err := a.PublishJS(context.Background(), "orders.x", order{ID: "1"}, WithExpectLastSeq(42)); err == nil {
		t.Fatal("wrong ExpectLastSeq must be rejected by the server")
	}
	ack1, _ := a.PublishJS(context.Background(), "orders.x", order{ID: "1"}, WithMsgID("d"))
	ack2, _ := a.PublishJS(context.Background(), "orders.x", order{ID: "1"}, WithMsgID("d"))
	if !ack2.Duplicate || ack2.Sequence != ack1.Sequence {
		t.Fatal(ack2)
	}
}

func TestWrapPublishAppliesMiddlewareInOrder(t *testing.T) {
	a, _ := New()
	var trace []string
	mk := func(n string) PublishMiddleware {
		return func(next PublishFunc) PublishFunc {
			return func(ctx context.Context, m *RawMsg) error {
				trace = append(trace, n)
				return next(ctx, m)
			}
		}
	}
	a.UsePublish(mk("p1"), mk("p2"))
	_ = a.WrapPublish(func(context.Context, *RawMsg) error { trace = append(trace, "send"); return nil })(context.Background(), &RawMsg{})
	if strings.Join(trace, ",") != "p1,p2,send" {
		t.Fatal(trace)
	}
}

func TestAttemptOutsideHandler(t *testing.T) {
	if Attempt(context.Background()) != 1 || Attempt(withAttempt(context.Background(), 3)) != 3 {
		t.Fatal()
	}
}
