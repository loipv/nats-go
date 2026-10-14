package nats

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type fakeJSMsg struct {
	jetstream.Msg
	called string
}

func (f *fakeJSMsg) Ack() error  { f.called = "ack"; return nil }
func (f *fakeJSMsg) Nak() error  { f.called = "nak"; return nil }
func (f *fakeJSMsg) Term() error { f.called = "term"; return nil }

func TestSettle(t *testing.T) {
	cases := []struct {
		err    error
		manual bool
		want   string
	}{
		{nil, false, "ack"},
		{errors.New("x"), false, "nak"},
		{Terminal(errors.New("x")), false, "term"},
		{errors.New("x"), true, ""},
	}
	for _, c := range cases {
		f := &fakeJSMsg{}
		_ = settle(f, c.err, c.manual)
		if f.called != c.want {
			t.Fatalf("%v manual=%v: got %q want %q", c.err, c.manual, f.called, c.want)
		}
	}
}

type order struct {
	ID string `json:"id"`
}

func (o order) Validate() error {
	if o.ID == "" {
		return errors.New("id required")
	}
	return nil
}

func TestTypedDecodeAndValidate(t *testing.T) {
	var got order
	h := typed(JSONCodec{}, func(_ context.Context, m *Msg[order]) error { got = m.Data; return nil })
	if err := h(context.Background(), &RawMsg{Subject: "o", Data: []byte(`{"id":"1"}`)}); err != nil || got.ID != "1" {
		t.Fatal(err, got)
	}
	if err := h(context.Background(), &RawMsg{Data: []byte(`{`)}); !IsTerminal(err) {
		t.Fatal("decode error must be terminal", err)
	}
	if err := h(context.Background(), &RawMsg{Data: []byte(`{}`)}); !IsTerminal(err) {
		t.Fatal("validate error must be terminal", err)
	}
}

func TestMsgAckOnCoreReturnsErrNotJetStream(t *testing.T) {
	if err := (&Msg[[]byte]{}).Ack(); !errors.Is(err, ErrNotJetStream) {
		t.Fatal(err)
	}
}

func TestHandlerOptionRules(t *testing.T) {
	if _, err := newHandlerConfig(kindOrdered, []HandlerOption{ManualAck()}); err == nil {
		t.Fatal("ManualAck not allowed on ordered")
	}
	if _, err := newHandlerConfig(kindConsume, []HandlerOption{Queue("q")}); err == nil {
		t.Fatal("Queue not allowed on consume")
	}
	if _, err := newHandlerConfig(kindSubscribe, []HandlerOption{Concurrency(0)}); err == nil {
		t.Fatal("Concurrency(0) invalid")
	}
	c, err := newHandlerConfig(kindSubscribe, nil)
	if err != nil || c.concurrency != 1 {
		t.Fatal(c, err)
	}
}

func TestPoolConcurrency(t *testing.T) {
	var cur, peak atomic.Int32
	p := newPool(3, func(context.Context, *RawMsg) {
		n := cur.Add(1)
		for {
			o := peak.Load()
			if n <= o || peak.CompareAndSwap(o, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		cur.Add(-1)
	})
	for range 9 {
		p.dispatch(context.Background(), &RawMsg{})
	}
	_ = p.close(context.Background())
	if peak.Load() != 3 {
		t.Fatal("peak", peak.Load())
	}
}
