package nats

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestConsumeTermCarriesReason(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	a.Consume("ORDERS", "billing", Typed(func(context.Context, *Msg[order]) error {
		return Terminal(errors.New("bad input\nsecret stack"))
	}))
	start(t, a)
	reasons := make(chan string, 1)
	sub, err := a.Conn().Subscribe("$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.ORDERS.billing", func(m *natsio.Msg) {
		var adv struct {
			Reason string `json:"reason"`
		}
		_ = json.Unmarshal(m.Data, &adv)
		reasons <- adv.Reason
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if _, err := a.PublishJS(context.Background(), "orders.x", order{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	if r := recv(t, reasons); r != "bad input" {
		t.Fatalf("reason = %q", r)
	}
}

func TestPublishJSWithJSPublishOpts(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	setupStream(t, a)
	start(t, a)
	ctx := context.Background()
	if _, err := a.PublishJS(ctx, "orders.x", order{ID: "1"}, WithMsgID("m-1")); err != nil {
		t.Fatal(err)
	}
	_, err := a.PublishJS(ctx, "orders.x", order{ID: "2"}, WithJSPublishOpts(jetstream.WithExpectLastMsgID("other")))
	if err == nil {
		t.Fatal("expected wrong last msg id to fail")
	}
	if _, err := a.PublishJS(ctx, "orders.x", order{ID: "2"}, WithJSPublishOpts(jetstream.WithExpectLastMsgID("m-1"))); err != nil {
		t.Fatal(err)
	}
}

func TestJSPublishOptsRejectedOutsidePublishJS(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	start(t, a)
	ctx := context.Background()
	opt := WithJSPublishOpts(jetstream.WithMsgID("x"))
	_, msgErr := a.Message("x", 1, opt)
	_, reqErr := a.Request(ctx, "x", 1, nil, opt)
	_, batchErr := a.PublishJSBatch(ctx, []BatchMsg{{Subject: "x", Data: 1, Opts: []PubOption{opt}}})
	for name, err := range map[string]error{
		"Publish": a.Publish(ctx, "x", 1, opt), "Request": reqErr, "Message": msgErr, "PublishJSBatch": batchErr,
	} {
		if err == nil || !strings.Contains(err.Error(), "WithJSPublishOpts") {
			t.Errorf("%s: %v", name, err)
		}
	}
}
