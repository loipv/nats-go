package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"
)

func requestCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestSubscribeRespond(t *testing.T) {
	a := newApp(t, runServer(t))
	a.Subscribe("math.double", Typed(func(_ context.Context, m *Msg[int]) error {
		return m.Respond(m.Data * 2)
	}))
	start(t, a)
	var out int
	if _, err := a.Request(requestCtx(t), "math.double", 21, &out); err != nil || out != 42 {
		t.Fatal(out, err)
	}
}

func TestSubscribeRespondError(t *testing.T) {
	a := newApp(t, runServer(t))
	a.Subscribe("check", Typed(func(_ context.Context, m *Msg[int]) error {
		if m.RespondError(nil) == nil {
			t.Error("RespondError(nil) must fail")
		}
		return m.RespondError(Terminal(errors.New("negative\nstack")))
	}))
	start(t, a)
	_, err := a.Request(requestCtx(t), "check", -1, nil)
	var se *ServiceError
	if !errors.As(err, &se) || se.Code != 400 || se.Description != "negative" {
		t.Fatal(err)
	}
}

func TestRespondWithoutReplySubject(t *testing.T) {
	a := newApp(t, runServer(t))
	errs := make(chan error, 1)
	a.Subscribe("fire", Typed(func(_ context.Context, m *Msg[int]) error {
		errs <- m.Respond(1)
		return nil
	}))
	start(t, a)
	if err := a.Publish(context.Background(), "fire", 1); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, errs); err == nil {
		t.Fatal("expected an error without a reply subject")
	}
}

func TestRespondRejectedOnJetStream(t *testing.T) {
	a := newApp(t, runServer(t))
	setupStream(t, a)
	errs := make(chan error, 2)
	a.Consume("ORDERS", "billing", Typed(func(_ context.Context, m *Msg[order]) error {
		errs <- m.Respond("x")
		errs <- m.RespondError(errors.New("x"))
		return nil
	}), WithManualAck())
	start(t, a)
	if _, err := a.PublishJS(context.Background(), "orders.x", order{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := recv(t, errs); !errors.Is(err, ErrNotCore) {
			t.Fatal(err)
		}
	}
	if n := pendingAcks(t, a, "billing"); n != 1 {
		t.Fatal("message must stay unacked, pending", n)
	}
}

func TestRespondRejectedInSubscribeReply(t *testing.T) {
	a := newApp(t, runServer(t))
	errs := make(chan error, 2)
	a.SubscribeReply("echo", TypedReply(func(_ context.Context, m *Msg[int]) (int, error) {
		errs <- m.Respond(0)
		errs <- m.RespondError(errors.New("x"))
		return m.Data, nil
	}))
	start(t, a)
	var out int
	if _, err := a.Request(requestCtx(t), "echo", 7, &out); err != nil || out != 7 {
		t.Fatal(out, err)
	}
	for range 2 {
		if err := recv(t, errs); !errors.Is(err, errReplyOwned) {
			t.Fatal(err)
		}
	}
}

func TestRespondOnHandBuiltMsg(t *testing.T) {
	m := &Msg[int]{Data: 1}
	if err := m.Respond(1); !errors.Is(err, natsio.ErrMsgNotBound) {
		t.Fatal(err)
	}
	if err := m.RespondError(errors.New("x")); !errors.Is(err, natsio.ErrMsgNotBound) {
		t.Fatal(err)
	}
}
