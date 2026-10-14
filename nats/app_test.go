package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
)

func TestNewValidatesOptions(t *testing.T) {
	if _, err := New(WithURL("")); err == nil {
		t.Fatal("empty url must fail")
	}
	if _, err := New(WithShutdownTimeout(0)); err == nil {
		t.Fatal("zero timeout must fail")
	}
}

func TestStartFailsFastWhenServerDown(t *testing.T) {
	a, _ := New(WithURL("nats://127.0.0.1:1"))
	begin := time.Now()
	if err := a.Start(context.Background()); err == nil {
		t.Fatal("want connect error")
	}
	if time.Since(begin) > 5*time.Second {
		t.Fatal("start must not retry")
	}
}

func TestStartTwice(t *testing.T) {
	a := newApp(t, runServer(t))
	start(t, a)
	if err := a.Start(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatal(err)
	}
}

func TestShutdownBeforeStartIsNoop(t *testing.T) {
	a, _ := New()
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRunReturnsNilOnCancel(t *testing.T) {
	a := newApp(t, runServer(t))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	waitFor(t, a.running)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !a.Conn().IsClosed() {
		t.Fatal("conn must be closed after Run")
	}
}

func TestStartReturnsJoinedRegistrationErrors(t *testing.T) {
	a := newApp(t, runServer(t))
	e1, e2 := errors.New("e1"), errors.New("e2")
	a.addErr(e1)
	a.addErr(e2)
	err := a.Start(context.Background())
	if !errors.Is(err, e1) || !errors.Is(err, e2) {
		t.Fatal(err)
	}
}

func TestTokenAuth(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.Authorization = "s3cret"
	s := natstest.RunServer(&opts)
	t.Cleanup(s.Shutdown)
	if err := newApp(t, s, Token("wrong")).Start(context.Background()); err == nil {
		t.Fatal("wrong token must fail")
	}
	a := newApp(t, s, Token("s3cret"))
	start(t, a)
}
