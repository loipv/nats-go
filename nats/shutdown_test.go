package nats

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestShutdownWaitsForInflight(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	started, finished := make(chan struct{}), make(chan struct{})
	Subscribe(a, "slow", func(context.Context, *Msg[[]byte]) error {
		close(started)
		time.Sleep(200 * time.Millisecond)
		close(finished)
		return nil
	})
	start(t, a)
	_ = a.Conn().Publish("slow", nil)
	<-started
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("shutdown returned before handler finished")
	}
}

func TestShutdownDeadlineExceeded(t *testing.T) {
	s := runServer(t)
	a := newApp(t, s)
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	Subscribe(a, "stuck", func(context.Context, *Msg[[]byte]) error {
		close(started)
		<-release
		return nil
	})
	start(t, a)
	_ = a.Conn().Publish("stuck", nil)
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	begin := time.Now()
	if err := a.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if time.Since(begin) > time.Second || !a.Conn().IsClosed() {
		t.Fatal("must return promptly and close conn")
	}
}
