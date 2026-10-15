package nats

import (
	"log/slog"
	"strings"
	"testing"
)

func TestEventOptionRejectsNil(t *testing.T) {
	cases := map[string]Option{
		"WithOnRetry":             WithOnRetry(nil),
		"WithOnFailed":            WithOnFailed(nil),
		"WithOnDeadLettered":      WithOnDeadLettered(nil),
		"WithOnDeadLetterFailed":  WithOnDeadLetterFailed(nil),
		"WithOnHandlerRestarting": WithOnHandlerRestarting(nil),
		"WithOnHandlerRestarted":  WithOnHandlerRestarted(nil),
		"WithOnHandlerStopped":    WithOnHandlerStopped(nil),
		"WithOnDisconnected":      WithOnDisconnected(nil),
		"WithOnReconnected":       WithOnReconnected(nil),
		"WithOnClosed":            WithOnClosed(nil),
		"WithOnAsyncError":        WithOnAsyncError(nil),
	}
	for name, opt := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := New(opt)
			if err == nil || !strings.Contains(err.Error(), name+": nil callback") {
				t.Fatalf("err = %v, want %q: nil callback", err, name)
			}
		})
	}
}

func TestFireOrderAndPanic(t *testing.T) {
	var buf syncBuf
	log := slog.New(slog.NewTextHandler(&buf, nil))
	var got []int
	fns := []func(MsgEvent){
		func(MsgEvent) { panic("boom") },
		func(MsgEvent) { got = append(got, 1) },
		func(MsgEvent) { got = append(got, 2) },
	}
	fire(log, fns, MsgEvent{Subject: "s"})
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("got %v, want [1 2]", got)
	}
	if !strings.Contains(buf.String(), "nats: event callback panicked") {
		t.Fatalf("log = %q, want panic message", buf.String())
	}
}

func TestEventOptionsAppend(t *testing.T) {
	a, err := New(WithOnClosed(func() {}), WithOnClosed(func() {}))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(a.opts.events.closed); n != 2 {
		t.Fatalf("len(closed) = %d, want 2", n)
	}
}
