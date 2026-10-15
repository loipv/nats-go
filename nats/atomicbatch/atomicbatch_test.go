package atomicbatch_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/loipv/nats-go/nats"
	"github.com/loipv/nats-go/nats/atomicbatch"
	"github.com/loipv/nats-go/nats/internal/testutil"
)

func startApp(t *testing.T, url string, setup ...func(*nats.App)) *nats.App {
	t.Helper()
	a := testutil.NewApp(t, url, setup...)
	testutil.Start(t, a)
	return a
}

func atomicStream(name, subj string, allow bool) func(*nats.App) {
	return func(a *nats.App) {
		a.Stream(jetstream.StreamConfig{Name: name, Subjects: []string{subj}, AllowAtomicPublish: allow})
	}
}

func streamMsgs(t *testing.T, a *nats.App, name string) uint64 {
	t.Helper()
	s, err := a.JetStream().Stream(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

func streamMsgHeader(t *testing.T, a *nats.App, name string, seq uint64, key string) string {
	t.Helper()
	s, err := a.JetStream().Stream(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.GetMsg(context.Background(), seq)
	if err != nil {
		t.Fatal(err)
	}
	return m.Header.Get(key)
}

func TestPublishAtomic(t *testing.T) {
	s := testutil.RunServer(t)
	a := startApp(t, s.ClientURL(), atomicStream("ATOMIC", "atomic.>", true))
	msgs := []nats.BatchMsg{{Subject: "atomic.a", Data: 1}, {Subject: "atomic.b", Data: 2}, {Subject: "atomic.c", Data: 3}}
	ack, err := atomicbatch.Publish(context.Background(), a, msgs)
	if err != nil || ack.BatchSize != 3 {
		t.Fatal(ack, err)
	}
	if streamMsgs(t, a, "ATOMIC") != 3 {
		t.Fatal()
	}
}

func TestPublishAtomicRejectedWithoutAllow(t *testing.T) {
	s := testutil.RunServer(t)
	a := startApp(t, s.ClientURL(), atomicStream("PLAIN", "plain.>", false))
	if _, err := atomicbatch.Publish(context.Background(), a, []nats.BatchMsg{{Subject: "plain.a", Data: 1}, {Subject: "plain.b", Data: 2}}); err == nil {
		t.Fatal("want error")
	}
	if streamMsgs(t, a, "PLAIN") != 0 {
		t.Fatal("nothing may be stored")
	}
}

func TestPublishAtomicSubjectOutsideStream(t *testing.T) {
	s := testutil.RunServer(t)
	a := startApp(t, s.ClientURL(), atomicStream("ATOMIC", "atomic.>", true))
	if _, err := atomicbatch.Publish(context.Background(), a, []nats.BatchMsg{{Subject: "atomic.a", Data: 1}, {Subject: "elsewhere.b", Data: 2}}); err == nil {
		t.Fatal("want error")
	}
	if streamMsgs(t, a, "ATOMIC") != 0 {
		t.Fatal("nothing may be stored")
	}
}

func TestPublishAtomicAppliesMiddleware(t *testing.T) {
	s := testutil.RunServer(t)
	a := startApp(t, s.ClientURL(), atomicStream("ATOMIC", "atomic.>", true), func(a *nats.App) {
		a.UsePublish(func(next nats.PublishFunc) nats.PublishFunc {
			return func(ctx context.Context, m *nats.RawMsg) error {
				if m.Header == nil {
					m.Header = nats.Header{}
				}
				m.Header.Set("X-Trace", "t")
				return next(ctx, m)
			}
		})
	})
	if _, err := atomicbatch.Publish(context.Background(), a, []nats.BatchMsg{{Subject: "atomic.a", Data: 1}, {Subject: "atomic.b", Data: 2}}); err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 2; seq++ {
		if streamMsgHeader(t, a, "ATOMIC", seq, "X-Trace") != "t" {
			t.Fatal(seq)
		}
	}
}

func TestPublishAtomicEmptyAndNotRunning(t *testing.T) {
	a, _ := nats.New()
	if _, err := atomicbatch.Publish(context.Background(), a, []nats.BatchMsg{{Subject: "x", Data: 1}}); !errors.Is(err, nats.ErrNotRunning) {
		t.Fatal(err)
	}
	s := testutil.RunServer(t)
	b := startApp(t, s.ClientURL())
	if _, err := atomicbatch.Publish(context.Background(), b, nil); err == nil {
		t.Fatal("empty batch must fail")
	}
}

func TestPublishAtomicSingleMessageNoHeaders(t *testing.T) {
	s := testutil.RunServer(t)
	a := startApp(t, s.ClientURL(), atomicStream("ATOMIC", "atomic.>", true))
	ack, err := atomicbatch.Publish(context.Background(), a, []nats.BatchMsg{{Subject: "atomic.a", Data: 1}})
	if err != nil || ack.BatchSize != 1 {
		t.Fatal(ack, err)
	}
}
