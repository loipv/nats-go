package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// runnerState is the health of a JetStream runner: running, restarting or
// stopped.
type runnerState struct {
	name string
	kind handlerKind

	mu       sync.Mutex
	state    string
	restarts int
	lastErr  error
}

func newRunnerState(name string, kind handlerKind) runnerState {
	return runnerState{name: name, kind: kind, state: "running"}
}

// setRestarting records err as the reason for a restart, unless stopped.
func (s *runnerState) setRestarting(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != "stopped" {
		s.state, s.lastErr = "restarting", err
	}
}

// setRestarted marks a successful restart, unless stopped meanwhile.
func (s *runnerState) setRestarted() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == "restarting" {
		s.state = "running"
		s.restarts++
	}
}

func (s *runnerState) setStopped() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = "stopped"
}

func (s *runnerState) status() HandlerStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := HandlerStatus{Name: s.name, Kind: string(s.kind), State: s.state, Restarts: s.restarts}
	if s.lastErr != nil {
		st.LastError = s.lastErr.Error()
	}
	return st
}

// mayBeGone reports errors that can mean the durable consumer was deleted.
// The server says so only to a waiting pull request; a later pull gets no
// responders and a push consumer just misses its heartbeats.
func mayBeGone(err error) bool {
	return errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound) ||
		errors.Is(err, natsio.ErrNoResponders) || errors.Is(err, jetstream.ErrNoHeartbeat)
}

// gone reports whether err means the durable consumer was deleted,
// confirming the ambiguous errors with a lookup.
func (r *jsReg) gone(ctx context.Context, a *App, err error) bool {
	if errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound) {
		return true
	}
	if !mayBeGone(err) {
		return false
	}
	_, err = a.js.Consumer(ctx, r.stream, r.consumer)
	return errors.Is(err, jetstream.ErrConsumerNotFound)
}

// recoverConsumer recreates stream and consumer if they were declared with
// App.Stream and App.Consumer. Undeclared ones are left for someone else to
// recreate.
func (a *App) recoverConsumer(ctx context.Context, stream, consumer string) error {
	for _, cfg := range a.streams {
		if cfg.Name == stream {
			if _, err := a.js.CreateOrUpdateStream(ctx, cfg); err != nil {
				return fmt.Errorf("nats: sync stream %q: %w", cfg.Name, err)
			}
			break
		}
	}
	for _, d := range a.consumers {
		if d.stream == stream && d.name() == consumer {
			return a.syncConsumer(ctx, d)
		}
	}
	return nil
}

// superviseLoop calls reconnect after each restartBackoff delay until it
// succeeds (true) or stopping is closed (false).
func (a *App) superviseLoop(ctx context.Context, stopping <-chan struct{}, reconnect func(ctx context.Context) error, log *slog.Logger) bool {
	for k := 1; ; k++ {
		select {
		case <-time.After(a.opts.restartBackoff(k)):
		case <-stopping:
			return false
		}
		err := reconnect(ctx)
		if err == nil {
			return true
		}
		log.Warn("nats: restart consumer", "attempt", k, "error", err)
	}
}
