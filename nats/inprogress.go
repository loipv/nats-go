package nats

import (
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// defaultAckWait is the server default, used when the consumer reports none.
const defaultAckWait = 30 * time.Second

// keepAlive sends InProgress for every msg each interval until stop is
// called, resetting the server's AckWait timer while a handler runs. stop
// waits for the goroutine to exit.
func keepAlive(msgs []jetstream.Msg, interval time.Duration, log *slog.Logger) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				for _, m := range msgs {
					if err := m.InProgress(); err != nil {
						log.Debug("nats: in-progress failed", "error", err)
					}
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		wg.Wait()
	}
}
