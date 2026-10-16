package nats

import (
	"encoding/json"
	"net/http"
	"slices"

	natsio "github.com/nats-io/nats.go"
)

// Report is the health of an App.
type Report struct {
	// Status is "ok", "degraded" (reconnecting, or a handler restarting or
	// stopped) or "down" (not started, shut down or connection closed).
	Status string `json:"status"`
	// Connection is the nats.go connection status, such as CONNECTED or RECONNECTING.
	Connection string          `json:"connection"`
	Ready      bool            `json:"ready"`
	Handlers   []HandlerStatus `json:"handlers"`
}

// Health reports the connection and handler state. It does not block while
// Start is connecting: it answers at once, with Ready false.
func (a *App) Health() Report {
	a.mu.Lock()
	nc, runners := a.nc(), slices.Clone(a.runners)
	if nc != nil && nc.IsClosed() && !a.started {
		nc = nil // left by a failed Start, maybe while a retry connects
	}
	a.mu.Unlock()

	r := Report{Connection: natsio.DISCONNECTED.String(), Ready: a.running(), Handlers: []HandlerStatus{}}
	if nc != nil {
		r.Connection = nc.Status().String()
	}
	notRunning := false
	for _, rn := range runners {
		// A service runner reports one status per endpoint.
		var sts []HandlerStatus
		if m, ok := rn.(interface{ statuses() []HandlerStatus }); ok {
			sts = m.statuses()
		} else {
			sts = []HandlerStatus{rn.status()}
		}
		for _, st := range sts {
			notRunning = notRunning || st.State != "running"
			r.Handlers = append(r.Handlers, st)
		}
	}
	switch {
	case !r.Ready || r.Connection == natsio.CLOSED.String():
		r.Status = "down"
	case r.Connection != natsio.CONNECTED.String() || notRunning:
		r.Status = "degraded"
	default:
		r.Status = "ok"
	}
	return r
}

// HealthHandler serves Health as JSON: 200 when the status is "ok",
// 503 otherwise. Mount it on your own HTTP server.
func (a *App) HealthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		r := a.Health()
		w.Header().Set("Content-Type", "application/json")
		if r.Status != "ok" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(r)
	})
}
