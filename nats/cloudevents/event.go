package cloudevents

import (
	"fmt"
	"strings"
	"time"

	"github.com/loipv/nats-go/nats"
)

const (
	specVersion = "1.0"
	prefix      = "ce-"
	contentType = "Content-Type"
)

// Event holds the CloudEvents context attributes of a received message.
type Event struct {
	ID, Source, Type, Subject, DataContentType, DataSchema string
	Time                                                   time.Time
	// Extensions holds the ce-* headers that are not standard attributes,
	// keyed by name without the ce- prefix.
	Extensions map[string]string
}

// FromHeader reads the CloudEvents attributes of a binary-mode message from
// its headers. It fails when ce-specversion, ce-id, ce-source or ce-type is
// missing, ce-specversion is not "1.0", or ce-time is not RFC 3339.
func FromHeader(h nats.Header) (Event, error) {
	get := func(k string) string {
		if v := h[k]; len(v) > 0 {
			return v[0]
		}
		return ""
	}
	e := Event{
		ID:              get("ce-id"),
		Source:          get("ce-source"),
		Type:            get("ce-type"),
		Subject:         get("ce-subject"),
		DataContentType: get(contentType),
		DataSchema:      get("ce-dataschema"),
	}
	switch v := get("ce-specversion"); v {
	case specVersion:
	case "":
		return Event{}, fmt.Errorf("nats: cloudevents: missing ce-specversion")
	default:
		return Event{}, fmt.Errorf("nats: cloudevents: unsupported ce-specversion %q", v)
	}
	for _, k := range []string{"ce-id", "ce-source", "ce-type"} {
		if get(k) == "" {
			return Event{}, fmt.Errorf("nats: cloudevents: missing %s", k)
		}
	}
	if s := get("ce-time"); s != "" {
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return Event{}, fmt.Errorf("nats: cloudevents: invalid ce-time %q: %w", s, err)
		}
		e.Time = t
	}
	for k, v := range h {
		name, ok := strings.CutPrefix(k, prefix)
		if !ok || len(v) == 0 || standard[name] {
			continue
		}
		if e.Extensions == nil {
			e.Extensions = map[string]string{}
		}
		e.Extensions[name] = v[0]
	}
	return e, nil
}

// standard lists the attribute names an extension must not reuse.
var standard = map[string]bool{
	"id": true, "source": true, "specversion": true, "type": true, "time": true,
	"subject": true, "datacontenttype": true, "dataschema": true, "data": true,
}
