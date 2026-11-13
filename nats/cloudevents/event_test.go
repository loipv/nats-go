package cloudevents_test

import (
	"strings"
	"testing"
	"time"

	"github.com/loipv/nats-go/nats"
	"github.com/loipv/nats-go/nats/cloudevents"
)

func TestFromHeaderParsesFractionalTime(t *testing.T) {
	h := nats.Header{"ce-specversion": {"1.0"}, "ce-id": {"1"}, "ce-source": {"s"}, "ce-type": {"t"}, "ce-time": {"2026-10-15T08:30:00.5+07:00"}, "ce-region": {"eu"}}
	e, err := cloudevents.FromHeader(h)
	if err != nil || !e.Time.Equal(time.Date(2026, 10, 15, 1, 30, 0, 5e8, time.UTC)) || e.Extensions["region"] != "eu" {
		t.Fatal(e, err)
	}
	h["ce-specversion"] = []string{"0.3"}
	if _, err := cloudevents.FromHeader(h); err == nil {
		t.Fatal("unsupported specversion")
	}
}

func TestFromHeaderRejects(t *testing.T) {
	valid := func() nats.Header {
		return nats.Header{"ce-specversion": {"1.0"}, "ce-id": {"1"}, "ce-source": {"s"}, "ce-type": {"t"}}
	}
	if _, err := cloudevents.FromHeader(valid()); err != nil {
		t.Fatal("the base header must be valid:", err)
	}
	for name, mutate := range map[string]func(nats.Header){
		"no specversion":     func(h nats.Header) { delete(h, "ce-specversion") },
		"old specversion":    func(h nats.Header) { h["ce-specversion"] = []string{"0.3"} },
		"no id":              func(h nats.Header) { delete(h, "ce-id") },
		"no source":          func(h nats.Header) { delete(h, "ce-source") },
		"no type":            func(h nats.Header) { delete(h, "ce-type") },
		"time not RFC 3339":  func(h nats.Header) { h["ce-time"] = []string{"yesterday"} },
		"empty id value":     func(h nats.Header) { h["ce-id"] = []string{""} },
		"id without a value": func(h nats.Header) { h["ce-id"] = nil },
	} {
		h := valid()
		mutate(h)
		if _, err := cloudevents.FromHeader(h); err == nil || !strings.HasPrefix(err.Error(), "nats: cloudevents: ") {
			t.Errorf("%s: err = %v, want a nats: cloudevents: error", name, err)
		}
	}
}

func TestFromHeaderStandardAttributes(t *testing.T) {
	e, err := cloudevents.FromHeader(nats.Header{
		"ce-specversion": {"1.0"}, "ce-id": {"1"}, "ce-source": {"s"}, "ce-type": {"t"},
		"ce-subject": {"order/1"}, "ce-dataschema": {"https://example.com/schema"},
		"Content-Type": {"application/json"}, "ce-region": {"eu"}, "X-Other": {"x"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if e.Subject != "order/1" || e.DataSchema != "https://example.com/schema" || e.DataContentType != "application/json" {
		t.Fatalf("standard attributes: %+v", e)
	}
	if len(e.Extensions) != 1 || e.Extensions["region"] != "eu" {
		t.Fatalf("extensions = %v, want only region", e.Extensions)
	}
	if !e.Time.Equal(time.Time{}) {
		t.Fatalf("time = %v, want the zero time without ce-time", e.Time)
	}
}

// FuzzFromHeader checks that FromHeader never panics, and that an event it
// accepts carries spec version 1.0 and the required attributes it was given.
func FuzzFromHeader(f *testing.F) {
	f.Add("1.0", "1", "src", "order.created", "2026-10-15T08:30:00Z", "region", "eu")
	f.Add("1.0", "", "src", "t", "", "", "")
	f.Add("0.3", "1", "src", "t", "yesterday", "id", "x")
	f.Add("", "1", "", "", "2026-10-15T08:30:00.5+07:00", "subject", "s")
	f.Fuzz(func(t *testing.T, specVersion, id, source, typ, tm, extName, extValue string) {
		h := nats.Header{"ce-specversion": {specVersion}, "ce-id": {id}, "ce-source": {source}, "ce-type": {typ}}
		if tm != "" {
			h["ce-time"] = []string{tm}
		}
		if extName != "" {
			h["ce-"+extName] = []string{extValue}
		}
		e, err := cloudevents.FromHeader(h)
		if err != nil {
			return
		}
		// An extension named like a standard attribute replaced it in the map
		// above, so compare with what the header finally holds.
		at := func(k string) string { return h[k][0] }
		if at("ce-specversion") != "1.0" {
			t.Fatalf("accepted ce-specversion %q", at("ce-specversion"))
		}
		if e.ID == "" || e.Source == "" || e.Type == "" {
			t.Fatalf("accepted an event without id, source or type: %+v", e)
		}
		if e.ID != at("ce-id") || e.Source != at("ce-source") || e.Type != at("ce-type") {
			t.Fatalf("attributes differ from the header: %+v from %v", e, h)
		}
	})
}
