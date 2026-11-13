package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestTerminal(t *testing.T) {
	base := errors.New("bad")
	err := fmt.Errorf("wrap: %w", Terminal(base))
	if !IsTerminal(err) || !errors.Is(err, base) {
		t.Fatal("terminal must be detectable and unwrap to base")
	}
	if IsTerminal(base) || Terminal(nil) != nil {
		t.Fatal("plain error is not terminal; Terminal(nil) is nil")
	}
}

func TestServiceErrorMessage(t *testing.T) {
	if got := (&ServiceError{Code: 400, Description: "bad input"}).Error(); got != "nats: service error 400: bad input" {
		t.Fatal(got)
	}
	// A handler can return a typed-nil *ServiceError; logging it must not panic.
	if got := (*ServiceError)(nil).Error(); got != "nats: service error" {
		t.Fatal(got)
	}
}

func TestReplyError(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		code int
		desc string
	}{
		{"service error", &ServiceError{Code: 404, Description: "no such order"}, 404, "no such order"},
		{"wrapped service error", fmt.Errorf("lookup: %w", &ServiceError{Code: 409, Description: "taken"}), 409, "taken"},
		{"terminal service error", Terminal(&ServiceError{Code: 422, Description: "bad sku"}), 422, "bad sku"},
		{"service error without code", &ServiceError{Description: "x"}, 500, "x"},
		{"terminal service error without code", Terminal(&ServiceError{Description: "x"}), 400, "x"},
		{"service error without description", &ServiceError{Code: 503}, 503, "internal error"},
		{"terminal service error without description", Terminal(&ServiceError{Code: 410}), 410, "bad request"},
		{"multi-line description", &ServiceError{Code: 404, Description: "gone\nstack line"}, 404, "gone"},
		{"typed-nil service error", error((*ServiceError)(nil)), 500, "internal error"},
		{"validation failure", Terminal(shown(errors.New("nats: validate: quantity must be positive"))), 400, "nats: validate: quantity must be positive"},
		{"duplicate", Terminal(ErrDuplicate), 400, "nats: duplicate message"},
		{"wrapped duplicate", fmt.Errorf("store: secret detail: %w", ErrDuplicate), 400, "nats: duplicate message"},
		{"wrapped in progress", fmt.Errorf("store: secret detail: %w", ErrInProgress), 500, "nats: message in progress elsewhere"},
		{"terminal error", Terminal(errors.New("pq: relation users")), 400, "bad request"},
		{"plain error", errors.New("dial tcp 10.0.0.5:5432: refused"), 500, "internal error"},
		{"panic", &panicError{v: "boom"}, 500, "internal error"},
		// A *ServiceError received from another service is not forwarded.
		{"received service error", &ServiceError{Code: 404, Description: "pq: relation invoices", received: true}, 500, "internal error"},
		{"wrapped received service error", fmt.Errorf("billing: %w", &ServiceError{Code: 404, Description: "x", received: true}), 500, "internal error"},
		{"terminal received service error", Terminal(&ServiceError{Code: 404, Description: "x", received: true}), 400, "bad request"},
		// A blank description would reach the requester as an empty header.
		{"blank description", &ServiceError{Code: 404, Description: " "}, 404, "internal error"},
		{"description starting with a line break", &ServiceError{Code: 404, Description: "\r\nreason"}, 404, "internal error"},
		{"padded description", &ServiceError{Code: 404, Description: "  gone\t"}, 404, "gone"},
	} {
		code, desc := replyError(c.err)
		if code != c.code || desc != c.desc {
			t.Errorf("%s: replyError = %d %q, want %d %q", c.name, code, desc, c.code, c.desc)
		}
	}

	_, err := decodeMsg[order](context.Background(), JSONCodec{}, &RawMsg{Data: []byte(`{"id":`)})
	if code, desc := replyError(err); code != 400 || !strings.HasPrefix(desc, "nats: decode: ") {
		t.Errorf("decode failure: replyError = %d %q, want 400 and the decode error", code, desc)
	}
}
