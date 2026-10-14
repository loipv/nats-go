package nats

import (
	"errors"
	"fmt"
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
}
