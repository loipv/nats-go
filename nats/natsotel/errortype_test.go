package natsotel

import (
	"errors"
	"fmt"
	"testing"
)

type codeError struct{}

func (codeError) Error() string { return "code" }

func TestErrorTypeSkipsFmtWrappers(t *testing.T) {
	for _, c := range []struct {
		err  error
		want string
	}{
		{nil, ""},
		{codeError{}, "natsotel.codeError"},
		{fmt.Errorf("a: %w", fmt.Errorf("b: %w", codeError{})), "natsotel.codeError"},
		{fmt.Errorf("a: %w", errors.New("x")), "*errors.errorString"},
		{fmt.Errorf("%w %w", codeError{}, errors.New("x")), "*fmt.wrapErrors"},
	} {
		if got := errorType(c.err); got != c.want {
			t.Errorf("errorType(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}
