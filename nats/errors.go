package nats

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the package.
var (
	// ErrNotRunning is returned when publishing before Start or after Shutdown.
	ErrNotRunning = errors.New("nats: app not running")
	// ErrAlreadyStarted is returned when Start is called twice or a handler is registered after Start.
	ErrAlreadyStarted = errors.New("nats: app already started")
	// ErrNotJetStream is returned when calling an ack method on a core NATS message.
	ErrNotJetStream = errors.New("nats: not a JetStream message")
)

// errMaxAttempts is the DLQ cause of a message redelivered past MaxAttempts.
var errMaxAttempts = errors.New("nats: max attempts exceeded")

type terminalError struct{ err error }

func (e *terminalError) Error() string { return e.err.Error() }
func (e *terminalError) Unwrap() error { return e.err }

// Terminal marks err as non-retryable. A JetStream message whose handler
// returns a terminal error is terminated (Term) instead of redelivered (Nak),
// and a Reply handler responds with code 400 instead of 500.
// Terminal(nil) returns nil.
func Terminal(err error) error {
	if err == nil {
		return nil
	}
	return &terminalError{err: err}
}

// IsTerminal reports whether any error in err's chain was marked with Terminal.
func IsTerminal(err error) bool {
	var t *terminalError
	return errors.As(err, &t)
}

// ServiceError is the error a requester receives when the responder's handler
// failed. It is carried in the Nats-Service-Error and Nats-Service-Error-Code
// headers, compatible with the NATS micro protocol.
type ServiceError struct {
	Code        int
	Description string
}

func (e *ServiceError) Error() string {
	return fmt.Sprintf("nats: service error %d: %s", e.Code, e.Description)
}
