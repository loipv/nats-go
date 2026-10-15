package nats

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the package.
var (
	// ErrNotRunning is returned when publishing before Start or after Shutdown.
	ErrNotRunning = errors.New("nats: app not running")
	// ErrAlreadyStarted is returned when Start is called twice. A registration
	// made after Start is ignored and logged with it.
	ErrAlreadyStarted = errors.New("nats: app already started")
	// ErrNotJetStream is returned when calling an ack method on a core NATS message.
	ErrNotJetStream = errors.New("nats: not a JetStream message")
	// ErrNotCore is returned by Msg.Respond and Msg.RespondError on a JetStream
	// message, whose reply subject is its ack subject.
	ErrNotCore = errors.New("nats: not a core NATS message")
	// ErrDuplicate is returned by IdempotencyStore.Begin when the key was
	// already completed.
	ErrDuplicate = errors.New("nats: duplicate message")
	// ErrInProgress is returned by IdempotencyStore.Begin when the key is
	// claimed and not yet completed.
	ErrInProgress = errors.New("nats: message in progress elsewhere")
)

// errMaxAttempts is the DLQ cause of a message redelivered past WithMaxAttempts.
var errMaxAttempts = errors.New("nats: max attempts exceeded")

// errReplyOwned is returned by Msg.Respond in a SubscribeReply or endpoint
// handler, whose return value is the response.
var errReplyOwned = errors.New("nats: Respond inside SubscribeReply or an endpoint: return the response instead")

type terminalError struct{ err error }

func (e *terminalError) Error() string { return e.err.Error() }
func (e *terminalError) Unwrap() error { return e.err }

// Terminal marks err as non-retryable. A JetStream message whose handler
// returns a terminal error is terminated (Term) instead of redelivered (Nak),
// and a SubscribeReply handler responds with code 400 instead of 500.
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
