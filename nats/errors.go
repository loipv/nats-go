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
	// ErrConfigDrift is returned by Start when a stream or consumer declared
	// with Verify differs from the server's config; the error lists the fields.
	// A supervised handler recreating a deleted consumer reports it as an
	// async error and keeps retrying until the config matches.
	ErrConfigDrift = errors.New("nats: config drift")
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
// and a SubscribeReply or endpoint requester gets code 400 "bad request"
// instead of 500 "internal error", unless err holds a *ServiceError.
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
//
// A SubscribeReply or endpoint handler, or Msg.RespondError, returns a
// *ServiceError to choose what the requester gets: Code (500 when not
// positive, 400 for a Terminal error) and the first line of Description
// ("internal error" or "bad request" when empty). The text of any other
// handler error is never sent; see Terminal. A *ServiceError only decides what
// the requester sees: wrap it with Terminal to also stop the retries.
type ServiceError struct {
	Code        int
	Description string
}

func (e *ServiceError) Error() string {
	if e == nil {
		return "nats: service error"
	}
	return fmt.Sprintf("nats: service error %d: %s", e.Code, e.Description)
}

// shownError marks a library error whose first line may be sent to a
// requester: a decode or validation failure.
type shownError struct{ err error }

func (e *shownError) Error() string { return e.err.Error() }
func (e *shownError) Unwrap() error { return e.err }

func shown(err error) error { return &shownError{err: err} }

// replyError returns the code and the description a requester gets for a
// handler error. Only a *ServiceError, a decode or validation failure and the
// idempotency sentinels carry their own text; every other error, a panic
// included, becomes "internal error" (500) or, when Terminal, "bad request"
// (400), and stays in the server log.
func replyError(err error) (code int, description string) {
	code, description = 500, "internal error"
	if IsTerminal(err) {
		code, description = 400, "bad request"
	}
	var se *ServiceError
	var sh *shownError
	switch {
	case errors.As(err, &se) && se != nil:
		if se.Code > 0 {
			code = se.Code
		}
		if d := firstLine(errors.New(se.Description)); d != "" {
			description = d
		}
	case errors.As(err, &sh):
		description = firstLine(sh)
	case errors.Is(err, ErrDuplicate):
		code, description = 400, ErrDuplicate.Error()
	case errors.Is(err, ErrInProgress):
		code, description = 500, ErrInProgress.Error()
	}
	return code, description
}
