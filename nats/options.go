package nats

import (
	"crypto/tls"
	"errors"
	"log/slog"
	"time"

	natsio "github.com/nats-io/nats.go"
)

// Header is the NATS message header type.
type Header = natsio.Header

// RawMsg is an undecoded NATS message, as seen by middleware.
type RawMsg = natsio.Msg

// Option configures an App. Options are validated by New.
type Option func(*options) error

type options struct {
	url             string
	natsOpts        []natsio.Option
	codec           Codec
	logger          *slog.Logger
	shutdownTimeout time.Duration
	restartBackoff  Backoff
}

func defaultOptions() options {
	return options{
		url: natsio.DefaultURL,
		// Keep reconnecting through outages; WithNATSOptions can override.
		natsOpts:        []natsio.Option{natsio.MaxReconnects(-1)},
		codec:           JSONCodec{},
		logger:          slog.Default(),
		shutdownTimeout: 30 * time.Second,
		restartBackoff:  ExponentialBackoff(time.Second, 30*time.Second),
	}
}

// WithURL sets the server URL. Multiple servers are separated by commas.
// Defaults to nats://127.0.0.1:4222.
func WithURL(url string) Option {
	return func(o *options) error {
		if url == "" {
			return errors.New("nats: empty url")
		}
		o.url = url
		return nil
	}
}

// WithNATSOptions passes options straight to nats.go for anything this
// package does not wrap. They are applied after the defaults (which include
// MaxReconnects(-1): reconnect forever), so they override them. A
// ClosedHandler passed here is replaced by the App's own.
func WithNATSOptions(opts ...natsio.Option) Option {
	return func(o *options) error {
		o.natsOpts = append(o.natsOpts, opts...)
		return nil
	}
}

// WithCodec sets the codec used for payloads. Defaults to JSONCodec.
func WithCodec(c Codec) Option {
	return func(o *options) error {
		if c == nil {
			return errors.New("nats: nil codec")
		}
		o.codec = c
		return nil
	}
}

// WithLogger sets the logger. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) error {
		if l == nil {
			return errors.New("nats: nil logger")
		}
		o.logger = l
		return nil
	}
}

// WithShutdownTimeout bounds the graceful shutdown performed by Run.
// Defaults to 30s.
func WithShutdownTimeout(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return errors.New("nats: shutdown timeout must be positive")
		}
		o.shutdownTimeout = d
		return nil
	}
}

// WithRestartBackoff sets the delay before each attempt to restart a
// Consume or ConsumeBatch handler whose consumer was deleted. Attempts go on
// until one succeeds or the App shuts down. Defaults to
// ExponentialBackoff(time.Second, 30*time.Second).
func WithRestartBackoff(b Backoff) Option {
	return func(o *options) error {
		if b == nil {
			return errors.New("nats: nil restart backoff")
		}
		o.restartBackoff = b
		return nil
	}
}

func natsOption(opt natsio.Option) Option {
	return WithNATSOptions(opt)
}

// Name sets the connection name shown in server monitoring.
func Name(name string) Option { return natsOption(natsio.Name(name)) }

// UserInfo authenticates with a username and password.
func UserInfo(user, password string) Option { return natsOption(natsio.UserInfo(user, password)) }

// Token authenticates with a token.
func Token(token string) Option { return natsOption(natsio.Token(token)) }

// NKeyFromSeed authenticates with the NKey seed stored in seedFile.
func NKeyFromSeed(seedFile string) Option {
	return func(o *options) error {
		opt, err := natsio.NkeyOptionFromSeed(seedFile)
		if err != nil {
			return err
		}
		o.natsOpts = append(o.natsOpts, opt)
		return nil
	}
}

// UserCredentials authenticates with a decentralized JWT credentials file.
func UserCredentials(path string) Option { return natsOption(natsio.UserCredentials(path)) }

// UserJWTAndSeed authenticates with a user JWT and NKey seed given as strings.
func UserJWTAndSeed(jwt, seed string) Option { return natsOption(natsio.UserJWTAndSeed(jwt, seed)) }

// RootCAs trusts the PEM-encoded CA certificates in files.
func RootCAs(files ...string) Option { return natsOption(natsio.RootCAs(files...)) }

// ClientCert presents a client certificate for mutual TLS.
func ClientCert(certFile, keyFile string) Option {
	return natsOption(natsio.ClientCert(certFile, keyFile))
}

// TLSConfig sets the full TLS configuration.
func TLSConfig(cfg *tls.Config) Option { return natsOption(natsio.Secure(cfg)) }
