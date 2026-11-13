package nats

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func TestNewValidatesOptions(t *testing.T) {
	if _, err := New(WithURL("")); err == nil {
		t.Fatal("empty url must fail")
	}
	if _, err := New(WithShutdownTimeout(0)); err == nil {
		t.Fatal("zero timeout must fail")
	}
}

// startFails reports whether an App with opts cannot start against s. Every
// auth and TLS test uses it to show the server refuses a client that lacks the
// credential, so the passing case proves the option was applied.
func startFails(t *testing.T, s *server.Server, opts ...Option) bool {
	t.Helper()
	a := newApp(t, s, opts...)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := a.Start(ctx)
	if err == nil {
		_ = a.Shutdown(ctx)
	}
	return err != nil
}

func TestTokenAuth(t *testing.T) {
	s := runServerWith(t, func(o *server.Options) { o.Authorization = "s3cret" })
	if !startFails(t, s, WithToken("wrong")) {
		t.Fatal("wrong token must fail")
	}
	start(t, newApp(t, s, WithToken("s3cret")))
}

func TestUserInfoAuth(t *testing.T) {
	s := runServerWith(t, func(o *server.Options) { o.Username, o.Password = "svc", "pw" })
	if !startFails(t, s, WithUserInfo("svc", "wrong")) {
		t.Fatal("wrong password must fail")
	}
	start(t, newApp(t, s, WithUserInfo("svc", "pw")))
}

func TestNKeyAuth(t *testing.T) {
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	pub, _ := kp.PublicKey()
	seed, _ := kp.Seed()
	seedFile := filepath.Join(t.TempDir(), "user.nk")
	if err := os.WriteFile(seedFile, seed, 0o600); err != nil {
		t.Fatal(err)
	}
	s := runServerWith(t, func(o *server.Options) { o.Nkeys = []*server.NkeyUser{{Nkey: pub}} })
	if !startFails(t, s) {
		t.Fatal("a client without the nkey must fail")
	}
	start(t, newApp(t, s, WithNKeyFromSeed(seedFile)))

	if _, err := New(WithNKeyFromSeed(filepath.Join(t.TempDir(), "missing"))); err == nil {
		t.Fatal("a missing seed file must fail New")
	}
}

// operatorServer starts a server in operator mode that trusts one account,
// and returns that account's key for signing user JWTs.
func operatorServer(t *testing.T) (*server.Server, nkeys.KeyPair) {
	t.Helper()
	operator, err := nkeys.CreateOperator()
	if err != nil {
		t.Fatal(err)
	}
	operatorPub, _ := operator.PublicKey()
	account, _ := nkeys.CreateAccount()
	accountPub, _ := account.PublicKey()
	accountJWT, err := jwt.NewAccountClaims(accountPub).Encode(operator)
	if err != nil {
		t.Fatal(err)
	}
	resolver := &server.MemAccResolver{}
	if err := resolver.Store(accountPub, accountJWT); err != nil {
		t.Fatal(err)
	}
	s := runServerWith(t, func(o *server.Options) {
		o.JetStream = false
		o.TrustedKeys = []string{operatorPub}
		o.AccountResolver = resolver
	})
	return s, account
}

// userJWT returns a user JWT signed by account, and the user's seed.
func userJWT(t *testing.T, account nkeys.KeyPair) (token string, seed []byte) {
	t.Helper()
	user, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	pub, _ := user.PublicKey()
	seed, _ = user.Seed()
	token, err = jwt.NewUserClaims(pub).Encode(account)
	if err != nil {
		t.Fatal(err)
	}
	return token, seed
}

func TestUserJWTAndSeedAuth(t *testing.T) {
	s, account := operatorServer(t)
	stranger, _ := nkeys.CreateAccount()
	badJWT, badSeed := userJWT(t, stranger)
	if !startFails(t, s, WithUserJWTAndSeed(badJWT, string(badSeed))) {
		t.Fatal("a user of an unknown account must fail")
	}
	token, seed := userJWT(t, account)
	start(t, newApp(t, s, WithUserJWTAndSeed(token, string(seed))))
}

func TestUserCredentialsAuth(t *testing.T) {
	s, account := operatorServer(t)
	if !startFails(t, s) {
		t.Fatal("a client without credentials must fail")
	}
	token, seed := userJWT(t, account)
	creds, err := jwt.FormatUserConfig(token, seed)
	if err != nil {
		t.Fatal(err)
	}
	credsFile := filepath.Join(t.TempDir(), "user.creds")
	if err := os.WriteFile(credsFile, creds, 0o600); err != nil {
		t.Fatal(err)
	}
	start(t, newApp(t, s, WithUserCredentials(credsFile)))
}

// tlsServer starts a server that requires a client certificate signed by the
// CA in caFile.
func tlsServer(t *testing.T, caFile, serverCert, serverKey string) *server.Server {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(serverCert, serverKey)
	if err != nil {
		t.Fatal(err)
	}
	return runServerWith(t, func(o *server.Options) {
		o.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientCAs:    certPool(t, caFile),
			ClientAuth:   tls.RequireAndVerifyClientCert,
			MinVersion:   tls.VersionTLS12,
		}
	})
}

func certPool(t *testing.T, caFile string) *x509.CertPool {
	t.Helper()
	pemBytes, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		t.Fatal("no CA certificate in", caFile)
	}
	return pool
}

func TestTLSRootCAsAndClientCert(t *testing.T) {
	caFile, serverCert, serverKey, clientCert, clientKey := testCerts(t)
	s := tlsServer(t, caFile, serverCert, serverKey)
	if !startFails(t, s) {
		t.Fatal("a client that does not trust the CA must fail")
	}
	if !startFails(t, s, WithRootCAs(caFile)) {
		t.Fatal("a client without a certificate must fail")
	}
	start(t, newApp(t, s, WithRootCAs(caFile), WithClientCert(clientCert, clientKey)))
}

func TestTLSConfig(t *testing.T) {
	caFile, serverCert, serverKey, clientCert, clientKey := testCerts(t)
	s := tlsServer(t, caFile, serverCert, serverKey)
	if !startFails(t, s, WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})) {
		t.Fatal("a config that does not trust the CA must fail")
	}
	cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		t.Fatal(err)
	}
	start(t, newApp(t, s, WithTLSConfig(&tls.Config{
		RootCAs:      certPool(t, caFile),
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})))
}

func TestWithName(t *testing.T) {
	a := newApp(t, runServer(t), WithName("billing"))
	start(t, a)
	if got := a.Conn().Opts.Name; got != "billing" {
		t.Fatalf("connection name = %q, want billing", got)
	}
}

// prefixCodec is JSON behind an "x:" marker, so a test can tell it was used.
type prefixCodec struct{}

func (prefixCodec) Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	return append([]byte("x:"), b...), err
}

func (prefixCodec) Unmarshal(data []byte, v any) error {
	rest, ok := bytes.CutPrefix(data, []byte("x:"))
	if !ok {
		return errors.New("payload was not encoded by prefixCodec")
	}
	return json.Unmarshal(rest, v)
}

func TestWithCodec(t *testing.T) {
	if _, err := New(WithCodec(nil)); err == nil {
		t.Fatal("nil codec must fail")
	}
	a := newApp(t, runServer(t), WithCodec(prefixCodec{}))
	got := make(chan *Msg[order], 1)
	a.Subscribe("codec.test", Typed(func(_ context.Context, m *Msg[order]) error { got <- m; return nil }))
	start(t, a)
	if err := a.Publish(context.Background(), "codec.test", order{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	m := recv(t, got)
	if m.Data.ID != "1" || !bytes.HasPrefix(m.Raw.Data, []byte("x:")) {
		t.Fatalf("data %+v, raw %q", m.Data, m.Raw.Data)
	}
}

// Options passed to nats.go run once per connect: some read files.
func TestNATSOptionsAppliedOnce(t *testing.T) {
	s := runServer(t)
	var n atomic.Int32
	a := newApp(t, s, WithNATSOptions(func(*natsio.Options) error { n.Add(1); return nil }))
	start(t, a)
	if got := n.Load(); got != 1 {
		t.Fatalf("option applied %d times, want 1", got)
	}
}
