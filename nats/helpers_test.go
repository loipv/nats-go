package nats

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
)

func runServer(t *testing.T) *server.Server {
	t.Helper()
	return runServerWith(t, nil)
}

// runServerWith starts an embedded JetStream server on a random port after mod,
// if any, changed its options.
func runServerWith(t testing.TB, mod func(*server.Options)) *server.Server {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	if mod != nil {
		mod(&opts)
	}
	s := natstest.RunServer(&opts)
	t.Cleanup(s.Shutdown)
	return s
}

// testCerts writes a throwaway CA, a server certificate for 127.0.0.1 and
// localhost, and a client certificate, all signed by the CA, as PEM files.
func testCerts(t testing.TB) (caFile, serverCert, serverKey, clientCert, clientKey string) {
	t.Helper()
	dir := t.TempDir()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	write := func(name, typ string, der []byte) string {
		t.Helper()
		path := filepath.Join(dir, name)
		must(os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600))
		return path
	}
	serial := int64(0)
	// issue signs tmpl with the CA, or with its own key when ca is nil.
	issue := func(name string, tmpl, ca *x509.Certificate, caKey *ecdsa.PrivateKey) (certFile, keyFile string, cert *x509.Certificate, key *ecdsa.PrivateKey) {
		t.Helper()
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		must(err)
		serial++
		tmpl.SerialNumber = big.NewInt(serial)
		tmpl.Subject = pkix.Name{CommonName: name}
		tmpl.NotBefore, tmpl.NotAfter = time.Now().Add(-time.Minute), time.Now().Add(time.Hour)
		if ca == nil {
			ca, caKey = tmpl, key
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		must(err)
		cert, err = x509.ParseCertificate(der)
		must(err)
		keyDER, err := x509.MarshalECPrivateKey(key)
		must(err)
		return write(name+".pem", "CERTIFICATE", der), write(name+"-key.pem", "EC PRIVATE KEY", keyDER), cert, key
	}
	caFile, _, ca, caKey := issue("ca", &x509.Certificate{
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}, nil, nil)
	serverCert, serverKey, _, _ = issue("server", &x509.Certificate{
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:    []string{"localhost"},
	}, ca, caKey)
	clientCert, clientKey, _, _ = issue("client", &x509.Certificate{
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, ca, caKey)
	return caFile, serverCert, serverKey, clientCert, clientKey
}

func newApp(t *testing.T, s *server.Server, opts ...Option) *App {
	t.Helper()
	a, err := New(append([]Option{WithURL(s.ClientURL())}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func start(t *testing.T, a *App) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = a.Shutdown(ctx)
	})
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("nothing received within 2s")
		var zero T
		return zero
	}
}

func expectNone[T any](t *testing.T, ch <-chan T, d time.Duration) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected value %v", v)
	case <-time.After(d):
	}
}
