package apiserver

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
)

func init() {
	register(Stage{Slug: "authn-cert", Run: stageAuthnCert})
}

// stageAuthnCert checks the server speaks TLS with the certificate it was
// given, and that a client certificate signed by the CA it trusts is a user:
// the common name is the username and each organization a group.
//
// The certificate is checked before the token, the first authenticator that
// names somebody wins, and a certificate the CA did not sign — or one that has
// expired — is never anybody.
func stageAuthnCert(ctx context.Context, _ *kube.Env, bin string) error {
	dir, err := os.MkdirTemp("", "byok8s-apiserver-*")
	if err != nil {
		return fmt.Errorf("make a directory for the certificates: %w", err)
	}
	defer os.RemoveAll(dir)
	pki, err := certMakePKI(dir)
	if err != nil {
		return err
	}
	tokens := filepath.Join(dir, "tokens.csv")
	if err := os.WriteFile(tokens, []byte(authnTokens), 0o600); err != nil {
		return fmt.Errorf("write the token file: %w", err)
	}

	tlsFlags := []string{"-tls-cert-file", pki.certFile, "-tls-private-key-file", pki.keyFile, "-client-ca-file", pki.caFile}
	srv, cleanup, err := serveTLS(ctx, bin, pki, append(tlsFlags, "-token-auth-file", tokens)...)
	if err != nil {
		return err
	}
	defer cleanup()

	nobody := pki.client(nil)
	res, _, err := certSend(ctx, srv, nobody, http.MethodGet, "/healthz", "", nil)
	if err != nil {
		return err
	}
	if res.TLS == nil || len(res.TLS.PeerCertificates) == 0 || !bytes.Equal(res.TLS.PeerCertificates[0].Raw, pki.serverDER) {
		return fmt.Errorf("the server answered over TLS with a certificate other than the one in -tls-cert-file: clients trust that one, because whoever set up the cluster put its CA in their kubeconfig")
	}
	for _, path := range []string{"/livez", "/readyz"} {
		res, body, err := certSend(ctx, srv, nobody, http.MethodGet, path, "", nil)
		if err != nil {
			return err
		}
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("GET %s over TLS with no certificate and no token answered %d: health is answered without authentication, whatever the transport\nthe body was:\n%s",
				path, res.StatusCode, tail(string(body)))
		}
	}

	const path = "/api/v1/namespaces/default/configmaps"
	plain := strings.Replace(srv.url, "https://", "http://", 1)
	if res, _, err := request(ctx, http.MethodGet, plain+path, nil); err == nil && res.StatusCode == http.StatusOK {
		return fmt.Errorf("GET %s in plain HTTP on the TLS port answered 200: given a certificate the server speaks only TLS, or every credential sent to it — a bearer token in a header — crosses the network readable", path)
	}

	alice := pki.client(&pki.alice)
	res, body, err := certSend(ctx, srv, alice, http.MethodGet, path, "", nil)
	if err != nil {
		return fmt.Errorf("%w\n\nalice's certificate is signed by the CA in -client-ca-file: the TLS handshake asks for a client certificate, verifies one if it is given, and lets a client with none through to try a token", err)
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s with alice's client certificate and no token answered %d rather than 200: a certificate the client CA signed is a user, as good as a token\nthe body was:\n%s",
			path, res.StatusCode, tail(string(body)))
	}
	user, err := certWhoami(ctx, srv, alice, "")
	if err != nil {
		return err
	}
	if user["username"] != "alice" {
		return fmt.Errorf("alice's certificate reviewed as username %v, and its subject is CN=alice: the common name is the username", user["username"])
	}
	if uid, ok := user["uid"]; ok && uid != "" {
		return fmt.Errorf("alice's certificate reviewed with uid %v: a certificate carries no uid, so the review leaves it out rather than inventing one from the serial number or the name", uid)
	}
	if err := certGroups("alice's certificate", user, "developers", "oncall", "system:authenticated"); err != nil {
		return fmt.Errorf("%w\n\nits subject is O=developers, O=oncall: each organization is a group, and everyone a certificate names is also in system:authenticated", err)
	}

	user, err = certWhoami(ctx, srv, pki.client(&pki.carol), "")
	if err != nil {
		return err
	}
	if err := certGroups("carol's certificate", user, "system:authenticated"); user["username"] != "carol" || err != nil {
		return fmt.Errorf("carol's certificate, CN=carol with no organization, reviewed as username %v with groups %v: she is carol, in system:authenticated and nothing else", user["username"], user["groups"])
	}

	// Both authenticators at once: the certificate is asked first, and the
	// first that names somebody wins, whatever the header says.
	for _, auth := range []string{"Bearer bob-token-0002", "Bearer nobody-knows-this"} {
		user, err := certWhoami(ctx, srv, alice, auth)
		if err != nil {
			return fmt.Errorf("%w\n\nthat request carried alice's certificate and Authorization %q: the certificate is checked first and it names alice, so the header is never consulted", err, auth)
		}
		if user["username"] != "alice" {
			return fmt.Errorf("a request with alice's certificate and Authorization %q reviewed as %v: authenticators are tried in order, certificate first, and the first that names somebody decides", auth, user["username"])
		}
	}

	user, err = certWhoami(ctx, srv, nobody, "Bearer bob-token-0002")
	if err != nil {
		return fmt.Errorf("%w\n\nwith no client certificate, a token from -token-auth-file is still a user: the two authenticators work side by side", err)
	}
	if user["username"] != "bob" || user["uid"] != "1002" {
		return fmt.Errorf("bob's token over TLS with no certificate reviewed as username %v, uid %v: with no certificate the token decides, exactly as it did before", user["username"], user["uid"])
	}

	res, body, err = certSend(ctx, srv, nobody, http.MethodGet, path, "", nil)
	if err != nil {
		return err
	}
	if err := wantStatus("GET "+path+" with no certificate and no token", res, body, http.StatusUnauthorized, "Unauthorized"); err != nil {
		return err
	}

	// A certificate from a CA the server was not told about says CN=alice
	// too, which is the point: anybody can write that.
	if err := certRefused(ctx, srv, pki.client(&pki.rogue), "a certificate for CN=alice, O=system:masters signed by a CA the server was not given",
		"a client certificate proves who it names only because the CA in -client-ca-file vouches for it; one the server cannot verify is a stranger who typed a name"); err != nil {
		return err
	}
	if err := certRefused(ctx, srv, pki.client(&pki.expired), "alice's certificate from the right CA that expired yesterday",
		"a certificate is good between its NotBefore and NotAfter, and the end date is how a lost or stolen one stops working without anyone revoking it"); err != nil {
		return err
	}
	intruder := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "intruder"}}
	if res, body, err := certSend(ctx, srv, pki.client(&pki.rogue), http.MethodPost, path, "", intruder); err == nil && res.StatusCode != http.StatusUnauthorized {
		return fmt.Errorf("POST %s with a certificate from the wrong CA answered %d: it is refused before the handler, so nothing it sends is stored\nthe body was:\n%s",
			path, res.StatusCode, tail(string(body)))
	}
	if res, body, err = certSend(ctx, srv, alice, http.MethodGet, path+"/intruder", "", nil); err != nil {
		return err
	}
	if res.StatusCode != http.StatusNotFound {
		return fmt.Errorf("GET %s/intruder answered %d after a POST of it with a certificate from the wrong CA: a refused write stores nothing\nthe body was:\n%s",
			path, res.StatusCode, tail(string(body)))
	}
	cleanup()

	return certOnly(ctx, bin, pki, tlsFlags)
}

// certOnly checks a server whose only authenticator is the client CA: a
// certificate is a user, and no certificate is nobody at all.
func certOnly(ctx context.Context, bin string, pki *certPKI, flags []string) error {
	srv, cleanup, err := serveTLS(ctx, bin, pki, flags...)
	if err != nil {
		return fmt.Errorf("%w\n\nthis run passes -client-ca-file and no -token-auth-file", err)
	}
	defer cleanup()

	user, err := certWhoami(ctx, srv, pki.client(&pki.alice), "")
	if err != nil {
		return fmt.Errorf("%w\n\nthis run has -client-ca-file and no -token-auth-file: the certificate is an authenticator on its own", err)
	}
	if user["username"] != "alice" {
		return fmt.Errorf("with only -client-ca-file, alice's certificate reviewed as %v: the certificate names her whether or not there is a token file", user["username"])
	}
	const path = "/api/v1/namespaces/default/configmaps"
	res, body, err := certSend(ctx, srv, pki.client(nil), http.MethodGet, path, "", nil)
	if err != nil {
		return err
	}
	if err := wantStatus("GET "+path+" with no certificate, on a server given -client-ca-file and no token file", res, body, http.StatusUnauthorized, "Unauthorized"); err != nil {
		return fmt.Errorf("%w\n\nonce any authenticator is configured, a request none of them can name is a 401 — a client CA counts as much as a token file", err)
	}
	return nil
}

// certRefused insists a request presenting this client is refused, either in
// the handshake or with a 401, and never reaches the API.
func certRefused(ctx context.Context, srv *server, client *http.Client, what, why string) error {
	const path = "/api/v1/namespaces/default/configmaps"
	res, body, err := certSend(ctx, srv, client, http.MethodGet, path, "", nil)
	if err != nil {
		return nil
	}
	if res.StatusCode == http.StatusUnauthorized {
		return nil
	}
	return fmt.Errorf("GET %s with %s answered %d: it should not get past the handshake, and at worst is a 401 — %s\nthe body was:\n%s",
		path, what, res.StatusCode, why, tail(string(body)))
}

// certGroups insists a review's groups are exactly these, in any order.
func certGroups(what string, user map[string]any, want ...string) error {
	groups, _ := user["groups"].([]any)
	if len(groups) != len(want) {
		return fmt.Errorf("%s reviewed with groups %v, and they are %v", what, user["groups"], want)
	}
	for _, g := range want {
		if !contains(groups, g) {
			return fmt.Errorf("%s reviewed with groups %v, missing %q", what, user["groups"], g)
		}
	}
	return nil
}

// serveTLS is serve for a program given a serving certificate: it waits for
// GET /healthz over HTTPS, trusting only the harness's CA, and returns the
// server with an https URL.
func serveTLS(ctx context.Context, bin string, pki *certPKI, args ...string) (*server, func(), error) {
	addr, err := freeAddr()
	if err != nil {
		return nil, nil, err
	}
	p, err := runner.Start(ctx, bin, nil, append([]string{"-addr", addr}, args...)...)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { p.Stop(5 * time.Second) }
	srv := &server{p: p, url: "https://" + addr}
	fail := func(format string, a ...any) (*server, func(), error) {
		cleanup()
		return nil, nil, fmt.Errorf(format, a...)
	}

	client := pki.client(nil)
	deadline := time.Now().Add(30 * time.Second)
	for {
		res, _, err := certSend(ctx, srv, client, http.MethodGet, "/healthz", "", nil)
		var verify *tls.CertificateVerificationError
		switch {
		case err == nil && res.StatusCode == http.StatusOK:
			return srv, cleanup, nil
		case err == nil:
			return fail("GET /healthz over TLS with no client certificate answered %d: the health endpoints answer whoever asks, certificate or not — whatever restarts a server has no credentials", res.StatusCode)
		case strings.Contains(err.Error(), "server gave HTTP response to HTTPS client"):
			return fail("the program answered plain HTTP on %s although it was given -tls-cert-file and -tls-private-key-file: with a certificate the server serves HTTPS on -addr, which is how every kube-apiserver serves", addr)
		case errors.As(err, &verify):
			return fail("the program served TLS on %s with a certificate the harness's CA did not sign (%v): serve the certificate and key in -tls-cert-file and -tls-private-key-file, the pair clients were told to trust", addr, verify.Err)
		case strings.Contains(err.Error(), "certificate required"):
			return fail("the TLS handshake on %s failed for a client with no certificate (%v): ask for a client certificate and verify it if one is given (tls.VerifyClientCertIfGiven), because a client with a token, and every health probe, has none", addr, err)
		}
		if done, result := p.Exited(); done {
			return fail("the program exited (%d) instead of serving HTTPS on %s with %s\nstdout:\n%s\nstderr:\n%s",
				result.ExitCode, addr, strings.Join(args, " "), tail(result.Stdout), tail(result.Stderr))
		}
		if time.Now().After(deadline) {
			return fail("nothing answered GET https://%s/healthz within 30s (last error: %v): this stage passes -tls-cert-file, -tls-private-key-file and -client-ca-file, and the server answers HTTPS on -addr\nthe program said:\n%s\nstderr:\n%s",
				addr, err, tail(p.Stdout()), tail(p.Stderr()))
		}
		select {
		case <-ctx.Done():
			cleanup()
			return nil, nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// certWhoami is authnWhoami over a client that may present a certificate.
func certWhoami(ctx context.Context, srv *server, client *http.Client, auth string) (map[string]any, error) {
	review := map[string]any{"apiVersion": "authentication.k8s.io/v1", "kind": "SelfSubjectReview"}
	res, body, err := certSend(ctx, srv, client, http.MethodPost, authnWhoamiPath, auth, review)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("POST %s answered %d rather than 201\nthe body was:\n%s", authnWhoamiPath, res.StatusCode, tail(string(body)))
	}
	obj, err := decode(body)
	if err != nil {
		return nil, fmt.Errorf("the reply to POST %s is not JSON (%w)\nthe body was:\n%s", authnWhoamiPath, err, tail(string(body)))
	}
	status, _ := obj["status"].(map[string]any)
	user, ok := status["userInfo"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("the SelfSubjectReview has no status.userInfo\nthe body was:\n%s", tail(string(body)))
	}
	return user, nil
}

// certSend is authnSend over the given client.
func certSend(ctx context.Context, srv *server, client *http.Client, method, path, auth string, body any) (*http.Response, []byte, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, nil, fmt.Errorf("encode the request body: %w", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, srv.url+path, reader)
	if err != nil {
		return nil, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("%s %s: %w\nthe program said:\n%s", method, path, err, tail(srv.p.Stdout()))
	}
	defer res.Body.Close()
	got, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, nil, fmt.Errorf("read the response body: %w", err)
	}
	return res, got, nil
}

// certPKI is everything the stage hands out or presents: the files the
// program is started with, and the client certificates the harness holds.
type certPKI struct {
	caFile, certFile, keyFile string
	pool                      *x509.CertPool
	serverDER                 []byte
	alice, carol              tls.Certificate
	rogue, expired            tls.Certificate
}

// client trusts only the harness's CA, and presents cert when it is non-nil.
// The certificate is offered whatever CAs the server says it accepts, so a
// server that skips verification is caught rather than never shown one.
func (p *certPKI) client(cert *tls.Certificate) *http.Client {
	config := &tls.Config{RootCAs: p.pool}
	if cert != nil {
		config.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return cert, nil }
	}
	return &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{TLSClientConfig: config}}
}

// certMakePKI writes a CA and a serving certificate for 127.0.0.1 into dir,
// and issues the client certificates.
func certMakePKI(dir string) (*certPKI, error) {
	now := time.Now()
	ca, caKey, err := certIssue(&x509.Certificate{
		Subject: pkix.Name{CommonName: "byok8s-ca"}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
	}, nil, nil)
	if err != nil {
		return nil, err
	}
	rogueCA, rogueKey, err := certIssue(&x509.Certificate{
		Subject: pkix.Name{CommonName: "someone-elses-ca"}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
	}, nil, nil)
	if err != nil {
		return nil, err
	}
	serving, servingKey, err := certIssue(&x509.Certificate{
		Subject: pkix.Name{CommonName: "byok8s-apiserver"}, DNSNames: []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
	}, ca, caKey)
	if err != nil {
		return nil, err
	}
	clientCert := func(subject pkix.Name, notAfter time.Time, issuer *x509.Certificate, issuerKey *ecdsa.PrivateKey) (tls.Certificate, error) {
		cert, key, err := certIssue(&x509.Certificate{
			Subject: subject, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			NotBefore: notAfter.Add(-48 * time.Hour), NotAfter: notAfter,
		}, issuer, issuerKey)
		if err != nil {
			return tls.Certificate{}, err
		}
		return tls.Certificate{Certificate: [][]byte{cert.Raw}, PrivateKey: key, Leaf: cert}, nil
	}

	p := &certPKI{
		caFile:    filepath.Join(dir, "ca.crt"),
		certFile:  filepath.Join(dir, "apiserver.crt"),
		keyFile:   filepath.Join(dir, "apiserver.key"),
		pool:      x509.NewCertPool(),
		serverDER: serving.Raw,
	}
	p.pool.AddCert(ca)
	if p.alice, err = clientCert(pkix.Name{CommonName: "alice", Organization: []string{"developers", "oncall"}}, now.Add(24*time.Hour), ca, caKey); err != nil {
		return nil, err
	}
	if p.carol, err = clientCert(pkix.Name{CommonName: "carol"}, now.Add(24*time.Hour), ca, caKey); err != nil {
		return nil, err
	}
	if p.rogue, err = clientCert(pkix.Name{CommonName: "alice", Organization: []string{"system:masters"}}, now.Add(24*time.Hour), rogueCA, rogueKey); err != nil {
		return nil, err
	}
	if p.expired, err = clientCert(pkix.Name{CommonName: "alice", Organization: []string{"developers", "oncall"}}, now.Add(-24*time.Hour), ca, caKey); err != nil {
		return nil, err
	}

	key, err := x509.MarshalPKCS8PrivateKey(servingKey)
	if err != nil {
		return nil, fmt.Errorf("encode the serving key: %w", err)
	}
	for file, block := range map[string]*pem.Block{
		p.caFile:   {Type: "CERTIFICATE", Bytes: ca.Raw},
		p.certFile: {Type: "CERTIFICATE", Bytes: serving.Raw},
		p.keyFile:  {Type: "PRIVATE KEY", Bytes: key},
	} {
		if err := os.WriteFile(file, pem.EncodeToMemory(block), 0o600); err != nil {
			return nil, fmt.Errorf("write %s: %w", file, err)
		}
	}
	return p, nil
}

// certIssue signs template with a fresh P-256 key, by issuer or by itself
// when issuer is nil.
func certIssue(template, issuer *x509.Certificate, issuerKey *ecdsa.PrivateKey) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate a key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		return nil, nil, err
	}
	template.SerialNumber = serial
	if issuer == nil {
		issuer, issuerKey = template, key
	}
	der, err := x509.CreateCertificate(rand.Reader, template, issuer, &key.PublicKey, issuerKey)
	if err != nil {
		return nil, nil, fmt.Errorf("sign the certificate for %s: %w", template.Subject.CommonName, err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}
