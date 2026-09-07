// Your admission webhook.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	// The API server calls this program, so it runs until something asks it to
	// stop. SIGTERM is what Kubernetes sends a pod it is deleting.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	addr := flag.String("addr", "0.0.0.0:8443", "address to serve the webhook on")
	externalHost := flag.String("external-host", "host.docker.internal", "the name the API server reaches this program by")
	flag.Parse()

	cert, _, err := selfSigned(*externalHost)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/validate", validate)

	srv := &http.Server{
		Addr:      *addr,
		Handler:   mux,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{*cert}},
	}
	go func() {
		<-ctx.Done()
		fmt.Println("shutting down")
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()

	fmt.Printf("serving on %s\n", *addr)
	// The certificate and key are already in TLSConfig, so this takes neither.
	if err := srv.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// validate answers an AdmissionReview with an AdmissionReview.
//
// The wrapper is the protocol: the same kind goes back, carrying the request's
// uid and a verdict. A reply that loses the uid is discarded — the API server
// cannot tell which request it answers.
func validate(w http.ResponseWriter, r *http.Request) {
	var review admissionv1.AdmissionReview
	if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
		http.Error(w, fmt.Sprintf("decode AdmissionReview: %v", err), http.StatusBadRequest)
		return
	}
	if review.Request == nil {
		http.Error(w, "AdmissionReview carried no request", http.StatusBadRequest)
		return
	}

	reply := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			APIVersion: admissionv1.SchemeGroupVersion.String(),
			Kind:       "AdmissionReview",
		},
		Response: &admissionv1.AdmissionResponse{
			UID:     review.Request.UID,
			Allowed: true,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(reply); err != nil {
		fmt.Fprintf(os.Stderr, "write AdmissionReview: %v\n", err)
	}
}

// selfSigned mints a certificate for the name the API server will use, and
// returns it alongside the DER bytes that go in a webhook's caBundle.
//
// The certificate signs itself, so it is its own CA: whoever is told to trust
// these bytes trusts this program and nothing else. A real deployment gets its
// certificate from cert-manager or the cluster's signer, but the shape is the
// same — something has to hand the API server a bundle it will accept.
func selfSigned(host string) (*tls.Certificate, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("serial number: %w", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: host},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{host},
	}
	// A caller reaching the webhook by address rather than name — the harness
	// does, over the loopback — is checked against the IP SANs, not the DNS
	// ones.
	if ip := net.ParseIP(host); ip != nil {
		tmpl.IPAddresses = []net.IP{ip}
	}
	tmpl.IPAddresses = append(tmpl.IPAddresses, net.IPv4(127, 0, 0, 1), net.IPv6loopback)

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("create certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, fmt.Errorf("parse certificate: %w", err)
	}
	return &tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
		Leaf:        leaf,
	}, der, nil
}
