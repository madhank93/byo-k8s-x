---
title: Serve HTTPS the API server will trust
concepts: [tls, self-signed certificates, sans, long-running programs]
---

## Core concept

An admission webhook is called, not calling. Before it can judge anything it
has to be a server the API server is willing to talk to — and the API server
has exactly one rule about that: **TLS or nothing**.

There is no plaintext fallback and no negotiation. A webhook serving HTTP is
simply unreachable, and the failure does not say so: the API server reports an
admission timeout, or a context deadline, several layers away from the missing
certificate.

So the first thing this program owns is a certificate. It signs its own, which
makes it its own certificate authority — whoever is handed those bytes trusts
this program and nothing else. That is unusual in production, where
cert-manager or the cluster's signer issues one instead, but the shape is
identical: something mints a certificate, and something hands the API server a
bundle that verifies it.

What goes *in* the certificate matters more than who signed it. A certificate
is checked against the name the caller dialled, and that name lives in the
Subject Alternative Names — `DNSNames` for a hostname, `IPAddresses` for an
address. A common name alone has not been enough for years.

The other new thing here is the program's lifetime. Everything you wrote in the
earlier courses ran and exited. This one has to be up whenever the API server
might call it, so it serves until a signal stops it — and `SIGTERM` is what
Kubernetes sends a pod it wants gone.

## Go APIs

- `ecdsa.GenerateKey(elliptic.P256(), rand.Reader)` and
  `x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)` —
  the same template twice is what makes it self-signed.
- `x509.Certificate{DNSNames: …, IPAddresses: …, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true}`.
- `tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}` in
  `http.Server.TLSConfig`.
- `srv.ListenAndServeTLS("", "")` — empty paths, because the certificate is
  already in the config.
- `signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)`.

## Hints

<details><summary>Nudge</summary>

Keep the DER bytes `x509.CreateCertificate` returns. Stage 3 hands them to the
API server as the `caBundle`, and re-deriving them later is more work than
returning them now.
</details>

<details><summary>Approach</summary>

Put the certificate in `TLSConfig` rather than writing it to disk, and call
`ListenAndServeTLS("", "")`. Answer `/healthz` with anything at all, and print
the address you are serving on so the harness knows you are up.
</details>

## Further reading

- [Webhook request and response](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#request)
- [crypto/tls](https://pkg.go.dev/crypto/tls)
- [RFC 6125 §6.4 — names in certificates](https://datatracker.ietf.org/doc/html/rfc6125#section-6.4)
