---
title: The certificate is the name
concepts: [tls, client-certificates, x509, union-authenticator, certificate-expiry]
---

## Core concept

A token file has two weaknesses the last stage named: it sits in plain text on
the server's disk, and the token crosses the network in a header anyone on the
path can read. Real clusters fix both with TLS, and then use TLS a second time
to say who the client is.

**First, the server proves who it is.** Given a certificate and its key, the
server stops speaking plain HTTP and serves HTTPS on the same address. The
flags are the real server's:

```
-tls-cert-file          the server's certificate, PEM
-tls-private-key-file   its key, PEM
-client-ca-file         a PEM bundle of CAs whose client certificates are users
```

A client trusts the server because the CA that signed this certificate is in
its kubeconfig (`certificate-authority-data`). The certificate has to name the
address the client dialled — here `127.0.0.1` — or the client refuses it.
Without the TLS flags nothing changes, and every earlier stage still runs over
plain HTTP.

**Then the client proves who it is.** In the handshake the server can ask for
a client certificate. With `-client-ca-file`, it asks, and verifies whatever
it is given against that bundle. A certificate the CA signed is a user:

```
Subject: CN=alice, O=developers, O=oncall

username  alice                  the common name
groups    developers, oncall     one per organization
          system:authenticated   added, as for every authenticated user
uid       (none)                 a certificate carries no uid
```

This is how `kubectl` talks to a kind cluster: the `client-certificate-data`
in your kubeconfig says `CN=kubernetes-admin, O=kubeadm:cluster-admins`, and
that group is the one with every permission.

**Asking is not requiring.** A client with no certificate is still let in —
it may have a token, and a health probe has nothing at all. So the server asks
for a certificate and verifies one *if it is given*. A certificate the CA did
not sign is refused in the handshake; so is one that has expired, which is how
a lost or stolen certificate stops working without anyone revoking it.
Anybody can write `CN=alice, O=system:masters` into a certificate; only the CA
signature makes it mean something.

**Two authenticators, one answer.** The real server runs its authenticators
in order — client certificate first, then tokens — and the first that names
somebody decides. A request with alice's certificate and a header for bob is
alice; one with alice's certificate and a token nobody knows is still alice,
because the header is never consulted. With no certificate, the token decides
exactly as before, and with neither, the request is 401 — as soon as any
authenticator is configured, a client CA as much as a token file.

The health endpoints still answer anybody, over HTTPS now.

## Go APIs

- `tls.LoadX509KeyPair(certFile, keyFile)` for the serving pair.
- `x509.NewCertPool()` and `pool.AppendCertsFromPEM(data)`; it returns false
  for a file with no certificates in it, which is worth refusing to start on.
- `tls.Config{ClientCAs: pool, ClientAuth: tls.VerifyClientCertIfGiven}` on
  `http.Server.TLSConfig`, then `srv.ListenAndServeTLS("", "")` — the empty
  file names mean "the certificate is already in `TLSConfig.Certificates`".
- `r.TLS.VerifiedChains` in a handler: non-empty only when the handshake
  verified the client's certificate. `VerifiedChains[0][0].Subject` holds
  `CommonName` and `Organization`.

## Hints

<details><summary>Nudge</summary>

Let crypto/tls do the verifying. `VerifyClientCertIfGiven` checks the
signature, the chain to your CA, the client-auth key usage and the dates, and
fails the handshake on any of them. All your middleware does is read the
subject of a certificate that has already passed.
</details>

<details><summary>Approach</summary>

At startup, build a `tls.Config` when `-tls-cert-file` is given, and add the
client CA pool and `VerifyClientCertIfGiven` when `-client-ca-file` is. In the
middleware, before the token: if `r.TLS` has a verified chain, the user is the
leaf's common name, its organizations plus `system:authenticated`, and you are
done. Otherwise fall through to the token code, which now answers 401 when
either a token file or a client CA is configured and the request has no good
token. Serve with `ListenAndServeTLS` when there is a config, `ListenAndServe`
when there is not.
</details>

## Further reading

- [X509 client certificates](https://kubernetes.io/docs/reference/access-authn-authz/authentication/#x509-client-certificates)
- [PKI certificates and requirements](https://kubernetes.io/docs/setup/best-practices/certificates/)
- [kube-apiserver flags](https://kubernetes.io/docs/reference/command-line-tools-reference/kube-apiserver/)
- [crypto/tls ClientAuthType](https://pkg.go.dev/crypto/tls#ClientAuthType)
