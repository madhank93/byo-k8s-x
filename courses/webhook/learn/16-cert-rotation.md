---
title: Replace the certificate without dropping a request
concepts: [certificate rotation, getcertificate, cabundle, expiry]
---

## Core concept

Certificates expire. A webhook whose certificate expires stops being callable,
and — depending on the failure policy — either silently stops enforcing or
stops the cluster writing. It is the single most common way a working webhook
breaks months after anyone touched it.

Rotation has two halves, and they must not happen at the same instant.

**The serving side.** `tls.Config.Certificates` is read once, at handshake
time, from a slice you filled at startup. Swapping that slice under a running
server is a data race and does not affect connections already open. The
supported hook is `GetCertificate`, called per handshake: keep the current
certificate behind a mutex, return it from there, and rotation becomes a
pointer swap that new handshakes pick up and old ones do not notice.

**The trust side.** The API server verifies against the `caBundle` in your
registration. Update the serving certificate before the bundle and every call
fails verification until the bundle catches up; update the bundle first and the
old certificate no longer verifies.

The way out is to make both valid at once. Either keep signing with the same
CA — then only the leaf changes and the bundle never has to — or put both CAs
in the bundle during the overlap, since `caBundle` is a list. Rotate the leaf,
wait, then drop the old CA.

This is what cert-manager's `ca-injector` does for real deployments: it watches
the secret and writes the bundle into the configuration for you. Doing it by
hand once is what makes that automation legible.

Reload on a signal or a timer, not on every request — and log it. A rotation
nobody can see is one you will not be able to correlate with the outage.

## Go APIs

- `tls.Config{GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error)}`.
- `sync.RWMutex` or `atomic.Pointer[tls.Certificate]` around the current
  certificate.
- `cert.Leaf.NotAfter` to know when the current one runs out.
- `Update` on the webhook configuration to replace `caBundle`, which may hold
  more than one certificate.

## What this stage checks

- The certificate the program presents verifies against the `caBundle` in its
  own registration.
- What it presents is a leaf, not the CA sitting in that bundle — they are two
  different certificates.
- The program says it rotated, and a handshake opened afterwards gets a
  certificate with a different serial. That only works if the swap happens
  behind `GetCertificate`; a slice read once at startup keeps serving the old
  one.
- The rotated certificate verifies against the *same* `caBundle`, and the
  registration was not rewritten to make that true.
- Pods are admitted and refused exactly as before, on the other side of the
  rotation.

Rotate on a timer short enough that a stage can watch it happen. Real
intervals are hours or days; nothing else about the shape changes.

## Hints

<details><summary>Nudge</summary>

Connections the API server already has open keep using the old certificate.
That is fine — it is why rotating the leaf while both CAs are trusted drops
nothing.
</details>

<details><summary>Approach</summary>

Mint the new certificate from the same CA you registered. Then rotation touches
only the serving side, and the `caBundle` you registered at stage 3 stays
correct.
</details>

<details><summary>Implementation</summary>

```go
var current atomic.Pointer[tls.Certificate]
cfg := &tls.Config{
    GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
        return current.Load(), nil
    },
}
```
</details>

## Further reading

- [crypto/tls: GetCertificate](https://pkg.go.dev/crypto/tls#Config)
- [cert-manager: CA injector](https://cert-manager.io/docs/concepts/ca-injector/)
