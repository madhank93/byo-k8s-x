---
title: Answer healthz and readyz
concepts: [liveness, readiness, cache sync, probes]
---

## Core concept

Once this runs in the cluster, the kubelet decides whether to restart it and
whether to send it traffic, and it decides by asking. Two endpoints, two
different questions:

- **`/healthz`** — is this process alive and worth keeping? Failing it gets the
  pod killed, so it must not depend on anything external. A liveness probe that
  fails when the API server is briefly unreachable turns one outage into a
  restart storm.
- **`/readyz`** — is it ready to do its job? For a controller, that means the
  informer cache has synced: before that it knows nothing, and anything it did
  would be based on an empty world.

The distinction is worth internalising because getting it backwards is a
classic outage. Liveness should be almost unconditional. Readiness is where the
real check goes.

## Go APIs

- The same mux as the metrics endpoint.
- `informer.HasSynced()` — cheap, safe to call from an HTTP handler.
- `w.WriteHeader(http.StatusServiceUnavailable)` for a "not yet".

## Hints

<details><summary>Nudge</summary>

Start the HTTP server before waiting for the cache to sync. A probe endpoint
that only exists after the thing it reports on is finished cannot report on it.
</details>

<details><summary>Implementation</summary>

```go
mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
    if !informer.HasSynced() {
        http.Error(w, "cache not synced", http.StatusServiceUnavailable)
        return
    }
    fmt.Fprintln(w, "ok")
})
```
</details>

## Further reading

- [Configure liveness, readiness and startup probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
- [Liveness probes are dangerous](https://srcco.de/posts/kubernetes-liveness-probes-are-dangerous.html)
