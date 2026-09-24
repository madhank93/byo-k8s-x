---
title: Something that answers
concepts: [healthz, livez, readyz, status, http]
---

## Core concept

Before this server can be wrong about anything interesting, it has to be
reachable and it has to say so. Two endpoints' worth of work, and both of them
matter more than their size suggests.

**The address comes from outside.** A server that picks its own port is a
server nobody can run twice on one machine, or under a harness, or beside
anything else. Read `-addr` and listen there.

```go
addr := flag.String("addr", "127.0.0.1:8080", "the address to serve the API on")
```

**Health is three answers, not one.** The real server serves `/healthz`,
`/livez` and `/readyz`, and they mean different things:

- `livez` — *the process is not wedged.* A failure here means restart me.
- `readyz` — *send me traffic.* A failure here means take me out of the load
  balancer but leave me alone; a server that is still filling its caches is
  live and not ready.
- `healthz` — the older one, which is both at once. It is deprecated upstream
  and still what half the world checks.

Answering all three with `ok` is right for now. The distinction earns its keep
in the stage where this server has to load state from etcd before it can serve,
and there is a window where it is alive and has nothing to say.

**Every failure is an object.** This is the part people skip, and it shapes
everything downstream. When the real API server refuses, the body is a
`Status`:

```json
{
  "kind": "Status",
  "apiVersion": "v1",
  "metadata": {},
  "status": "Failure",
  "message": "the server could not find the requested resource: /nothing-here",
  "reason": "NotFound",
  "code": 404
}
```

`reason` is the machine-readable half and `message` is the human half. On the
other side of the wire, client-go turns this object back into a typed error,
and `apierrors.IsNotFound(err)` is a comparison against `reason`. A server that
answers `404 page not found` as plain text has given a client nothing to branch
on — the controller course's "ignore a missing object, retry on a conflict"
pattern simply cannot be written against it.

So: a `Status` for every failure, with the same code in the body as in the HTTP
status line, and `Content-Type: application/json`.

## Go APIs

- `net/http` is the whole of it. `http.ServeMux` in Go 1.22+ takes method and
  wildcard patterns — `mux.HandleFunc("GET /healthz", …)` — so routing needs no
  dependency.
- The `"/"` pattern is the catch-all: it matches anything no other pattern
  does, which is where the 404 belongs.
- `http.Server` with an explicit `Addr` and `Handler`, so shutdown is available:
  `srv.Shutdown(ctx)` stops accepting and lets in-flight requests finish.
- `signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)` gives you the
  context to hang that shutdown off. A harness — and a kubelet — stops a
  process with a signal, not by closing its stdin.
- `ListenAndServe` returns `http.ErrServerClosed` on a clean shutdown. Treating
  that as an error makes every clean stop look like a crash.

## Hints

<details><summary>Nudge</summary>

Three handlers and a catch-all. The only thing worth designing here is the
function that writes a failure, because every later stage will call it.
</details>

<details><summary>Approach</summary>

Write `writeJSON(w, code, body)` and `writeStatus(w, code, reason, message)`
first, then the handlers. Register `/healthz`, `/livez` and `/readyz` in a
loop, and `"/"` last for everything else.
</details>

## Further reading

- [Kubernetes API health endpoints](https://kubernetes.io/docs/reference/using-api/health-checks/)
- [`Status` in the API conventions](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#response-status-kind)
