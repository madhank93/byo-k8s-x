---
title: Answer inside the deadline, or be one
concepts: [timeoutseconds, latency, serial webhooks, context deadline]
---

## Core concept

Every request your rules match waits for you. That is the whole cost model of
admission, and `timeoutSeconds` is the bound on it:

```yaml
timeoutSeconds: 5
```

When you do not answer in time, the call has failed, and `failurePolicy`
decides the rest.

The number is small on purpose. Webhooks in the same configuration are called
in parallel, but *mutating* webhooks run one after another, and the API
server's own request deadline covers all of it. A chain of webhooks that each
take "only" ten seconds is how a cluster gets a write path measured in minutes.

What makes a webhook slow is almost never the judging. It is the API call you
made in the handler — reading a ConfigMap, asking another service, listing
pods. Every one of those is a round trip inside a request that is already
waiting on you, and any of them can hang.

The fixes are ordinary server engineering:

- **Do not call the API server from the handler.** Cache what you need with an
  informer and read the cache.
- **Bound everything you cannot avoid** with the request's own context, so a
  slow dependency fails fast instead of holding the deadline.
- **Answer, then work.** If something must happen afterwards, do it after the
  reply, not before it.
- **Set the deadline you actually meet**, not the maximum. The default is 10
  seconds and the ceiling is 30; a webhook that needs either is doing something
  in the wrong place.

One asymmetry is worth remembering: a timeout costs the *caller* the full
deadline, but your program may still be working. A handler that keeps running
after the API server has given up is a handler that can act on a decision
nobody used — which is exactly what `sideEffects` is asking about.

## Go APIs

- `ValidatingWebhook.TimeoutSeconds` — a `*int32`, 1 to 30.
- `r.Context()` in the handler: it is cancelled when the caller gives up.
- `context.WithTimeout` around anything the handler must call.
- `context.WithoutCancel` for work that must finish after you have replied,
  since the request's context dies with the reply.
- `http.NewResponseController(w).Flush()` — until you flush, the reply is still
  in the server's buffer and the caller is still waiting for you.
- `http.Server{ReadHeaderTimeout: …}` so a slow client cannot hold a
  connection open.

## What this stage checks

- The webhook declares a `timeoutSeconds` between 1 and 5 — the deadline you
  meet, not the ceiling.
- A pod annotated `byok8s.dev/delay: 60s` is judged slowly: the API server
  gives up and the write fails with `failed calling webhook`, in roughly the
  deadline rather than in sixty seconds.
- Your handler notices. When the caller's context is cancelled it stops waiting
  and prints a line containing `gave up`, instead of finishing a judgement
  nobody is left to read.
- Pods with no `owner` label are still refused.

The delay annotation is a teaching device — it exists so an outside observer can
watch a deadline expire. Nothing in production should offer callers a way to
make admission slower.

## Hints

<details><summary>Nudge</summary>

The handler's context is the API server's deadline. Passing it into every call
you make is what turns "hangs forever" into "fails in time to be handled".
</details>

<details><summary>Approach</summary>

Make the wait selectable, not unconditional: read a duration from the pod's
`byok8s.dev/delay` annotation and sleep only when it is there. Then `select` on
that timer against `r.Context().Done()`, so the sleep ends either when it is
over or when the API server stops caring — whichever comes first.

Watching it fail is the point. Set `timeoutSeconds` to something you comfortably
meet, then send a pod asking for sixty seconds and time how long the write takes
to be refused.
</details>

## Further reading

- [Timeouts](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#timeouts)
- [Admission webhook good practices: latency](https://kubernetes.io/docs/concepts/cluster-administration/admission-webhooks-good-practices/)
