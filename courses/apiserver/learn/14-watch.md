---
title: The request that never ends
concepts: [watch, streaming, watch-events, informers, flushing]
---

## Core concept

Every answer so far has been one request, one response, done. **A watch is one
request that stays open and reports every change as it happens.**

```
GET /api/v1/namespaces/default/configmaps?watch=true

{"type":"ADDED","object":{...}}
{"type":"MODIFIED","object":{...}}
{"type":"DELETED","object":{...}}
```

It is the same URL as the list, with a flag on it: same resource, same
namespace, the same selectors — a different shape of answer.

**This is what Kubernetes is made of.** Every controller, the scheduler, the
kubelet and kube-proxy are a cache filled from a list and kept in step by a
watch. Nothing in the system polls. A cluster where 5,000 kubelets asked "any
changes?" every second would spend its entire apiserver budget answering "no".

**Four event types, and the object is the whole object.** `ADDED`, `MODIFIED`,
`DELETED` (`BOOKMARK` arrives two stages from now). Each carries the object as
it then was — a client builds its whole cache out of these and never fetches an
object individually. `DELETED` carries the object as it last was, so a
controller learns *what* went, not merely that something did.

**A watch with no `resourceVersion` starts by sending everything.** The client
said nothing about what it holds, so it holds nothing, and every stored object
is new to it: synthetic `ADDED` events, then the live stream. The next stage is
where the client gets to say otherwise.

**Register the watcher and take the snapshot under one lock.** Snapshot first
and you miss whatever is written in between; subscribe first and you send an
object twice. This is the single place a hand-written apiserver goes wrong.

**Flush every event.** An event sitting in a `bufio.Writer` until the buffer
fills is an event the client has not been told about. A watch that arrives in
batches is a watch nothing can react to — `http.Flusher` after every line.

**An open watch must not stop the server.** Holding the store's lock for the
life of a stream deadlocks the first write that follows. The lock covers
registering the watcher; the stream itself runs off a channel. And a write must
never wait on a client's socket: buffer per watcher, and drop a watcher that
fills its buffer. Its stream ends, and the contract is that it lists again and
starts over — one slow client stalling every write is how an apiserver falls
over.

**Scope and selectors apply.** A watch is scoped exactly like the list it was
opened on, and `fieldSelector` and `labelSelector` filter the events too. This
is what makes the kubelet's `spec.nodeName=<me>` watch proportional to its node
rather than to the cluster.

## Go APIs

- `w.(http.Flusher)` — `Flush()` after each event; no `Content-Length` anywhere.
- `r.Context().Done()` fires when the client hangs up. That is the signal to
  unregister the watcher.
- One buffered `chan` per watcher; a non-blocking `select` with a `default` that
  closes and drops the channel is the whole of the slow-client policy.
- Publish from inside the store's write methods, while the lock is held, so
  watchers see writes in the order the store applied them.
- `json.Marshal` plus `'\n'` — `json.Encoder` writing to the `ResponseWriter`
  works too; the flush is what matters.

## Hints

<details><summary>Nudge</summary>

The store grows a map of watcher channels and one `publish` call at the end of
each write. The handler branches on `?watch=true` before it does any paging.
</details>

<details><summary>Approach</summary>

Add `watchers map[int]chan watchEvent` to the store, and a `watchFrom` that
takes the lock once to register a channel *and* snapshot the current objects.
Call `publish` at the end of `create`, `update` and `remove` (including one
event per object the namespace cascade removed), with a non-blocking send that
closes and deletes any channel that is full. In the handler: write the 200 and
the JSON content type, send the snapshot as `ADDED`, then loop on a `select` over
the channel and `r.Context().Done()`, filtering each event by resource,
namespace and the selectors, writing one line per event and flushing.
</details>

## Further reading

- [API concepts: efficient detection of changes](https://kubernetes.io/docs/reference/using-api/api-concepts/#efficient-detection-of-changes)
- [client-go: informers and the shared cache](https://github.com/kubernetes/sample-controller/blob/master/docs/controller-client-go.md)
- [`watch.Event`](https://pkg.go.dev/k8s.io/apimachinery/pkg/watch#Event)
