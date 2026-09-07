---
title: Keep watching
concepts: [watch, resourceversion, controllers, streaming]
---

## Core concept

A watch is a long-lived HTTP response the server keeps appending to, one JSON
event per change: `ADDED`, `MODIFIED`, `DELETED`, `BOOKMARK`, `ERROR`. The
mechanism is simple; the correctness is all in where you start.

List and watch race. If you list, print, and then open a watch, anything that
changed in between is lost — silently, and only under load. The fix is the
`resourceVersion` the *list* carries: it marks the exact point that snapshot was
consistent, and a watch started from it begins precisely where the list ended.

```
   list ──► items + resourceVersion=1042
                        │  "start the stream exactly here"
                        ▼
   watch(resourceVersion=1042) ──► ADDED    pod-c   (rv 1043)
                                   MODIFIED pod-a   (rv 1051)
                                   DELETED  pod-b   (rv 1060)
                                        ⋮
                                   ERROR 410 Gone   ← history expired
```

That is **list-then-watch**, and every informer, operator and controller in the
ecosystem is built on it. The `410 Gone` is not a bug: the server keeps a
bounded history, so a client that falls behind is told to relist rather than
served a gap it cannot detect.

One more sharp edge: an `ERROR` event's `Object` is a `*metav1.Status`, not
your resource. A type assertion that assumes otherwise panics the first time a
watch expires — which is exactly the moment you were not watching.

## Go APIs

- `dyn.Resource(gvr).Namespace(ns).Watch(ctx, metav1.ListOptions{ResourceVersion: rv})`.
- `list.GetResourceVersion()` — the collection's version, not any item's.
- `for event := range w.ResultChan()` and `defer w.Stop()`.
- Flush the tabwriter after every row; a buffered stream that prints nothing
  for ten minutes looks like a hang.

## Hints

<details><summary>Nudge</summary>

The list you already printed is carrying the one value that makes the stream
correct.
</details>

<details><summary>Approach</summary>

Print the list as usual, then pass `list.GetResourceVersion()` into the watch's
`ListOptions`. Keep the same selectors on both calls, or the stream will not
match the snapshot.
</details>

<details><summary>Implementation</summary>

```go
obj, ok := event.Object.(*unstructured.Unstructured)
if !ok {
    continue // an Error event carries a Status, not your object
}
```

For production code you would also handle `410 Gone` by relisting; here,
noticing why it happens is enough.
</details>

## Further reading

- [Efficient detection of changes](https://kubernetes.io/docs/reference/using-api/api-concepts/#efficient-detection-of-changes)
- [watch.Interface](https://pkg.go.dev/k8s.io/apimachinery/pkg/watch#Interface)
