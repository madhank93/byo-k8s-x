---
title: Attach its events
concepts: [events, field selectors, object references]
---

## What this stage teaches

Events are the cluster's explanation of *why*, and the surprise is that they
are not part of the object they describe. An Event is its own resource in its
own namespace, pointing back through `involvedObject`:

```
  Pod web-7d9f          Event
  ├─ uid: 9c1e-...  ◄── involvedObject.uid:  9c1e-...
  └─ name: web-7d9f ◄── involvedObject.name: web-7d9f
                        reason: Scheduled / Pulled / BackOff
                        (expires on its own, ~1h)
```

Two consequences fall out of that relationship. Events expire independently, so
a describe of a day-old pod correctly shows none — the object is fine, its
history is simply gone. And fetching them is a **second request**, with a field
selector on `involvedObject`. Listing every event in the namespace and
filtering in your loop would work and would also pull down every other
object's history.

Match on the **uid**, not just the name. Names are reused, and a pod deleted
and recreated under the same name would otherwise inherit its predecessor's
events — a genuinely misleading bug, since the events would look plausible.

Sort by timestamp: events are a narrative, and a narrative out of order is
worse than none.

## Go you'll reach for

- `fields.AndSelectors(fields.OneTermEqualSelector("involvedObject.name", n), ...)`
  and `.String()` to render it for the request.
- `cs.CoreV1().Events(ns).List(ctx, metav1.ListOptions{FieldSelector: sel})` —
  the typed client is fine here; Event is a core type that will not surprise you.
- `e.Type`, `e.Reason`, `e.LastTimestamp`, `e.Source.Component`, `e.Message`.

## Hints

<details><summary>Nudge</summary>

`involvedObject` is a struct in the Event, and its subfields are among the ones
the apiserver indexes — which is what makes stage 17's mechanism apply here.
</details>

<details><summary>Approach</summary>

Build the selector from name AND uid, list, sort by `LastTimestamp`, and print
`<none>` when the list is empty rather than an empty table.
</details>

<details><summary>The API</summary>

```go
selector := fields.AndSelectors(
    fields.OneTermEqualSelector("involvedObject.name", obj.GetName()),
    fields.OneTermEqualSelector("involvedObject.uid", string(obj.GetUID())),
).String()
```

To generate some, create a pod with an image that does not exist and describe
it.
</details>

## Going deeper

- [Event v1 core](https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/event-v1/)
- [fields](https://pkg.go.dev/k8s.io/apimachinery/pkg/fields)
