---
title: Record events
concepts: [events, event recorder, aggregation, observability]
---

## Core concept

Status says what is true now. **Events** say what happened, and they are what
`kubectl describe` shows at the bottom — the first place anyone looks when
something is not working.

They are deliberately cheap and deliberately lossy: events expire (an hour by
default), identical ones are aggregated into a count rather than stored
repeatedly, and nothing should ever be derived from them programmatically. They
are a log for people, attached to the object they are about.

Which makes the discipline about *what* to record: transitions, not passes.
"Created Deployment blog" is worth an event. "Reconciled" every ten seconds is
noise that will bury the one line that mattered, and will do it while the
cluster is busiest.

`Normal` versus `Warning` is the other half — a Warning is a thing a human
should look at, and using it for routine outcomes is how people learn to ignore
your controller's events.

## Go APIs

- `record.NewBroadcaster()`, `.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: clientset.CoreV1().Events("")})`.
- `broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "byok8s-controller"})`.
- `recorder.Eventf(object, corev1.EventTypeNormal, "Created", "Deployment %s", name)`.
- `broadcaster.Shutdown()` on the way out, or the last events are never sent.

## Hints

<details><summary>Nudge</summary>

The recorder needs an object it can build a reference from — the unstructured
Website works, because it carries its own apiVersion and kind.
</details>

<details><summary>Approach</summary>

You already know when something changed: the `Ready` condition either moved or
it did not. Record there, where the transition is visible, rather than at the
end of a pass that cannot tell the difference.
</details>

<details><summary>Implementation</summary>

```go
broadcaster := record.NewBroadcaster()
broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: clientset.CoreV1().Events("")})
recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "byok8s-controller"})
defer broadcaster.Shutdown()
```
</details>

## Further reading

- [Event API](https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/event-v1/)
- [client-go record](https://pkg.go.dev/k8s.io/client-go/tools/record)
