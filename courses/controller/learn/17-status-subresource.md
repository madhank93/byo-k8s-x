---
title: Report what you did
concepts: [status subresource, spec vs status, observedGeneration]
---

## Core concept

Spec is the wish. Status is the world. Keeping them apart is not tidiness — the
**status subresource** makes the API server enforce it: a write to the main
endpoint cannot change status, and a write to `/status` cannot change spec. Two
writers, two halves, no way to clobber each other by sending back a whole
object they had read a moment ago.

`observedGeneration` is the honest field. `metadata.generation` counts spec
changes; copying it into status says *which version of the wish this status
describes*. A client comparing the two can tell "everything is fine" from "I
have not caught up with your change yet" — and without it, those look
identical, which is how a stuck controller passes for a healthy one.

Writing status must not look like a spec change, or every status write would
queue another pass and the loop would never settle. The subresource is what
guarantees that: it does not bump the generation.

## Go APIs

- `spec.versions[].subresources.status: {}` in the CRD.
- `dyn.Resource(gvr).Namespace(ns).UpdateStatus(ctx, obj, metav1.UpdateOptions{})`.
- `site.GetGeneration()`.
- `unstructured.SetNestedField(obj.Object, value, "status", "replicas")` — the
  value must be `int64`, not `int`.

## Hints

<details><summary>Nudge</summary>

Adding the subresource to an existing CRD changes an object that is already
stored. Objects created before it keep working; their status is simply empty
until something writes one.
</details>

<details><summary>Approach</summary>

Read the object fresh from the API before writing status. The cached copy is a
snapshot from whenever the last event arrived, and a write built on a stale
resourceVersion is rejected.
</details>

<details><summary>Implementation</summary>

```go
updated, err := client.Get(ctx, name, metav1.GetOptions{})
if err != nil {
    return err
}
unstructured.SetNestedField(updated.Object, ready, "status", "replicas")
unstructured.SetNestedField(updated.Object, site.GetGeneration(), "status", "observedGeneration")
_, err = client.UpdateStatus(ctx, updated, metav1.UpdateOptions{})
```
</details>

## Further reading

- [Status subresource](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#status-subresource)
- [Spec and status](https://kubernetes.io/docs/concepts/overview/working-with-objects/kubernetes-objects/#object-spec-and-status)
