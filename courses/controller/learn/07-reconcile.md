---
title: Reconcile, don't react
concepts: [level triggered, edge triggered, idempotence, deleted objects]
---

## Core concept

A reconcile function is handed a key and answers one question: *given how the
world is right now, what should I do?* It does not ask what happened. The event
that queued the key is a hint that something may have changed — nothing more.

This is the difference between **level-triggered** and **edge-triggered**
logic, and it is the whole reason Kubernetes controllers are reliable. An
edge-triggered controller that misses one event is wrong forever. A
level-triggered one that misses an event is wrong until the next pass, and
every later pass repairs it.

Two consequences follow immediately:

- The function must be **idempotent**. It will run again on the same
  unchanged object, many times, and that must be a no-op.
- "The object is gone" is a normal outcome, not an error. A key that no longer
  resolves means the world should now contain nothing for it.

## Go APIs

- `cache.SplitMetaNamespaceKey(key)` → namespace, name.
- `lister.ByNamespace(ns).Get(name)` and `apierrors.IsNotFound(err)`.
- `unstructured.NestedString(obj.Object, "spec", "image")` and
  `NestedInt64` for reading the spec out of an untyped object.

## Hints

<details><summary>Nudge</summary>

A key that will never parse — as opposed to one whose object is missing — is
not worth retrying. Drop it rather than requeueing it forever.
</details>

<details><summary>Approach</summary>

Read the object first, handle "gone" first, and only then look at the spec.
Every later stage inserts itself into that same order.
</details>

<details><summary>Implementation</summary>

```go
obj, err := lister.ByNamespace(ns).Get(name)
if apierrors.IsNotFound(err) {
    fmt.Printf("reconcile %s gone\n", key)
    return nil
}
```
</details>

## Further reading

- [Level triggering and reconciliation](https://kubernetes.io/blog/2021/06/21/writing-a-controller-for-pod-labels/#level-triggered-vs-edge-triggered)
- [Kubernetes controllers, the design](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-api-machinery/controllers.md)
