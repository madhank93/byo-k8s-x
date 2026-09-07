---
title: Change one field
concepts: [patch types, write verbs, concurrency]
---

## Core concept

Patch is the smallest write verb: the body names only what changes, so nothing
has to be read first and there is no `resourceVersion` to conflict on. Two
patches to different fields never collide — which is what you want — and a
patch that races a controller wins silently, which is not always.

The **patch type** is the real content of this stage. Four exist and they are
not interchangeable:

| Type | Body | Notes |
|---|---|---|
| **JSON merge** (RFC 7386) | a partial object | replaces whole values; `null` deletes a key. Patching one list element replaces the *whole list* |
| **Strategic merge** | a partial object | Kubernetes-only: knows each list's merge key, so it can add a container without dropping the others |
| **JSON patch** (RFC 6902) | a list of ops | `add`/`remove`/`replace` with paths; precise and verbose |
| **Apply** | the full manifest | stage 22 |

Strategic merge is the one people assume is universal. It is not: the merge
keys come from the Go struct tags of the built-in types, so the apiserver has
no strategy for a CRD — a strategic patch against a custom resource is
rejected, and merge patch is what works there.

## Go APIs

- `types.MergePatchType`, `types.StrategicMergePatchType`, `types.JSONPatchType`.
- `dyn.Resource(gvr).Namespace(ns).Patch(ctx, name, types.MergePatchType, []byte(body), metav1.PatchOptions{})`.

## Hints

<details><summary>Nudge</summary>

The patch body arrives from the command line as a string; it goes to the server
as bytes, unparsed by you.
</details>

<details><summary>Approach</summary>

Use `types.MergePatchType` — it works on every resource, custom ones included.
Try `{"metadata":{"labels":{"tier":null}}}` to see deletion, and try patching a
single container in a multi-container pod to see the list-replacement footgun
first-hand.
</details>

## Further reading

- [Update API objects in place using kubectl patch](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/)
- [RFC 7386 — JSON Merge Patch](https://datatracker.ietf.org/doc/html/rfc7386)
