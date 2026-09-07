---
title: Apply, not create
concepts: [server-side apply, field managers, declarative config]
---

## Core concept

Apply is not "create if missing, update if present". It is a different claim:
*these fields should look like this, and I am the one saying so.* The server
records a **field manager** against every field your request sets, and keeps
that record in `metadata.managedFields`.

```
   your manifest (fieldManager: byok8s)      live object, by owner
   ─────────────────────────────────────     ────────────────────────────────
   spec.replicas: 3                    ───►  spec.replicas   byok8s
   spec.template.spec.containers[0]    ───►  spec.template   byok8s
                                             status.*        deployment-controller

   next apply drops spec.replicas
        └─► "I no longer manage this"  ───►  the field is removed
```

That last arrow is what a create-or-update loop can never get right. Updating
with the full object cannot distinguish "a field someone else set" from "a
field I deleted", so it either clobbers other managers or leaks removed fields
forever. Ownership is the extra information that makes convergence possible.

Mechanically it is a **PATCH** with content type `application/apply-patch+yaml`
whose body is the manifest itself, unchanged. The patch *type* carries the
whole mechanism.

The field manager string is an **identity**, not a label. Two tools that share
a name share ownership and neither can tell.

## Go APIs

- `types.ApplyPatchType` from `k8s.io/apimachinery/pkg/types`.
- `dyn.Resource(gvr).Namespace(ns).Patch(ctx, name, types.ApplyPatchType, body, metav1.PatchOptions{FieldManager: "byok8s"})`.
- The body is the manifest serialised as JSON — YAML is accepted too; JSON is a
  subset.
- `FieldManager` is **required** for apply; the server rejects the request
  without it.

## Hints

<details><summary>Nudge</summary>

Everything you built for `create` still applies — the decode, the mapper, the
namespace rule. Only the final call changes, and it is not a call to `Create`.
</details>

<details><summary>Approach</summary>

Apply addresses an object by name, so `obj.GetName()` goes into the Patch call
and the body is the whole manifest. Run it twice and diff `managedFields` in
`-o yaml`; that is the stage's real output.
</details>

<details><summary>Implementation</summary>

```go
body, err := json.Marshal(obj.Object)
applied, err := dyn.Resource(gvr).Namespace(target).Patch(
    ctx, obj.GetName(), types.ApplyPatchType, body,
    metav1.PatchOptions{FieldManager: "byok8s"})
```
</details>

## Further reading

- [Server-Side Apply](https://kubernetes.io/docs/reference/using-api/server-side-apply/)
- [managedFields](https://kubernetes.io/docs/reference/using-api/server-side-apply/#field-management)
