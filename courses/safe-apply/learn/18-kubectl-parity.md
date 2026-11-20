---
title: Agree with kubectl
concepts: [server-side apply, managedFields, client-side apply, migration]
---

## Core concept

This course is graded the way any apply tool should be: apply the same
manifests through your tool and through `kubectl apply --server-side`, then
require the cluster to end up the same, `managedFields` included. Give kubectl
your manager name with `--field-manager=byok8s` and, on objects that only
server-side apply has ever touched, the two tools already agree. Both send
the whole object as an apply patch, and the server does the rest.

They disagree on objects that client-side `kubectl apply` wrote first, which
in a real cluster is most of them. Client-side apply writes with a plain
`Update` under the manager `kubectl-client-side-apply` and keeps its record in
the `last-applied-configuration` annotation (stage 3). Apply server-side over
such an object and that `Update` manager still owns every field. Drop a field
from the manifest and it stays, because only you let go of it. Change a value
and you get a conflict with a manager no one runs any more:

```
ConfigMap/cfg: conflict: .data.k owned by "kubectl-client-side-apply"
```

`kubectl apply --server-side` migrates the object instead. After its apply,
it looks for `Update` managers that own the last-applied annotation, the mark
of client-side apply. It merges their field sets into its own `Apply` entry
with a JSON patch, then applies again, so the dropped fields go with the
second apply:

```
  managedFields before                    after
  ─────────────────────────────────────   ──────────────────────────────
  byok8s                     Apply        byok8s   Apply   (everything)
  kubectl-client-side-apply  Update
```

The annotation goes too. After the merge, your manager owns it and your
manifest doesn't mention it. Your tool has to do the same, as `byok8s`, after
every real apply. Once it does, `kubectl diff --server-side` finds nothing to
change after your apply, and `kubectl apply --server-side` after yours writes
nothing at all: the resourceVersion stays the same.

The migration code is in client-go, and kubectl uses it from there:
`k8s.io/client-go/util/csaupgrade`.

## Go APIs

- `csaupgrade.FindFieldsOwners(managedFields, metav1.ManagedFieldsOperationUpdate, set)`:
  the entries that own the paths in `set`. Build the set with
  `fieldpath.NewSet(fieldpath.MakePathOrDie("metadata", "annotations", lastApplied))`.
- `csaupgrade.UpgradeManagedFieldsPatch(obj, managers, "byok8s")`: the JSON
  patch, or nil when there is nothing to do. It includes a `test` on the
  resourceVersion, so it can't land on an object that changed meanwhile.
- `r.Patch(ctx, name, types.JSONPatchType, patch, metav1.PatchOptions{})`.

## Hints

<details><summary>Nudge</summary>

Start from the object the apply returned. It already has the managedFields
the migration has to rewrite, so you don't need another read.
</details>

<details><summary>Approach</summary>

Write one function that runs after each real apply, in `apply` and in
`apply --plan`. Find the client-side managers. If there are none, return.
Otherwise, send the upgrade patch and apply the same object once more,
without the plan's pins: you have just written it yourself.
</details>

<details><summary>Implementation</summary>

```go
names := sets.New[string]()
for _, e := range csaupgrade.FindFieldsOwners(live.GetManagedFields(),
    metav1.ManagedFieldsOperationUpdate, lastAppliedPath) {
    names.Insert(e.Manager)
}
patch, err := csaupgrade.UpgradeManagedFieldsPatch(live, names, c.manager)
```
</details>

## Further reading

- [Upgrading from client-side apply to server-side apply](https://kubernetes.io/docs/reference/using-api/server-side-apply/#upgrading-from-client-side-apply-to-server-side-apply)
- [csaupgrade](https://pkg.go.dev/k8s.io/client-go/util/csaupgrade)
