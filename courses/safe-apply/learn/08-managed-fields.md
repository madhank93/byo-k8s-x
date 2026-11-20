---
title: Read who owns what
concepts: [managedFields, fieldsV1, field managers]
---

## Core concept

Server-side apply replaced the last-applied annotation with something much
richer: `metadata.managedFields`, one entry per manager and operation,
recording exactly which fields that manager set.

```yaml
managedFields:
- manager: byok8s
  operation: Apply
  fieldsType: FieldsV1
  fieldsV1:
    f:data:
      f:a: {}
      f:shared: {}
- manager: kubectl-edit
  operation: Update
  fieldsV1:
    f:data:
      f:e: {}
```

`fieldsV1` is a set of paths written as a tree. `f:name` is a map field,
`k:{"name":"app"}` a list item picked by its key, `v:...` a list item by
value, and `.` the node itself.

Two things to notice. A field can have **several owners**: two managers that
apply the same value share it, and it stays until both let go. And
**operation** matters: an `Apply` entry is a declaration that can conflict; an
`Update` entry (a PUT or a non-apply PATCH, which is what `kubectl edit` and
most controllers send) just takes the fields it wrote.

For this stage, add:

```
app owners -f FILE
```

For each object, print `Kind/name`, then one indented line per leaf field per
owner, sorted by path then manager:

```
ConfigMap/owned
  .data.a byok8s Apply
  .data.shared byok8s Apply
  .data.shared other-tool Apply
```

An object that does not exist prints `Kind/name absent`.

## Go APIs

- `obj.GetManagedFields()` gives `[]metav1.ManagedFieldsEntry`.
- `fieldpath.Set.FromJSON(bytes.NewReader(entry.FieldsV1.Raw))` from
  `sigs.k8s.io/structured-merge-diff/v6/fieldpath` decodes the tree.
- `set.Leaves().Iterate(func(p fieldpath.Path) {...})`; `p.String()` gives
  `.data.a`.

## Hints

<details><summary>Nudge</summary>

You could walk the `f:` tree by hand. The library the API server uses to
write it can also read it.
</details>

<details><summary>Approach</summary>

Collect (path, manager, operation) triples across every entry, then sort. The
helper you write here answers "who owns this field?" for three later stages.
</details>

## Further reading

- [Server-Side Apply: field management](https://kubernetes.io/docs/reference/using-api/server-side-apply/#field-management)
- [structured-merge-diff](https://github.com/kubernetes-sigs/structured-merge-diff)
