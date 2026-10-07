---
title: The client you did not write
concepts: [table-output, content-negotiation, field-validation, openapi-parameters, kubectl]
---

## Core concept

Every stage so far was graded by requests the harness wrote. This one hands
the real `kubectl` a kubeconfig pointing at your server — your TLS
certificate, alice's client certificate — and grades what it makes of you:

```
kubectl apply -f -                    client-side, then again, then changed
kubectl get configmaps [-A --show-labels]
kubectl apply -f - (a misspelt field) refused, warned, or ignored
kubectl edit configmap settings       KUBE_EDITOR is a script
kubectl apply --server-side -f -      and a conflict with another manager
kubectl scale replicationcontroller web --replicas=5
kubectl get replicationcontrollers --watch
```

Most of it already works: apply is the patches and server-side apply you
built, scale is the subresource, edit is a GET and a strategic merge patch.
Two things are new.

**The server prints the table.** `kubectl get` does not know what columns a
ConfigMap has. It asks the server, in the Accept header:

```
Accept: application/json;as=Table;v=v1;g=meta.k8s.io,
        application/json;as=Table;v=v1beta1;g=meta.k8s.io,
        application/json
```

The first media type you can produce wins. Answer the Table and kubectl lays
it out; answer the plain list and kubectl falls back to `NAME` and `AGE`. A
Table is `meta.k8s.io/v1`, with `columnDefinitions` (name, type, format —
`name` for the name column) and one `rows[]` entry per object: the `cells`,
in column order, and an `object` holding a `PartialObjectMetadata` with the
object's metadata. kubectl fills `NAMESPACE` for `-A` and `LABELS` for
`--show-labels` from that, not from the cells.

```
configmaps               NAME  DATA  AGE              DATA = keys in data + binaryData
replicationcontrollers   NAME  DESIRED  CURRENT  READY  AGE
anything else            NAME  AGE
```

A GET by name asks for a Table too, and gets one row. So does the watch
behind `--watch`: each event's `object` is a one-row Table, so a change
prints as a line under the ones before. Bookmarks stay as they are.

**The server checks the field names.** A manifest with `dta:` for `data:` is a
typo that, unchecked, is silently dropped. kubectl will not check it itself if
the server says it will: it looks in `/openapi/v3` for a PATCH of that kind —
found by the `x-kubernetes-group-version-kind` on the operation — that takes a
`fieldValidation` query parameter. Finding none, it looks for `/openapi/v2` to
check the manifest itself, and on this server fails. Finding it, every write
carries the directive, and only the server stands between the typo and storage:

```
?fieldValidation=Strict   400, "unknown field \"dta\"" in the message, nothing stored
?fieldValidation=Warn     stored without the field; Warning: 299 - "unknown field \"dta\""
?fieldValidation=Ignore   stored without the field, silently
(absent)                  as before — nothing checked
```

The schema you publish is the one you check against: a key the schema has no
property for is unknown, a map or free-form object takes any key, and a
strategic merge patch's `$`-keys are directives, not fields. The check runs
on creates, updates and patches alike — `kubectl apply` of an object that
exists arrives as a PATCH.

## Go APIs

- `mime.ParseMediaType(part)` on each comma-separated part of `Accept`; the
  `as`, `v` and `g` come back in the params map.
- `w.Header().Add("Warning", ...)` once per problem; kubectl prints each one.
- `exec.LookPath("kubectl")` — the grader needs one on `PATH`, the version in
  `mise.toml`.

## Hints

<details><summary>Nudge</summary>

Run it yourself before the grader does. Write a kubeconfig with your CA and a
client certificate, run `kubectl get configmaps -v=8`, and read the requests
it makes and the headers it sends: every failure here is visible there first.
Note that kubectl will not send a bearer token over plain HTTP — that is why
this stage runs over TLS.
</details>

<details><summary>Approach</summary>

One function decides "did this read ask for a Table": walk `Accept` in order,
return true at `application/json` with `as=Table;v=v1;g=meta.k8s.io`, false
at a plain `application/json` or `*/*`. The list handler, the get handler and
the watch's send all consult it and wrap their objects with one `tableOf`.
For validation, add the GVK and a `fieldValidation` parameter to each write
operation in the OpenAPI document, then walk each write body beside the
schema, collecting the paths of keys the schema does not name — deleting them
unless the directive is Strict.
</details>

## Further reading

- [Receiving resources as Tables](https://kubernetes.io/docs/reference/using-api/api-concepts/#receiving-resources-as-tables)
- [Field validation](https://kubernetes.io/docs/reference/using-api/api-concepts/#field-validation)
- [kubectl apply, declaratively](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/declarative-config/)
- [Warnings](https://kubernetes.io/blog/2020/09/03/warnings/)
