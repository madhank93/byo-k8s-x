---
title: The half of the object that isn't yours
concepts: [subresources, status-subresource, spec-vs-status, rbac, optimistic-concurrency]
---

## Core concept

Most Kubernetes objects come in two halves. **`spec` is what someone asked
for; `status` is what a controller saw.** A Deployment's spec says three
replicas; its status says two are ready. A namespace has no spec worth the
name, but it has the other half: `status.phase`, which the server set to
`Active` back in stage 10.

The two halves have different owners, and one PUT that replaces the whole
object cannot respect that. Picture it: a user reads a namespace, a controller
then writes a new phase, and the user sends back their copy with one label
changed. A plain update stores the user's copy — including the phase as it was
when they read it — and the controller's write is gone. The resourceVersion
check catches this only when the user sends one — `kubectl replace` does not —
and even then a status that changes every few seconds turns every label edit
into a race the user keeps losing.

**So the object gets a second endpoint, and each one takes only its half:**

```
PUT /api/v1/namespaces/team-a          labels, annotations, spec — status ignored
PUT /api/v1/namespaces/team-a/status   status only — everything else ignored
GET /api/v1/namespaces/team-a/status   the whole object, as GET without /status
```

Both take the whole object as their body, and both answer with the whole
object as stored. What changes is what they copy from the body. Through the
main endpoint, the stored status is kept whatever the body says; through
`/status`, the stored metadata is kept and only `status` comes from the body.

**Two endpoints mean two things RBAC can grant.** A rule names a resource,
and `namespaces/status` is a resource name of its own. A controller is given
`update` on `deployments/status` and nothing on `deployments`; a user is given
the reverse. Neither can write the other's half, however wrong the copy they
are holding. One endpoint could not express that.

**Everything else about a write is unchanged.** There is still one
resourceVersion for the whole object: a stale one on either PUT is 409
`Conflict`, and a write through `/status` moves it and reaches watchers as
`MODIFIED`, because it goes through the same store. A missing object is 404 on
both; a name in the body that disagrees with the URL is 400 `BadRequest`.

**Discovery lists a subresource as its own entry.** `namespaces` gains the
`update` verb, and beside it:

```json
{"name":"namespaces/status","singularName":"","namespaced":false,
 "kind":"Namespace","verbs":["get","update"]}
```

The slash in the name is how a client knows it is a subresource, and `kind` is
`Namespace` because what goes in and out of it is a whole Namespace.

This is not special to namespaces. Pods, Deployments, Nodes and every custom
resource that declares a status subresource work exactly this way: `kubectl
edit` goes through the main endpoint and cannot move a pod's phase, and the
kubelet writes that phase through `pods/status`.

## Go APIs

- `mux.HandleFunc("PUT /api/v1/namespaces/{name}/status", …)` sits beside
  `PUT /api/v1/namespaces/{name}`; the patterns do not overlap.
- Both handlers are the same read-check-write as the configmap PUT. Before
  writing, copy one field across: `obj["status"] = stored["status"]` for the
  main endpoint, or start from `stored` and set `status` from the body for
  `/status`.

## Hints

<details><summary>Nudge</summary>

Write one update helper that takes the stored object and the body and returns
what to store, and pass a different one to each route. The precondition, the
404, the name check and the watch event are all the existing update path.
</details>

<details><summary>Approach</summary>

Generalise the configmap PUT into a function over (resource, namespace, name)
plus a `merge(stored, body) object`. The namespace PUT's merge is the body with
the stored `status` put back; the status PUT's merge is a copy of the stored
object with the body's `status`. Keep the identity fields from stored either
way, as before. Add `update` to the namespaces verbs and the
`namespaces/status` entry to discovery.
</details>

## Further reading

- [Kubernetes object spec and status](https://kubernetes.io/docs/concepts/overview/working-with-objects/#object-spec-and-status)
- [API conventions: spec and status](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#spec-and-status)
- [Custom resources: the status subresource](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#status-subresource)
- [RBAC: referring to subresources](https://kubernetes.io/docs/reference/access-authn-authz/rbac/#referring-to-resources)
