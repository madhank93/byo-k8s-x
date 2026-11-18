---
title: How many, and nothing else
concepts: [subresources, scale-subresource, replicationcontroller, defaulting, autoscaling]
---

## Core concept

Everything so far has been one resource. A real API server serves dozens, and
they are mostly the same code: a store keyed by resource, namespace and name,
the same verbs, the same Status failures. **This stage adds a second one —
`replicationcontrollers`, kind `ReplicationController`, short name `rc`** —
and it has to do everything a configmap does: create, get, list with
selectors and pages, watch, PUT, the three patch dialects, apply, delete, and
leave with its namespace. If your handlers say "configmap" in a hundred
places, this is the stage that makes you take the word out.

A replication controller keeps N copies of a pod running. The server fills in
three fields on create:

- `spec.replicas` — how many. Absent means 1.
- `spec.selector` — which pods count as its own. Absent means a copy of
  `spec.template.metadata.labels`.
- `status.replicas` — how many it has seen. The server sets `{"replicas": 0}`
  on create whatever the body says, and the main PUT keeps the stored status,
  exactly as namespaces did last stage. `replicationcontrollers/status` is the
  way in for the controller.

**Then the real subject: `/scale`.** Think about what wants to change a
replica count. The HorizontalPodAutoscaler resizes Deployments, StatefulSets,
ReplicaSets, controllers and custom resources it has never heard of. `kubectl
scale` does the same. Neither can know every schema — where the count lives,
what the selector is called. So every scalable resource offers the same small
object:

```json
GET /api/v1/namespaces/team-a/replicationcontrollers/web/scale

{"kind":"Scale","apiVersion":"autoscaling/v1",
 "metadata":{"name":"web","namespace":"team-a","uid":"…",
             "resourceVersion":"41","creationTimestamp":"…"},
 "spec":{"replicas":3},
 "status":{"replicas":2,"selector":"app=web,tier=front"}}
```

**A Scale is a view, not a stored object.** Its metadata is the controller's
own; `spec.replicas` and `status.replicas` are the controller's; the selector
is `spec.selector` written as one label-selector string, pairs sorted by key,
so the autoscaler can list the pods with `?labelSelector=` and the same
selector is always the same string.

**Writing it writes one field.** `PUT …/scale` takes `spec.replicas` from the
body and nothing else — not the status, not the selector, not a name. The
controller's count changes, it gets a new resourceVersion, watchers see
`MODIFIED`, and the reply is the new Scale. That narrowness is why the
subresource exists for RBAC too: `update` on `replicationcontrollers/scale`
lets something resize a workload without letting it change the image.

The rules are the ones you already have:

- A `metadata.resourceVersion` in the Scale is a precondition on the
  controller: stale is 409 `Conflict`.
- A `metadata.name` that disagrees with the URL is 400 `BadRequest`.
- `spec.replicas` missing, negative or not an integer is 422 `Invalid`.
- No controller is 404 `NotFound`, on GET and PUT alike.
- Asking for the count it already has is not a write: the resourceVersion
  stays where it was, and no event goes out.

`kubectl scale --replicas=5 rc/web` sends a **PATCH**, a merge patch
`{"spec":{"replicas":5}}`. Apply it to the current Scale with the code you
already have, then write the result as if it had been PUT.

Discovery gains three entries: `replicationcontrollers`, its `/status`, and
`replicationcontrollers/scale` with `"group":"autoscaling","version":"v1",
"kind":"Scale"` and the verbs get, patch and update — a subresource can answer
a kind from another group, and the entry says which.

## Go APIs

- `slices.Sorted(maps.Keys(selector))` gives the keys in order;
  `strings.Join` the `k=v` pairs.
- A JSON number decodes into `any` as `float64`. Check it is whole and not
  negative before you store it, or decode into a struct with an `*int32` and
  let a nil pointer mean "missing".

## Hints

<details><summary>Nudge</summary>

Before writing anything about Scale, make a configmap a value of a type: a
resource name, a kind, a list kind, and a hook for create defaults. Register
your routes once per value. replicationcontrollers then costs a table entry.
</details>

<details><summary>Approach</summary>

Generalise the handlers over a `(resource, kind)` and route
`/api/v1/namespaces/{ns}/{resource}[/{name}]` and `/api/v1/{resource}`
through them; give the namespace delete a loop over every namespaced resource.
For `/scale`, write `toScale(obj)` and reuse your update path with a
`merge(stored, body)` that copies only `spec.replicas` across and returns the
stored object unchanged — no new version — when the count is the same. PATCH
is `toScale`, your patch function, then that same update.
</details>

## Further reading

- [Scale subresource](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#scale-subresource)
- [ReplicationController](https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller/)
- [Horizontal Pod Autoscaling](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- [kubectl scale](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_scale/)
