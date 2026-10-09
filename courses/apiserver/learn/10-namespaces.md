---
title: The object every other object is in
concepts: [namespaces, cluster-scoped, all-namespaces, cascade-delete]
---

## Core concept

Until now a namespace has been a word in a URL, and any word would do. That is
the gap this stage closes: **a namespace is an object**, stored like every other
one, and being in one has to mean something.

```
POST /api/v1/namespaces            {"kind":"Namespace","metadata":{"name":"team-a"}}
GET  /api/v1/namespaces            → NamespaceList
GET  /api/v1/namespaces/team-a     → the object, status.phase Active
DELETE /api/v1/namespaces/team-a   → and everything in it goes too
```

**Cluster-scoped means no namespace segment.** A Namespace cannot be in a
namespace — it is what a namespace field refers to. So its URLs are
`/api/v1/namespaces/<name>`, and its stored object has no `metadata.namespace`.
Discovery says `namespaced: false`, and that one field is how every client
decides which of the two URL shapes to build. Get it wrong and `kubectl` asks for
`/api/v1/namespaces/default/namespaces/team-a`.

**A write into a namespace nobody created is a 404** — `namespaces "nowhere" not
found`, naming the namespace rather than the object. Allowing it leaves objects
in a place no quota covers, no delete sweeps and nothing lists. Note the
asymmetry, which is real behaviour and not an oversight: a **list** in a
namespace that does not exist is still `200` with an empty list. Reading finds
nothing; writing needs somewhere to put it.

**A cluster is born with four namespaces:** `default`, `kube-system`,
`kube-public`, `kube-node-lease`. The server creates them, not whoever uses it —
`default` above all, because a kubeconfig that names no namespace means that one,
and a server without it 404s the simplest request anyone makes. Create them only
when there are none, or a restart makes them again on top of what it read back.

**One resource, two URLs.** `/api/v1/configmaps` is every configmap in the
cluster; `/api/v1/namespaces/<ns>/configmaps` is one namespace's. Same `kind`
in the answer, so a client decodes both with the same code, and `kubectl get cm
-A` is the first URL. This is nearly free once keys are `/registry/<resource>/<ns>/<name>`:
the cluster-wide list is the shorter prefix. In a cluster-wide answer each object's
own `metadata.namespace` is the only thing telling two same-named objects apart.

**Deleting a namespace is not deleting a folder.** In a real cluster the delete
marks it `Terminating`, the namespace controller deletes everything inside, and
only when the last finalizer clears does the namespace itself go — which is why a
stuck namespace hangs in `Terminating` forever and is the most-searched Kubernetes
question there is. Here it is one write. The rule it stands for is what matters:
nothing outlives the namespace it was in, or the next `team-a` inherits
somebody's old data at a URL that briefly did not exist.

## Go APIs

- Add the resource as a segment in the key and pass it into every store method:
  `create("configmaps", ns, name, obj)`, `get("namespaces", "", name)`. An empty
  namespace is both "cluster-scoped" and "every namespace", which is the same
  thing to a prefix scan.
- Sort a list by **key** rather than by name now: namespace-then-name is the
  order etcd returns a range in, and it is what a cluster-wide list should print.
- `mux` tells `GET /api/v1/namespaces/{name}` and
  `GET /api/v1/namespaces/{namespace}/configmaps` apart by shape — no ordering
  care needed.

## Hints

<details><summary>Nudge</summary>

Nothing new is needed in the store but a resource segment in the key. Everything
else is handlers, plus one existence check on the configmap create.
</details>

<details><summary>Approach</summary>

Make `registryKey(resource, namespace, name)` and `listPrefix(resource,
namespace)`, thread `resource` through the store methods, and drop
`metadata.namespace` when the namespace is empty. Add the four namespace
handlers, stamp `status.phase: Active` on create, and have the namespace delete
also drop every key under `/registry/configmaps/<name>/`. Then `bootstrap()` at
startup when no namespaces exist, the configmap create checks the namespace is
there, and `GET /api/v1/configmaps` lists with an empty namespace.
</details>

## Further reading

- [Namespaces](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/)
- [Namespaces walkthrough: resources in a namespace](https://kubernetes.io/docs/tasks/administer-cluster/namespaces/)
- [Finalizers](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/)
