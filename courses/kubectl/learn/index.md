---
title: Build your own kubectl — primer
concepts: [kubeconfig, rest api, discovery, resources, clients, controllers]
---

## The mental model

kubectl is not special. It is an HTTP client with good manners: it finds a
server, works out what that server serves, turns what you typed into a URL,
sends a request, and prints the answer. Everything the cluster can do is
reachable with `curl` and a token — kubectl exists because the URL is tedious
to build and the answer is tedious to read.

So the course is really about four questions, asked over and over:

1. **Which server, as whom?** — kubeconfig, contexts, the REST config.
2. **What is this thing called?** — kinds, resources, groups, versions.
3. **What am I asking for?** — get, list, watch, create, apply, patch, delete.
4. **How should it look?** — tables, JSON, YAML, describe.

Nothing in your program will hardcode a resource after stage 13. That is the
arc: you start with a pod lister and end with a client that can address a
resource type that did not exist when you compiled it.

## Finding the cluster

`rest.Config` is the connection: a host, a CA, credentials, timeouts. You
never build one by hand — the loading rules do it, and they are the reason
your tool behaves the same as kubectl on someone else's laptop.

```
  $KUBECONFIG (colon-separated, merged left to right)
        │  else
        ▼
  ~/.kube/config
        │
        ▼
  ┌─────────────────────┐     overrides
  │ merged  kubeconfig  │ ◄── --context   (which context)
  └──────────┬──────────┘     --namespace (which namespace)
             ▼
      current context ──► cluster (server URL, CA)
             │        └─► user    (token, client cert, exec plugin)
             ▼
        namespace:  --namespace  >  context's namespace  >  "default"
```

The middle step of that last line is the one everyone forgets. A context can
carry a namespace; a tool that reads only the flag works perfectly for its
author, whose context is unset, and quietly acts in the wrong namespace for
everyone else.

## Kinds, resources, and the two triples

The API has two vocabularies for the same thing and you need both.

| | Looks like | Where it lives |
|---|---|---|
| **GVK** — GroupVersionKind | `apps/v1, Kind=Deployment` | inside objects: a manifest's `apiVersion` + `kind` |
| **GVR** — GroupVersionResource | `apps/v1, Resource=deployments` | inside URLs: `/apis/apps/v1/namespaces/web/deployments` |

A manifest is self-describing because it carries its GVK. A request is
addressed by GVR. Converting one to the other is the **RESTMapper**'s job, and
it can only do it by asking the server — because the mapping includes
shortnames (`po`, `deploy`) declared by whoever defined the resource,
including a CRD installed five minutes ago.

The URL shape is worth memorising, because every stage builds one:

```
  /api/v1/namespaces/{ns}/pods/{name}          core group: no /apis, no group name
  /apis/apps/v1/namespaces/{ns}/deployments    every other group
  /apis/apps/v1/deployments                    no namespace segment = every namespace
  /api/v1/namespaces/{ns}/pods/{name}/log      a subresource of the object
```

Two consequences fall straight out of that picture. Listing across all
namespaces is not a filter, it is a *shorter path*. And a subresource — `/log`,
`/exec`, `/scale`, `/status` — is its own address, which is exactly how RBAC
grants "may read logs" without granting "may edit pods".

## Discovery: how a client works with types it has never seen

`GET /apis` returns the groups, `GET /apis/apps/v1` returns the resources in
one. That is discovery, and it is the whole reason kubectl can print a CRD it
has never heard of. Your program will call it three ways: directly
(`api-resources`), through the RESTMapper (resolving what the user typed), and
implicitly (deciding whether a resource is namespaced).

Discovery **partially fails** on real clusters — an aggregated API whose
backend is down fails its own group and nothing else. Reporting what did
answer beats failing the command.

## Typed clients versus the dynamic client

| | Typed (`kubernetes.Clientset`) | Dynamic (`dynamic.Interface`) |
|---|---|---|
| Speaks | Go structs, `*corev1.Pod` | `unstructured.Unstructured` — `map[string]any` |
| Addressed by | a method per resource, `CoreV1().Pods(ns)` | any GVR at runtime |
| Knows a CRD? | only if you generated code for it | always |
| Compile-time safety | yes | none: typos become runtime lookups |

Neither is the winner. Pick typed when you know the type at compile time —
a controller for your own CRD — and dynamic when you do not, which is every
general-purpose tool. This course does both, and switching from one to the
other at stage 14 is the single largest step in it.

## Reading state: list, then watch

`GET` returns state now. `WATCH` streams changes. Used separately they race:
list, then start watching, and anything that changed in the gap is lost
forever. The fix is the `resourceVersion` on the *list*, which is a consistent
snapshot marker for the whole collection.

```
   list ──► items + resourceVersion=1042
                        │
                        │  "start the stream exactly here"
                        ▼
   watch(resourceVersion=1042) ──► ADDED    pod-c   (rv 1043)
                                   MODIFIED pod-a   (rv 1051)
                                   DELETED  pod-b   (rv 1060)
                                   ...
                                   Error: 410 Gone  ← history expired: relist
```

That is list-then-watch, and every informer, every controller and every
`kubectl get -w` in existence is built on it. The `410 Gone` at the end is not
a bug: the server only keeps so much history, so a client that falls behind is
told to start over with a fresh list.

## Writing state: the three verbs and who owns a field

```
  create   "make this"            fails if it exists
  patch    "change these fields"  no read first, no conflict detection
  apply    "these fields should look like this, and I am saying so"
```

Apply is the interesting one. It is not "create or update": the server records
a **field manager** against every field your request sets, and remembers it.

```
   manifest (manager: byok8s)        live object with ownership
   ────────────────────────────      ─────────────────────────────────
   spec.replicas: 3            ───►  spec.replicas   owned by byok8s
   spec.template...            ───►  spec.template   owned by byok8s
                                     status.*        owned by the controller
                                     spec.replicas   also wanted by hpa  ⚠ conflict

   drop a field from the manifest, re-apply
        └─► "I no longer manage this" ─► the server deletes it
```

Two things follow. Removing a field from your manifest actually removes it
from the cluster, which a create-or-update loop can never get right. And when
two managers claim one field the server returns **409 Conflict** — a question,
not a fault. `--force-conflicts` answers it by taking ownership, which is right
when a human is correcting a mistake and wrong when the other manager is a
controller that will simply set it back.

## The stage arc

```
   1–3    find the cluster, choose a context, ask the server what it is
   4–10   list pods: namespaces, tables, ages, JSON, YAML, every namespace
  11–15   one object, discovery, the RESTMapper, the dynamic client, wide
  16–19   select on the server: labels, fields, stable order, watch
  20–25   write: delete, create, apply, conflicts, patch, scale
  26–27   describe, and the events that belong to an object
  28–30   the three that leave the apiserver's comfort zone: logs, exec,
          port-forward — all of which reach a kubelet
```

Stages 28–30 are different in kind. Everything before them is a JSON request
against etcd-backed state; those three open a *stream* the apiserver proxies to
a node. That is why they need a real cluster, and why they are last.

## Going deeper

- [Kubernetes API concepts](https://kubernetes.io/docs/reference/using-api/api-concepts/)
- [client-go](https://pkg.go.dev/k8s.io/client-go) and its [examples](https://github.com/kubernetes/client-go/tree/master/examples)
- [Server-Side Apply](https://kubernetes.io/docs/reference/using-api/server-side-apply/)
- [Organizing cluster access with kubeconfig](https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/)
