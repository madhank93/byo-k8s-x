---
title: Ask for it by name
concepts: [get, notfound, status, namespaces]
---

## Core concept

A get is the cheapest thing this server does, and every other verb is built on
it. An update reads before it writes. A watch is a get that does not end.
`kubectl get configmap settings -o yaml` is this endpoint with a printer
attached.

```
GET /api/v1/namespaces/default/configmaps/settings
200 OK   → the stored object, exactly as it is stored

GET /api/v1/namespaces/default/configmaps/absent
404 Not Found
{"kind":"Status","apiVersion":"v1","status":"Failure",
 "message":"configmaps \"absent\" not found","reason":"NotFound","code":404}
```

**The object answers with its own `apiVersion` and `kind`.** It has just been
detached from the URL that described it — written to a file, piped into
something, held in a variable — and those two fields are all that is left to say
what it is. This is why `kubectl get -o yaml` output can be fed straight back
into `kubectl apply`.

**Nothing about the object changes between the write and the read.** Not the
`uid`, not the `creationTimestamp`, not the `resourceVersion`. A get that minted
a fresh uid would break every owner reference in the cluster, and no error
message anywhere would say so — the objects would simply stop being the objects
that were referenced.

**The key is the namespace and the name together.** `settings` in `default` and
`settings` in `kube-system` are two unrelated objects. If the namespace is not
part of your lookup, every namespace in the cluster is the same namespace, and
the isolation the whole model rests on is gone.

**404 `NotFound` is a load-bearing answer, not a failure to handle.** It is what
client-go turns into the error `IsNotFound(err)` asks about, and "get it, and if
it is not there, create it" is most of what a reconcile loop does. The reason
field is what a program branches on; the message is for the person reading the
terminal.

## Go APIs

- `mux.HandleFunc("GET /api/v1/namespaces/{namespace}/configmaps/{name}", …)`.
  Two wildcards, both read with `r.PathValue`. This pattern does not collide
  with the collection one — Go's mux prefers the more specific of the two.
- One more method on the store, taking the lock the same way the create does:
  a read that races a write sees half of it otherwise.

## Hints

<details><summary>Nudge</summary>

The store already knows how to build a key from a namespace and a name — the
create does it. The get is the same key and one map lookup.
</details>

<details><summary>Approach</summary>

`get(namespace, name) (object, bool)` under the lock. The handler reads both
path values, calls it, and either writes 200 with the object or the 404 Status
your `writeStatus` already produces.
</details>

## Further reading

- [API concepts: resources and sub-resources](https://kubernetes.io/docs/reference/using-api/api-concepts/#standard-api-terminology)
- [Status and reasons in the API machinery](https://github.com/kubernetes/apimachinery/blob/master/pkg/apis/meta/v1/types.go)
