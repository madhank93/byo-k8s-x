---
title: What this server can do
concepts: [discovery, apigroups, apiresourcelist, restmapper, verbs]
---

## Core concept

`kubectl get cm` works. Nothing in kubectl knows what a ConfigMap is — not the
plural, not the namespace, not the short name. It asks the server, and the
server's answer is what turns three letters into
`GET /api/v1/namespaces/default/configmaps`.

That conversation is **discovery**, and it is three endpoints.

**`GET /api`** — the core group's versions. The core group has no name, which
is a historical accident you inherit: everything else lives under `/apis/<group>`,
and the oldest resources live directly under `/api`.

```json
{"kind": "APIVersions", "versions": ["v1"]}
```

**`GET /apis`** — every *named* group and its versions. You have none yet, and
the empty list still has to be served:

```json
{"kind": "APIGroupList", "apiVersion": "v1", "groups": []}
```

A 404 here is not "no groups". A client reads a 404 as a broken server and
stops, so the difference between an empty list and a missing endpoint is the
difference between a working cluster and an unusable one.

**`GET /api/v1`** — what that version actually offers:

```json
{
  "kind": "APIResourceList",
  "groupVersion": "v1",
  "resources": [
    {
      "name": "configmaps",
      "singularName": "configmap",
      "namespaced": true,
      "kind": "ConfigMap",
      "shortNames": ["cm"],
      "verbs": ["create", "delete", "get", "list", "update", "watch"]
    }
  ]
}
```

Every field there does a job:

- **`name`** is the URL segment. Not a label — the path is built from it.
- **`namespaced`** decides the *shape* of the path: `/api/v1/configmaps` or
  `/api/v1/namespaces/<ns>/configmaps`. Get this wrong and every request from
  every client goes to the wrong URL.
- **`kind`** is what goes in the object's `kind` field, and what kubectl prints.
- **`verbs`** is what may be done. kubectl hides a command whose verb is not
  listed, and RBAC is written in terms of these words.
- **`shortNames`** is where `cm` comes from.

**This is why Kubernetes is extensible at all.** A CRD installed this morning
appears in this list, and a kubectl compiled two years ago drives it correctly
— because kubectl never knew any of the built-in types either. The machinery
that caches these answers and maps a word to a URL is the **RESTMapper**, which
the kubectl course builds from the client side.

## Go APIs

- Three more `mux.HandleFunc` calls, and a JSON writer you already have.
- The shapes are `metav1.APIVersions`, `metav1.APIGroupList` and
  `metav1.APIResourceList` in `k8s.io/apimachinery`. You can import them for
  the field names, or write the maps by hand while the server serves one
  resource — this course adds apimachinery when a stage needs more from it than
  a struct tag.
- Keep the resource list in one place. Later stages add resources, and a
  discovery document that disagrees with what the handlers serve is worse than
  one that is out of date, because clients believe it.

## Hints

<details><summary>Nudge</summary>

Nothing is stored yet, and discovery does not care: it describes what the
server *would* do, which is why you can write it before the handlers it
describes.
</details>

<details><summary>Approach</summary>

Three handlers returning constant JSON. Serve `/api` and `/apis` first, then
`/api/v1` with one entry for configmaps — the resource stage 3 stores.
</details>

## Further reading

- [API concepts: discovery](https://kubernetes.io/docs/reference/using-api/api-concepts/)
- [API groups and versioning](https://kubernetes.io/docs/reference/using-api/#api-groups-and-versioning)
