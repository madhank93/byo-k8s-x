---
title: The schema kubectl reads first
concepts: [openapi, kubectl-explain, client-side-validation, patch-strategy, http-caching]
---

## Core concept

Discovery, back in stage 2, tells a client *which* resources exist. It says
nothing about what is inside one. `kubectl explain configmap.data` has to get
its answer from somewhere, and so does `kubectl apply` when it refuses a
manifest with `replicas: "three"` before sending it. **That somewhere is the
OpenAPI document: a schema for every type the server serves.**

It is also where the merge rules of stage 19 are published. Your server knows
that `finalizers` merges as a set and `ownerReferences` merges by `uid`, but
kubectl builds the strategic patch on its own side, and it learns how each list
merges from the schema:

```json
"finalizers": {"type":"array","items":{"type":"string"},
               "x-kubernetes-patch-strategy":"merge"},
"ownerReferences": {"type":"array","items":{…},
               "x-kubernetes-patch-strategy":"merge",
               "x-kubernetes-patch-merge-key":"uid"}
```

**If the schema and the server disagree, kubectl builds patches the server
reads differently.** The `x-kubernetes-*` keys are Kubernetes' additions to
OpenAPI. Server-side apply reads the same kind of metadata (`x-kubernetes-list-type`,
`x-kubernetes-map-type`) to decide what one field is when it records ownership.

**The document is split per group-version, behind an index:**

```
GET /openapi/v3
{"paths":{"api/v1":{"serverRelativeURL":"/openapi/v3/api/v1?hash=4F1C…"}}}

GET /openapi/v3/api/v1?hash=4F1C…
{"openapi":"3.0.0","info":{"title":"Kubernetes","version":"…"},
 "paths":{"/api/v1/namespaces/{namespace}/configmaps/{name}":{"get":…,"put":…,…}},
 "components":{"schemas":{"io.k8s.api.core.v1.ConfigMap":{…},…}}}
```

The real server has dozens of group-versions, and the whole document runs to
megabytes. Nobody wants that on every `kubectl get`. **So the index gives each
group-version a URL with a hash of its document in it.** A URL that changes
whenever the content changes can be cached for ever: answer it with
`Cache-Control: … immutable`, and kubectl keeps its copy until the index names
a different hash. Then it fetches only the group-versions that moved.

That only works if the hash is stable. Build the document the same way every
time (Go's `encoding/json` sorts map keys, so a map is fine; a timestamp is
not), hash it once, and serve those exact bytes. A hash that changes for no
reason empties every client's cache.

**The document also answers conditional requests.** Send `ETag: "<hash>"`;
a request carrying `If-None-Match` with that tag gets `304 Not Modified` and no
body. A group-version the server doesn't have is a 404 Status, like any other
missing thing.

**Schemas are named after the Go types the real server generates them from**,
`io.k8s.api.core.v1.ConfigMap`, and each top-level kind carries
`x-kubernetes-group-version-kind`. That is how `kubectl explain configmap`
finds the schema. `metadata` is a `$ref` to the shared
`io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta`. A map with keys you can't
list in advance (`data`, `labels`, `spec.selector`) is `type: object` with
`additionalProperties: {type: string}`. Numbers say how wide they are:
`spec.replicas` is `integer` with `format: int32`.

## Go APIs

- Write the document as nested `map[string]any`, `json.Marshal` it once at
  startup, and keep the bytes.
- `crypto/sha512.Sum512(doc)`, then `strings.ToUpper(hex.EncodeToString(sum[:]))`.
- `r.Header.Get("If-None-Match") == etag` → `w.WriteHeader(http.StatusNotModified)`
  and return.
- `r.URL.Query().Get("hash") == hash` decides whether to add `immutable`.

## Hints

<details><summary>Nudge</summary>

You only need four schemas: ConfigMap, Namespace, ReplicationController and
ObjectMeta. Write helper functions for "string", "map of string" and
"$ref to X" and each schema fits on a screen.
</details>

<details><summary>Approach</summary>

Build `components.schemas` from those helpers. Build `paths` from a loop over
your routes, with an empty operation object per method. Marshal, hash, store.
`GET /openapi/v3` returns the index with the hash in the URL.
`GET /openapi/v3/api/v1` sets `ETag` and `Content-Type: application/json`,
checks `If-None-Match`, adds `Cache-Control: public, max-age=31536000, immutable`
when `?hash=` matches, and writes the stored bytes. Any other
`/openapi/v3/...` path is a 404 Status.
</details>

## Further reading

- [OpenAPI v3 in Kubernetes](https://kubernetes.io/docs/concepts/overview/kubernetes-api/#openapi-v3)
- [KEP-2896: OpenAPI v3](https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/2896-openapi-v3)
- [OpenAPI 3.0 specification](https://spec.openapis.org/oas/v3.0.3)
- [Merge strategy markers (x-kubernetes-*)](https://kubernetes.io/docs/reference/using-api/server-side-apply/#merge-strategy)
