---
title: Create from a manifest
concepts: [manifests, gvk vs gvr, restmapper, write verbs]
---

## What this stage teaches

A manifest is **self-describing**: it carries `apiVersion` and `kind`, which is
why `kubectl create -f` needs no flag saying what is in the file. Your program
has to make use of that, and three things happen in order:

```
  YAML bytes
     │ unmarshal into unstructured
     ▼
  object + GVK  (apps/v1, Deployment)      ← what the file says it is
     │ RESTMapper
     ▼
  GVR  (apps/v1, deployments)              ← what the URL needs
     │ + namespace
     ▼
  POST /apis/apps/v1/namespaces/{ns}/deployments
```

Namespace precedence is its own decision: what the manifest says wins, and the
context or `-n` only fills the gap. A manifest that names a namespace means it,
and quietly relocating someone's object is worse than refusing.

Create is not idempotent. Running it twice gives **409 AlreadyExists**, which
is the correct behaviour and the thing stage 22 changes.

## Go you'll reach for

- `yaml.Unmarshal(data, &obj.Object)` from `sigs.k8s.io/yaml`, into
  `unstructured.Unstructured`.
- `obj.GroupVersionKind()`, then
  `mapper.RESTMapping(gvk.GroupKind(), gvk.Version)` — note this is the *GVK*
  direction of the mapper, unlike stage 13's resource lookup.
- `mapping.Resource` is the GVR; `mapping.Scope` says whether it is namespaced.
- `dyn.Resource(mapping.Resource).Namespace(ns).Create(ctx, obj, metav1.CreateOptions{})`.

## Hints

<details><summary>Nudge</summary>

The file already says what it is. Your job is to translate that into the two
things a URL needs: a resource and a namespace.
</details>

<details><summary>Approach</summary>

Unmarshal into `&obj.Object` (the map), not into `obj` itself. Then check
`gvk.Kind != ""` before mapping — a YAML file with a typo'd `kind:` produces a
confusing mapper error otherwise.
</details>

<details><summary>The API</summary>

```go
gvk := obj.GroupVersionKind()
mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)

target := ns
if in := obj.GetNamespace(); in != "" { target = in }
obj.SetNamespace(target)

created, err := dyn.Resource(mapping.Resource).Namespace(target).
    Create(ctx, obj, metav1.CreateOptions{})
```
</details>

## Going deeper

- [Understanding Kubernetes objects](https://kubernetes.io/docs/concepts/overview/working-with-objects/)
- [meta.RESTMapping](https://pkg.go.dev/k8s.io/apimachinery/pkg/api/meta#RESTMapping)
