---
title: Get anything at all
concepts: [dynamic client, unstructured, clients]
---

## Core concept

This is the largest single step in the course: your program stops being a pod
client. The dynamic client speaks in GVRs and `unstructured.Unstructured` —
a `map[string]any` with helpers — instead of Go types, so one code path serves
pods, ConfigMaps and a CRD nobody had written when you compiled.

```
  typed:     cs.CoreV1().Pods(ns).List(...)   → *corev1.PodList   (compile-time)
  dynamic:   dyn.Resource(gvr).Namespace(ns).List(...) → *unstructured.UnstructuredList
```

What you give up is real: no compile-time checking, and every field access is a
map lookup that can miss. What you gain is that the *set of resources you
support* is decided at runtime by the server rather than at build time by your
imports.

`Unstructured` still knows about the metadata every object shares —
`GetName()`, `GetNamespace()`, `GetLabels()`, `GetCreationTimestamp()` — so the
table code barely changes. Anything below metadata needs
`unstructured.NestedString(obj.Object, "spec", "nodeName")`, which returns
value, found, and an error for "found but the wrong type" separately.

This is a trade, not an upgrade. A controller that owns its own CRD should use
a generated typed client; a general-purpose tool cannot.

## Go APIs

- `dynamic.NewForConfig(cfg)` and `dyn.Resource(gvr).Namespace(ns)`.
- `unstructured.NestedString` / `NestedSlice` / `NestedMap`.
- The GVR comes from the mapper you built in stage 13.

## Hints

<details><summary>Nudge</summary>

The list type changes, but the methods your table calls — `GetName`,
`GetCreationTimestamp` — exist on both.
</details>

<details><summary>Approach</summary>

Replace the typed fetch with `dyn.Resource(gvr).Namespace(ns).List(...)` and
keep everything downstream working on the shared metadata accessors. Resist
converting the unstructured object back into a typed one; that reintroduces
exactly the compile-time knowledge you are removing.
</details>

<details><summary>Implementation</summary>

```go
dyn, err := dynamic.NewForConfig(cfg)
list, err := dyn.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
for _, o := range list.Items { fmt.Println(o.GetName()) }
```
</details>

## Further reading

- [dynamic client](https://pkg.go.dev/k8s.io/client-go/dynamic)
- [unstructured](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1/unstructured)
