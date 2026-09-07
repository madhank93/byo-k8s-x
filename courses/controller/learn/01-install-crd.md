---
title: Teach the cluster a new kind
concepts: [custom resources, crd, dynamic client, established condition]
---

## Core concept

A Kubernetes API is not a fixed list. A **CustomResourceDefinition** is an
ordinary object that, once stored, makes the API server serve a whole new
resource: `/apis/byok8s.dev/v1alpha1/namespaces/<ns>/websites`, with the same
create, list, watch, patch and delete semantics every built-in kind has. You
are not writing a server. You are describing a kind, and getting one.

That is why controllers ship their own definition. A controller whose CRD has
to be applied by hand before it starts is a controller with an undocumented
setup step; one that installs it is a program you can just run.

Creating it is not enough. The API server needs a moment to wire up storage
for the new kind, and until it reports the **Established** condition a request
for `websites` can still come back 404. The wait is the stage.

## Go APIs

- `clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides)` —
  the same kubeconfig rules every Kubernetes tool follows, plus `.Namespace()`
  for the namespace the context selects.
- `dynamic.NewForConfig(cfg)` — a client for any resource, including one whose
  Go type does not exist. A CRD is such a resource, so the client that will
  read Websites can also create the definition of them.
- `dyn.Resource(crdGVR)` where the GVR is
  `apiextensions.k8s.io/v1, customresourcedefinitions`.
- `wait.PollUntilContextCancel` for the Established wait.

## Hints

<details><summary>Nudge</summary>

Build the definition as an `unstructured.Unstructured` — a `map[string]any`
that mirrors the YAML you would otherwise apply. Every number in it must be an
`int64` and every fraction a `float64`, because the object is JSON.
</details>

<details><summary>Approach</summary>

Create it if it is missing, update it if it is not — a controller restarts, and
the second start must not fail because the first one succeeded. An update has
to carry the `resourceVersion` you read, so `Get` first and copy it over.

Then poll until `status.conditions` holds `Established: "True"`.
</details>

<details><summary>Implementation</summary>

```go
conds, _, _ := unstructured.NestedSlice(got.Object, "status", "conditions")
for _, c := range conds {
    cond, _ := c.(map[string]any)
    if cond["type"] == "Established" && cond["status"] == "True" {
        return true, nil
    }
}
```
</details>

## Further reading

- [Extend the Kubernetes API with CustomResourceDefinitions](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/)
- [dynamic.Interface](https://pkg.go.dev/k8s.io/client-go/dynamic#Interface)
