---
title: Find what is already there
concepts: [restmapper, dynamic client, namespace scope]
---

## Core concept

A diff needs two sides, and the second one is on the server. To fetch it you
have to turn a manifest's `apiVersion` and `kind` into a URL, and that is
what the **RESTMapper** is for: discovery tells it that `apps/v1, Kind=Deployment`
is served as the resource `deployments`, and whether that resource is
namespaced.

The namespace is the part tools get wrong. An object is looked up where it
*would be applied*:

```
  manifest names a namespace?  ──yes──►  that one
             │ no
             ▼
  kind is namespaced?  ──yes──►  the kubeconfig context's namespace
             │ no
             ▼
          no namespace at all (Namespace, ClusterRole, …)
```

Get this wrong and the tool reports an object as missing when it is sitting
one namespace over — and a later apply creates a duplicate.

For this stage, add:

```
app get -f FILE
```

printing one line per object, `Kind/name present` or `Kind/name absent`. A
kind the server does not serve is an error (non-zero exit), never "absent".

## Go APIs

- `clientcmd.NewNonInteractiveDeferredLoadingClientConfig(...)`: its
  `ClientConfig()` gives the `rest.Config`, its `Namespace()` the context's
  namespace.
- `restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))`
  and `mapper.RESTMapping(gvk.GroupKind(), gvk.Version)`.
- `mapping.Scope.Name() == meta.RESTScopeNameNamespace`.
- `dynamic.NewForConfig(cfg).Resource(gvr).Namespace(ns).Get(...)`;
  `apierrors.IsNotFound` separates absent from broken.

## Hints

<details><summary>Nudge</summary>

`kubectl get namespace default -n kube-system` still works. Why, and what does
that tell you about where to look for a Namespace?
</details>

<details><summary>Approach</summary>

Write one helper that takes an object and returns the
`dynamic.ResourceInterface` it lives behind, filling in its namespace as it
goes. Every later stage reads and writes through it.
</details>

## Further reading

- [API concepts: resource URIs](https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-uris)
- [Namespaces: not all objects are in a namespace](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/#not-all-objects-are-in-a-namespace)
