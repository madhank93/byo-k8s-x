---
title: Install a release
concepts: [releases, ownership metadata, dynamic client, kubeconfig namespace]
---

## Core concept

Everything so far has been `helm template`. Now the manifests go to the
cluster:

```
app install [-n NS] [-f FILE]... [--set k=v]... NAME DIR
```

renders the chart exactly as `template` does, then creates every document,
in install order, in the release's namespace. Three things make it an
*install* rather than `kubectl create`:

1. **The namespace comes from the kubeconfig.** Without `-n`, the release
   lives in the current context's namespace, the one kubectl would use, and
   `.Release.Namespace` says so (switch `template`'s default too: Helm's
   does the same). A namespaced object whose manifest names no namespace
   goes there; a cluster-scoped one has none.

2. **Every object is marked as the release's.** Helm adds, at install time
   and not in the rendered manifest:

   ```yaml
   metadata:
     labels:
       app.kubernetes.io/managed-by: Helm
     annotations:
       meta.helm.sh/release-name: NAME
       meta.helm.sh/release-namespace: NS
   ```

3. **It never takes over what it does not own.** Before creating anything,
   look each object up. One that exists without exactly those three marks
   stops the install, and nothing is created, not even the objects that come
   earlier in install order. Helm's error lists every mark that is wrong:

   ```
   Error: INSTALLATION FAILED: Unable to continue with install: ConfigMap "taken" in namespace "shop" exists and cannot be imported into the current release: invalid ownership metadata; label validation error: missing key "app.kubernetes.io/managed-by": must be set to "Helm"; annotation validation error: missing key "meta.helm.sh/release-name": must be set to "web"; annotation validation error: missing key "meta.helm.sh/release-namespace": must be set to "shop"
   ```

   A mark with the wrong value reads `key "meta.helm.sh/release-name" must
   equal "web": current value is "other"`.

The release name must be a lowercase DNS name of at most 53 characters
(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`);
`template` checks that too.

On success, print Helm's summary. `LAST DEPLOYED` is Go's `time.ANSIC`,
and `NOTES:` appears only when the top chart has a `templates/NOTES.txt`
that renders to something. Render it with the other templates, then print
it trimmed; a subchart's notes are left out.

```
NAME: web
LAST DEPLOYED: Sat Nov 21 00:08:14 2026
NAMESPACE: shop
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
Thanks for installing web.
```

## Go APIs

- `clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{})`
  reads the kubeconfig as kubectl does; `.Namespace()` is the context's
  namespace and `.ClientConfig()` the connection.
- `dynamic.NewForConfig` creates objects of any kind as
  `*unstructured.Unstructured`; `restmapper.NewDeferredDiscoveryRESTMapper`
  turns a kind into the resource that serves it and says whether it is
  namespaced.
- `apierrors.IsNotFound` tells "absent" from a real failure on `Get`.

## Hints

<details><summary>Nudge</summary>

Two loops over the objects: the first only reads, the second only creates.
</details>

<details><summary>Approach</summary>

Pull the render pipeline out of `template` into one function both commands
call. Unmarshal each manifest into an `Unstructured`, map its kind, fill in
the namespace. Check ownership of every one that exists, then set the marks
and `Create` each in order.
</details>

## Further reading

- [helm install](https://helm.sh/docs/helm/helm_install/)
- [Helm's ownership check](https://github.com/helm/helm/blob/v3.18.6/pkg/action/validate.go)
