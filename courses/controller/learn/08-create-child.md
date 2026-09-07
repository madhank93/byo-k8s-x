---
title: Create the Deployment
concepts: [desired state, child resources, labels]
---

## Core concept

Here the controller stops observing and starts acting. A Website is a wish; a
Deployment is the thing that grants it. Every controller you will ever write is
a variation on the sentence this stage introduces: **read the wish, look at
what exists, make the difference go away.**

Note what is *not* here. There is no "on create, do X" branch. The pass asks
whether the Deployment exists and creates it if not, which means running the
same pass twice is harmless and running it after a restart is how the world
gets repaired.

The labels matter more than they look. They are the join between the objects a
Website owns — the Service two stages later selects on exactly these — so they
are defined once and used everywhere.

## Go APIs

- `kubernetes.NewForConfig(cfg)` for the typed clientset: Deployments have a Go
  type, so there is no reason to build them out of maps.
- `clientset.AppsV1().Deployments(ns).Get/Create`.
- `apierrors.IsNotFound(err)` to tell "not there" from "could not ask".
- `apierrors.IsAlreadyExists(err)` on create — two passes can race.

## Hints

<details><summary>Nudge</summary>

`Spec.Replicas` is a `*int32`. Take the address of a local variable, not of the
loop or parameter you were handed, if you ever build several in one pass.
</details>

<details><summary>Approach</summary>

Keep the "what should it look like" part in its own function that takes the
Website and returns a `*appsv1.Deployment`. Later stages compare against it and
patch towards it, and they will want it in one place.
</details>

<details><summary>Implementation</summary>

```go
_, err := deployments.Get(ctx, name, metav1.GetOptions{})
switch {
case err == nil:
    return nil
case !apierrors.IsNotFound(err):
    return fmt.Errorf("get deployment: %w", err)
}
_, err = deployments.Create(ctx, deploymentFor(site, image, replicas), metav1.CreateOptions{})
```
</details>

## Further reading

- [Deployment API](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/deployment-v1/)
- [Labels and selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/)
