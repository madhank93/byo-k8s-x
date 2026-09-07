---
title: Add the Service
concepts: [multiple children, selectors, service, composition]
---

## Core concept

One wish, several objects. A Website is not "a Deployment with extra steps" —
it is a thing a person wants, and granting it takes a Deployment *and* a
Service, and later possibly an Ingress, a PodDisruptionBudget, a
ServiceAccount.

Each child is reconciled the same way, which is what keeps this from turning
into a script: what should exist, what does exist, close the gap. Adding a
child is adding one more call in the pass, not a new mechanism.

The Service is also where a controller can produce something silently broken.
Its selector has to match the labels the Deployment stamps on its *pods* — not
the labels on the Deployment object itself. Get that wrong and you have a
Service with no endpoints and no error anywhere.

## Go APIs

- `clientset.CoreV1().Services(ns).Get/Create`.
- `corev1.ServiceSpec{Selector, Ports}` with `intstr.FromInt32(80)` for the
  target port.
- The same `ownerRef(site)`, so the Service is collected with everything else.

## Hints

<details><summary>Nudge</summary>

Define the label set in one function and call it from both children. Two
literals that must agree will eventually stop agreeing.
</details>

<details><summary>Implementation</summary>

```go
svc := &corev1.Service{
    ObjectMeta: metav1.ObjectMeta{
        Name: name, Namespace: ns, Labels: labels,
        OwnerReferences: []metav1.OwnerReference{ownerRef(site)},
    },
    Spec: corev1.ServiceSpec{
        Selector: labels,
        Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt32(80)}},
    },
}
```
</details>

## Further reading

- [Service](https://kubernetes.io/docs/concepts/services-networking/service/)
- [Debugging Services: no endpoints](https://kubernetes.io/docs/tasks/debug/debug-application/debug-service/)
