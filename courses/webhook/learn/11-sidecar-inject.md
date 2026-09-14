---
title: Add a container nobody asked for
concepts: [sidecar injection, idempotence, json patch append, opt-in]
---

## Core concept

Injecting a container is the most useful and most dangerous thing a mutating
webhook does. It is how service meshes, log shippers and secret agents get into
pods that know nothing about them — and it is a stranger editing a workload
someone else owns.

Mechanically it is one patch operation:

```json
{"op": "add", "path": "/spec/containers/-", "value": {"name": "sidecar", "image": "…"}}
```

The `-` means append. Everything difficult about this stage is around that line.

**Idempotence is not optional here.** If you append without checking, a second
call gives the pod two identical containers and the API server rejects it for
duplicate names — a confusing error that points at the pod, not at you. Look
for your container by name first, and do nothing if it is there.

**Opt-in, not opt-out.** A webhook that injects into everything will eventually
inject into something that cannot tolerate it. The convention is an annotation
or label the workload sets, enforced by `objectSelector` so the request never
reaches you otherwise.

**Pods are mostly immutable.** You can add a container at admission; you cannot
add one to a running pod. That is why injection happens here rather than in a
controller, and why the pod that exists is the pod you decided on.

**A sidecar is a real container.** It gets scheduled, counts toward requests,
and — unless it is a native sidecar, an init container with
`restartPolicy: Always` — can keep a Job's pod from ever completing, because
the pod is only Succeeded when every container has exited.

**Never inject into your own namespace.** If your webhook's pods get a sidecar
that depends on your webhook being up, you have built a restart deadlock.

## Go APIs

- `{"op": "add", "path": "/spec/containers/-", "value": corev1.Container{…}}`.
- `slices.ContainsFunc(pod.Spec.Containers, func(c corev1.Container) bool { … })`
  to make the patch conditional.
- `pod.Annotations["byok8s.dev/inject"]` as the opt-in switch.
- `corev1.Container{RestartPolicy: &always}` on an init container, for the
  native-sidecar shape.

## Hints

<details><summary>Nudge</summary>

The patch you send is built from the pod *as it arrived*. If a previous webhook
already added your container, it is in that object — which is exactly what
makes the check work.
</details>

<details><summary>Approach</summary>

Decide in two steps: does this pod want a sidecar, and does it already have
one? Only the second question needs the object; the first is what the selector
should have answered for you.
</details>

## Further reading

- [Sidecar containers](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/)
- [Reinvocation policy](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#reinvocation-policy)
