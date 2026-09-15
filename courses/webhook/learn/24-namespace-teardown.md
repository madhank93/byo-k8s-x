---
title: Let the namespace go
concepts: [namespace-deletion, namespace-controller, delete, terminating]
---

## Core concept

Stage 22 taught the webhook to refuse the deletion of a protected pod. It does
exactly that — including when nobody meant to delete that pod on its own.

`kubectl delete namespace` does not remove what is inside. It marks the
namespace `Terminating`, and the **namespace controller** then deletes its
contents, one resource type at a time, through the same API everyone else
uses. Its pod deletes pass through admission like anybody's. Your webhook
refuses the protected one, the controller retries, and the namespace stays
where it is with a condition like:

```
NamespaceDeletionContentFailure  Failed to delete all resource types, 1 remaining:
  pods "..." is forbidden: admission webhook "pods.byok8s.dev" denied the request: ...
```

It stays that way until someone deletes the webhook configuration by hand,
which is usually how a team finds out.

The protection was never about the namespace. The label says *do not take this
pod away on its own*. When the whole namespace is going, there is nothing left
for the pod to be part of, and refusing protects nothing.

There are two ways to recognise the teardown, and one is sturdier:

- **Recognise the caller.** In a kubeadm cluster the controller is
  `system:serviceaccount:kube-system:namespace-controller`, and stage 18's
  match conditions could let it through without a round trip. But the name
  depends on how the controller manager was started, and the controller is not
  the only one deleting: once its delete is admitted, the pod shuts down and
  the kubelet removes it with a second DELETE of its own, as
  `system:node:<node>`. Exempt only the controller and that second delete is
  refused — the pod is stuck terminating, and so is the namespace.
- **Recognise the situation.** A namespace being torn down has
  `status.phase: Terminating`. Every delete inside it — the controller's, the
  kubelet's, anyone's — is a delete of something that is going anyway. Look
  the namespace up, and let it through.

The lookup is a round trip, so pay for it only when you are about to refuse. A
delete you were going to allow does not need it.

## Go APIs

- `cs.CoreV1().Namespaces().Get(ctx, req.Namespace, metav1.GetOptions{})`, then
  `ns.Status.Phase == corev1.NamespaceTerminating`.
- `req.UserInfo.Username` says who is asking: useful in a log line, fragile as
  a policy.
- The controller deletes pods as a collection, and each pod is admitted with
  `req.Name` empty. The name is in `oldObject`; a message built from
  `req.Name` says `pod ""`.
- `kubectl get ns <name> -o jsonpath='{.status.conditions}'` shows what is
  holding a namespace open.

## Hints

<details><summary>Nudge</summary>

Who deletes the pods in a namespace you delete? And once a pod has stopped,
who deletes it again?
</details>

<details><summary>Approach</summary>

Keep the stage 22 check exactly as it is. Before a refusal of a DELETE goes
out, look up the request's namespace; if it is `Terminating`, allow instead.
If the lookup fails, the refusal stands.
</details>

## Further reading

- [Namespaces](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/)
- [Termination of Pods](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination)
