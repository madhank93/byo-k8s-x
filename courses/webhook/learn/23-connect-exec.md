---
title: Guard every way in
concepts: [connect, exec, attach, subresources, podexecoptions]
---

## Core concept

Stages 21 and 22 closed the two ways a protected pod could be taken away.
Neither stops anyone getting inside it. `kubectl exec` creates, updates and
deletes nothing: it opens a stream to a process in a running container.
Admission has a fourth operation for exactly this — **`CONNECT`**.

A connect is made against a subresource, and stage 6 already said a rule on
`pods` does not match one. So the rule names them:

```yaml
operations: ["CONNECT"]
resources: ["pods/exec", "pods/attach"]
```

The second name is the one people forget. `pods/exec` starts a new process in
the container; `pods/attach` joins the one already running, its stdin and its
output. Refuse exec, allow attach, and you have locked one door and left the
one beside it open. (`pods/portforward` is a third, into the pod's network
rather than its processes. This stage does not grade it, but a real policy
should decide about it on purpose rather than by omission.)

Now the trap, and it is a quiet one. Your webhook has carried an
`objectSelector` since stage 8, the opt-out label. Add the connect rule to that
same webhook and nothing happens: `kubectl exec` walks straight in, and your
program never logs a request.

The object of a connect is a `PodExecOptions` or a `PodAttachOptions` — which
container, which command, whether stdin is wanted. It has no metadata at all.
The API server matches an `objectSelector` against the labels of the object
and the old object, and when there is no metadata to read, it counts that as
*no match*. Not "no labels, so `DoesNotExist` holds": no match. A webhook with
any selector at all is skipped for every connect, and nothing tells you.

So connects get a webhook entry of their own. One `ValidatingWebhookConfiguration`
can hold several: same URL, same CA bundle, same namespace selector and match
conditions, and no `objectSelector`. The opt-out label does not apply to
connects, which is the right answer anyway — a label anyone can set on a pod
should not be what decides who gets a shell in it.

Once the request does arrive, it is the eviction trap a second time: read the
options as a pod and every connect looks like a pod with no labels. Same fix —
look the pod up by name.

The verdict comes before the stream exists. The API server asks your webhook
first, and only an allowed request is upgraded and proxied on to the kubelet.
That is what makes a connect enforceable at all: a refusal costs the caller
nothing, and a wrong admission hands over a shell.

## Go APIs

- `admissionregistrationv1.Connect` in the rule's `Operations`, with
  `pods/exec` and `pods/attach` in its `Resources`.
- `req.Operation == admissionv1.Connect`, with `req.SubResource` set to
  `"exec"` or `"attach"`.
- `req.Object` holds the options, not the pod; `req.Name` is the pod's name.
- The HTTP method does not matter: kubectl opens exec over a WebSocket `GET` or
  a SPDY `POST`, and both reach admission as `CONNECT`.

## Hints

<details><summary>Nudge</summary>

How many ways are there to reach a process inside a running container? The
stage 21 handler already knows how to judge a request whose object is not the
pod.
</details>

<details><summary>Approach</summary>

One rule: `CONNECT`, both subresources. In the handler, send exec and attach
to the same lookup an eviction uses; only the wording of the refusal changes.
</details>

## Further reading

- [Matching requests: rules](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#matching-requests-rules)
- [Get a shell to a running container](https://kubernetes.io/docs/tasks/debug/debug-application/get-shell-running-container/)
