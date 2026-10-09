---
title: Say why, where people look
concepts: [events, eventsk8sio, describe, failedscheduling]
---

## Core concept

Your scheduler now makes real decisions, and nobody can see them. A pod is on a
node, or it is not, and the reasoning lives in your program's stdout — which
nobody reads. The cluster has a place for this: **Events**.

```
$ kubectl describe pod web
...
Events:
  Type    Reason     Age   From    Message
  Normal  Scheduled  2s    byok8s  bound to byok8s-worker2
```

That is the first thing anyone runs when a pod is not where they expected, so
that is where a scheduler owes an answer — both answers, in fact: where a pod
went, and why one is still waiting.

Events have two APIs. The original is `core/v1`, with `involvedObject`,
`reason` and `message`. The current one is `events.k8s.io/v1`, and it is the
one to write:

```go
&eventsv1.Event{
    ObjectMeta:          metav1.ObjectMeta{Name: …, Namespace: pod.Namespace},
    EventTime:           metav1.NewMicroTime(time.Now()),
    ReportingController: "byok8s",
    ReportingInstance:   "byok8s",
    Action:              "Scheduling",
    Reason:              "Scheduled",
    Type:                corev1.EventTypeNormal,
    Note:                "bound to byok8s-worker2",
    Regarding:           corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", …},
}
```

Four details the API server will hold you to:

- **`eventTime` is required**, and it is a `MicroTime` — six fractional digits.
  `metav1.NewMicroTime(time.Now())` gets it right; a timestamp with three
  digits is rejected before it leaves your client.
- **`action` and `reason` are both required.** `reason` is the short
  machine-ish word people grep for (`Scheduled`, `FailedScheduling`); `action`
  is what was being attempted.
- **`regarding` names the pod** — kind, namespace, name and UID. That is what
  makes the event show up under `kubectl describe pod`, and the UID is what
  keeps it attached to *this* pod rather than the next one with the same name.
- **`type` is `Normal` or `Warning`.** A pod nobody could place is a `Warning`;
  that is what `FailedScheduling` is from the default scheduler.

An event is a side effect, not the decision. If writing one fails, say so on
stderr and carry on: a scheduler that stops placing pods because it could not
write a note is worse than one that places them quietly.

For this stage: record a `Normal` event naming the node when you bind, and a
`Warning` when no node fits.

## Go APIs

- `cs.EventsV1().Events(ns).Create(ctx, event, metav1.CreateOptions{})`.
- `eventsv1 "k8s.io/api/events/v1"`, `metav1.NewMicroTime`,
  `corev1.EventTypeNormal` / `…Warning`, `corev1.ObjectReference`.
- Event names must be unique; the convention is the pod's name plus a suffix,
  e.g. `fmt.Sprintf("%s.%x", pod.Name, time.Now().UnixNano())`.

## Hints

<details><summary>Nudge</summary>

Where does someone look first when a pod is not running — your logs, or
`kubectl describe`?
</details>

<details><summary>Approach</summary>

One small function that takes the pod, a type, a reason and a note, and
creates the event; call it in both branches of your decision. Report a failure
to write one on stderr and keep going.
</details>

## Further reading

- [Event (events.k8s.io/v1) API reference](https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/event-v1/)
- [Debug pods: check pod events](https://kubernetes.io/docs/tasks/debug/debug-application/debug-pods/)
