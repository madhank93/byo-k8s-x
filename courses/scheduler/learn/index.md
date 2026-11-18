---
title: Build your own scheduler — primer
concepts: [scheduling, binding, schedulerName, filter, score]
---

## What a scheduler actually does

A pod you create has no node. Something has to choose one and write it down,
and that something is the scheduler. It is not special: it is a client of the
API server like any other. It watches for pods that have no node yet, decides
where each should go, and records the decision by creating a **Binding** — a
write to the pod's `binding` subresource that sets `spec.nodeName`.

It never starts a container. The kubelet on the chosen node sees a pod with its
name on it and runs it. That split is the whole design: the scheduler decides,
the kubelet does, and the only thing between them is one field on the pod.

## Your scheduler and the default one

Every pod names its scheduler in `spec.schedulerName`. Leave it empty and it
means `default-scheduler`, the one every cluster runs. Name `byok8s` instead and
the default scheduler ignores the pod completely: it stays `Pending` forever
unless something else places it.

That is how this course is graded. The tester creates pods that name `byok8s`,
so your program is the only thing that can place them, and the verdict is which
node each one lands on — or that it was, correctly, left waiting.

## The cluster

A scheduler needs nodes to choose between, so this course asks for three
workers beside the control plane (`workers: 3` in its `course.yml`).
`byok8s up` builds the cluster to that size; if yours was created earlier with
a single node, `byok8s doctor` says so, and `byok8s down && byok8s up`
recreates it. The control plane is tainted `NoSchedule`, so the workers are the
only nodes a pod without a matching toleration should ever land on.

## The shape of the course

One program, grown stage by stage: find the pods waiting for you, bind them,
then learn to say no — resources, ports, node selectors and affinity, taints,
cordoned nodes. Then the parts that make it a real scheduler rather than a
loop: events that explain a decision, a cache instead of asking the API server
every time, scores that choose among nodes that all fit, spreading, pod
affinity, volumes, and finally priority and preemption.

## Two things worth knowing before you start

A binding is a create, not an update. You do not edit the pod to set its node;
you post a `Binding` naming the pod and the node, and the API server sets the
field for you. It does so exactly once: a pod that already has a node cannot be
bound again, which is what stops two schedulers from both placing it.

Deciding and binding are separate moments, and the cluster does not wait for
you in between. A node that fit when you looked may be full by the time you
bind. Most of what makes a production scheduler complicated follows from that
gap.
