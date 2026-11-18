---
title: Build your own kube-apiserver — primer
concepts: [restapi, resources, discovery, etcd, watch]
---

## The thing everything else talks to

Every other course in this repo is a client. `kubectl` asks; the controller
watches and writes; the webhook is called mid-write; the scheduler reads pods
and writes one field. All four are pointed at the same process, and none of
them contains any Kubernetes logic that is not a conversation with it.

This course is that process.

It is worth saying plainly what the API server is, because its name suggests
something grander: **it is a REST API in front of a key-value store.** Objects
go in, objects come out, and a client can ask to be told when they change.
That is the whole of it. The cluster's behaviour — pods landing on nodes,
deployments rolling out, secrets mounting — is not in here. It is in the
controllers, and every one of them is a loop somewhere else reading this
server's answers.

What makes it interesting is everything the sentence "a REST API in front of a
store" quietly requires:

- **A URL shape that a client can derive rather than be told.** `kubectl get
  configmaps` becomes `GET /api/v1/namespaces/default/configmaps` without
  anyone hardcoding it, because the server publishes what it has and kubectl
  builds the path from that. That is discovery, and it is why `kubectl` works
  against a CRD nobody had written when kubectl was compiled.
- **An ordering every reader can agree on.** `resourceVersion` is one counter
  over every write, which is what makes "tell me what changed since I last
  looked" answerable at all — the difference between a watch and a poll.
- **Errors as objects.** A failure is a `Status`, with a `reason` a program can
  branch on. `IsNotFound(err)` on the client side is reading a field this
  server set.
- **A write path with opinions in it.** Admission runs between the request and
  the store, which is where the webhook course plugs in; validation, defaulting
  and ownership tracking all happen in that gap.

You will build it in that order, and the early stages are deliberately small:
serve, say what you have, store one thing. By the end, the real `kubectl` will
drive this server — `get`, `apply`, `edit`, `--watch` — with no kind cluster
involved anywhere.

## How this course is graded

Unlike every other course here, there is no cluster. The harness starts your
program on an address it chose, and asserts on what comes back over HTTP: the
status code, the headers, and the JSON. Late stages write a kubeconfig pointing
at your server and run the real `kubectl` against it, and the verdict is what
that prints.

Two consequences worth internalising early:

- **The address is not yours to pick.** Read `-addr`. The harness uses a free
  port so that two runs cannot collide.
- **Your program is long-running.** It serves until it is stopped, and it is
  stopped with a signal. Shut down cleanly.

## What you will not build

Roughly in order of how much work they are: etcd itself (you will store objects
in a map, and one stage swaps in a real etcd), the aggregation layer, a scheme
of Go types with conversion between API versions, protobuf serialisation, and
the controller-manager's hundred controllers. None of those change the shape of
what you are building, and all of them would bury it.
