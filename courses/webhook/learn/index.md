---
title: Build your own admission webhook — primer
concepts: [admission control, validating, mutating, admissionreview, failure policy]
---

## Where admission sits

Every write to a Kubernetes cluster takes the same road. The API server
authenticates you, decides whether you are allowed, and then — before anything
is written down — runs **admission**: a chain of small decisions that may
reject the object or change it.

Most of that chain is compiled into the API server. The last two links are not:
`MutatingAdmissionWebhook` and `ValidatingAdmissionWebhook` call out over HTTPS
to programs that are not part of Kubernetes at all. That is where your program
goes.

The order is fixed and worth memorising, because half the confusing behaviour
in this course follows from it:

> mutating webhooks → object schema validation → validating webhooks → storage

Mutation happens first, so a validating webhook always judges the final object.
Mutating webhooks run in sequence and can undo each other's work; validating
webhooks run in parallel and cannot change anything, so a single rejection is
the answer no matter what the others said.

## The other side of the call

You are used to being the client. Here the API server is the client and your
program is the server, which inverts nearly everything:

- **You cannot be trusted by default.** The API server speaks TLS and checks
  the certificate against a bundle it was given in your registration.
- **You are on the critical path.** Every matching request waits for you, up to
  the timeout you declare. A slow webhook is a slow cluster.
- **Being down is a policy decision.** `failurePolicy: Fail` means your outage
  becomes the cluster's outage; `Ignore` means your rule quietly stops
  applying. Neither is free.
- **You are asked, not told.** The payload is an `AdmissionReview`, and the same
  kind goes back with your verdict inside it. Lose the request's `uid` and the
  answer is discarded.

This is why admission webhooks have a reputation for taking clusters down: a
rule that is too wide, a certificate that expired, or a program that gates the
namespace it runs in, and nothing can be created any more — including the thing
that would fix it.

## What you will build

One program that grows for twenty-two stages: an HTTPS server that registers
itself, judges pods, then rewrites them, and finally survives the operational
traps that make webhooks dangerous.

1. **Stages 1-5 — the protocol.** Serve TLS, answer an `AdmissionReview`,
   register yourself with the cluster, reject what breaks your rule, and echo
   the uid that makes the verdict count.
2. **Stages 6-11 — what you are asked about, and what you change.** Narrow the
   rules to the objects you actually judge, filter by namespace and by label,
   then start mutating: a JSON patch, a default, an injected sidecar.
3. **Stages 12-22 — behaving in production.** Honour a dry run, choose a
   failure policy, answer inside the deadline, survive reinvocation, rotate a
   certificate without dropping a request, leave an audit trail, filter with
   CEL, express the same rule with no webhook at all, never gate yourself,
   tell an eviction from a delete, and judge the delete itself.

## Two things worth knowing before you start

**Your program runs on your machine, not in the cluster.** A real webhook is a
Deployment behind a Service, and the registration points at that Service. Here
the registration points at a `url` instead — the API server dials back out to
the host. Nothing about the protocol changes; it only means you can put a
breakpoint in a webhook the API server is calling.

**You mint your own certificate.** Stage 1 generates a self-signed one and
stage 3 hands its DER bytes to the API server as the `caBundle`. In a real
deployment cert-manager or the cluster signer does this, and the shape is the
same: something has to give the API server a bundle it will accept.

## Running it

```sh
byok8s up                             # the kind cluster
byok8s --course webhook list          # the stages
byok8s --course webhook run 1         # verify stage 1
byok8s --course webhook learn 3       # the note for stage 3
byok8s --course webhook learn 3 -hints
```

The program does not exit — the API server has to be able to call it, so it
runs until something stops it. The harness starts it, applies objects, reads
what the cluster did with them, and then stops it.
