---
title: Build your own controller — primer
concepts: [control loops, level triggering, custom resources, operators]
---

Read this before stage 1.

## What a controller actually is

Kubernetes is not a system that does what you tell it. It is a system that
records what you asked for and then a great many small programs argue the world
into agreeing. Each of those programs is a **controller**, and each one runs
the same loop:

> Read the desired state. Look at the actual state. Do something about the
> difference. Repeat.

That is the whole idea. A Deployment does not create pods; the Deployment
controller notices that a Deployment says 3 and a ReplicaSet has 2, and closes
the gap. Nothing in Kubernetes is imperative underneath — it is loops all the
way down, and you are about to write one.

## Level, not edge

The single most important property of that loop is that it is
**level-triggered**. It looks at the current state, not at what changed.

An edge-triggered program — "when a Website is created, create a Deployment" —
is correct exactly until it misses an event. Then it is wrong forever, and the
only fix is a human. A level-triggered program that misses an event is wrong
until the next pass, and every later pass repairs it. Events become an
optimisation: they tell you *when* to look, not *what to do*.

Everything else in this course follows from that:

- Passes must be **idempotent**, because they run again on unchanged objects.
- A pass reads the object itself rather than trusting the event that woke it.
- "The object is gone" is an ordinary outcome, not an error.
- A timer is a legitimate backstop, because looking again is always safe.

## The API server is the only source of truth

Your controller holds no state that matters. Everything it knows is in the
cluster, and everything it decides can be re-derived from the cluster. Restart
it, run it on another machine, run it after a week of downtime — it reads the
world and carries on.

This is why controllers are written against a **cache** (an informer) rather
than against their own bookkeeping, why they write **status** back to the
object instead of into a log, and why the object — not a queue entry, not an
event — is the record of what should exist.

## What you are building

A `Website` custom resource:

```yaml
apiVersion: byok8s.dev/v1alpha1
kind: Website
metadata:
  name: blog
spec:
  image: nginx:1.27
  replicas: 3
  host: blog.example.com
```

…and the controller that makes it real: a Deployment and a Service, owned by
the Website, repaired when they drift, cleaned up when it is deleted, with
status that says whether it is actually serving.

The course goes in three movements:

1. **Stages 1-7 — how a controller sees.** Define the resource, list and watch
   it, cache it in an informer, turn events into keys on a queue, and write a
   pass that reads the world rather than the event.
2. **Stages 8-16 — how a controller acts.** Create children, own them, adopt
   what is already there, repair drift, roll out changes, survive failures,
   come back on a timer, and let a human pause you.
3. **Stages 17-28 — how a controller behaves in production.** Report status and
   conditions, clean up with finalizers, lose races gracefully, apply as a
   field manager, watch what you own, respect other people's objects, emit
   events, expose metrics and health, elect a leader, and shut down cleanly.

## Two things worth knowing before you start

**Everything is untyped here, on purpose.** Websites are read as
`unstructured.Unstructured` through the dynamic client, because that is what
you have before generating Go types for your CRD. It keeps the whole course to
one file and one dependency, and it makes the shape of the API visible instead
of hiding it behind generated code. A real operator generates types; nothing
else about the loop changes.

**The controller installs its own CRD.** That is stage 1, and it means the
program you write is the entire deployment: run it against an empty cluster and
the new kind exists.

## Running it

```sh
byok8s up                              # the kind cluster
byok8s --course controller list        # the stages
byok8s --course controller run 1       # verify stage 1
byok8s --course controller learn 7     # the note for stage 7
byok8s --course controller learn 7 -hints
```

From stage 3 the program does not exit — it runs until it is stopped, which is
what a controller does. The harness starts it, changes the cluster underneath
it, waits for it to converge, and then stops it.
