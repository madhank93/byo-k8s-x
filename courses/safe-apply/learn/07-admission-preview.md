---
title: Hear admission's verdict first
concepts: [admission, dry-run, validatingadmissionpolicy, limitrange]
---

## Core concept

Admission is the part of a write you cannot predict from the client.
Mutating admission adds things — a LimitRange fills in resource limits, a
webhook injects a sidecar. Validating admission says no — a
ValidatingAdmissionPolicy, Pod Security, a quota. Both run on a dry-run, and
that is the second reason the diff asks the server: it previews admission too.

```
  dry-run apply ─► mutating admission ─► validation ─► validating admission ─► (not stored)
                     LimitRange adds        schema       policy may refuse
                     cpu: 250m
```

A refusal is an answer, not a crash. `kubectl diff` stops at the first one;
a reviewer wants all of them, and the rest of the diff alongside. So when the
server refuses one object, say which object and why, and carry on with the
others.

Tell a refusal apart from a failure to ask: a refusal is a Kubernetes
`Status` with a reason and a message; a dropped connection is not.

For this stage, when the dry-run for an object is refused, print

```
Kind/name: rejected: <the server's message>
```

and continue with the next object. If anything was refused, exit **2** — the
diff is incomplete — after printing everything else.

## Go APIs

- `apierrors.APIStatus` is the interface every server answer implements;
  `errors.As(err, &status)` tells a refusal from a transport error.
- The error's `Error()` carries the policy's message.

## Hints

<details><summary>Nudge</summary>

Where in your loop does a dry-run error currently go, and what happens to the
objects after it?
</details>

<details><summary>Approach</summary>

Record the refusal, print it, and give that object no diff — neither side. At
the end, a refusal outranks a change in the exit code.
</details>

## Further reading

- [Admission control](https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/)
- [Validating Admission Policy](https://kubernetes.io/docs/reference/access-authn-authz/validating-admission-policy/)
- [LimitRange](https://kubernetes.io/docs/concepts/policy/limit-range/)
