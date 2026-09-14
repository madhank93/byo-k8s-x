---
title: The same rule, with no webhook at all
concepts: [validatingadmissionpolicy, cel, in-process admission, migration]
---

## Core concept

Everything you have built exists because the API server had no way to evaluate
a rule itself. Since 1.30 it does: **ValidatingAdmissionPolicy** runs CEL
in-process, with no server to write, no certificate to rotate, and no failure
mode where your program being down becomes the cluster's problem.

It is three objects:

```yaml
kind: ValidatingAdmissionPolicy      # the rule, in CEL
kind: ValidatingAdmissionPolicyBinding   # where it applies, and how hard
# plus an optional paramKind: a config object the expression can read
```

The rule from stage 4, with no webhook:

```yaml
validations:
  - expression: "has(object.metadata.labels) && 'owner' in object.metadata.labels"
    message: "every pod must carry an owner label"
```

The split between policy and binding is the part worth internalising. The
policy says *what is true*; the binding says *where it is enforced and what
happens* — `Deny`, `Warn`, or `Audit`. So the same policy can be audited in one
namespace and enforced in another, and rolling out a rule stops being a
deployment.

What it buys you: no network hop, no timeout, no TLS, no availability
dependency. A policy cannot be down.

What it cannot do — the reasons webhooks still exist:

- **It cannot mutate.** (MutatingAdmissionPolicy is the newer, separate
  answer to that.)
- **It cannot look anything up.** No API calls, no external service, no state.
  A rule that needs to know about *other* objects needs a webhook — with the
  narrow exception of a `paramKind` object the binding names.
- **It cannot run arbitrary code.** CEL is deliberately bounded, and a
  sufficiently complicated rule stops fitting in it.

The honest summary is that most validating webhooks in the wild should be
policies, and the ones that should not are the ones that talk to something
else. Knowing which you have is the point of writing both.

## Go APIs

- `admissionregistrationv1.ValidatingAdmissionPolicy` and
  `ValidatingAdmissionPolicyBinding`.
- `Validation{Expression: …, Message: …}` and `MatchResources` on the binding.
- `ValidationAction`: `Deny`, `Warn`, `Audit` — a list, so warn and audit
  together is legal.
- `variables:` in the policy, to name a subexpression used more than once.

## Hints

<details><summary>Nudge</summary>

Bind it with `Warn` first and apply the objects your webhook already refuses.
The two should disagree about nothing; where they do, the CEL is wrong.
</details>

<details><summary>Approach</summary>

Install the policy and the binding the same way the program installs its own
registration — at startup, from the code — so the comparison is running side
by side rather than in two different setups.
</details>

## Further reading

- [Validating Admission Policy](https://kubernetes.io/docs/reference/access-authn-authz/validating-admission-policy/)
- [CEL expression language](https://kubernetes.io/docs/reference/using-api/cel/)
