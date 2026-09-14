---
title: Decide what happens when you are down
concepts: [failurepolicy, fail, ignore, availability, blast radius]
---

## Core concept

`failurePolicy` answers one question: when the API server cannot get an answer
out of you, does the request go through?

```yaml
failurePolicy: Fail    # no answer, no write
failurePolicy: Ignore  # no answer, write it anyway
```

There is no third option, and both are bad in a different direction.

**`Fail` means your webhook is now a dependency of every write it matches.**
Your deploy, your crash, your expired certificate, your network — all of them
become cluster-wide failures for the objects in your rules. This is the correct
choice for a policy that must not be bypassed: a security control that fails
open is not a control.

**`Ignore` means your policy is advisory.** Anything written while you are down
is written unchecked, and nothing goes back and re-checks it later. This is the
correct choice when your webhook is a convenience — a defaulter, a labeller —
and being briefly absent is better than being briefly fatal.

"A failure" is wider than a crash: a connection refused, a TLS handshake that
does not verify, a timeout, an HTTP error, a body that is not an
AdmissionReview, a reply with the wrong uid. Every one of those goes to the
failure policy, which is why stage 5 mattered.

The dangerous combination is `Fail` plus a wide rule plus no namespace
exclusions. If your webhook's own pods match its rules, restarting it requires
it to be up — and the only way out is deleting the webhook configuration by
hand.

Two things soften the trade-off. A short `timeoutSeconds` bounds how long each
matching request waits before the policy applies. And running more than one
replica means "you are down" needs more than one failure at once.

## Go APIs

- `admissionregistrationv1.Fail` and `admissionregistrationv1.Ignore`, assigned
  to `ValidatingWebhook.FailurePolicy` (a pointer).
- `NamespaceSelector` with `NotIn` on the namespaces that must keep working.
- Deleting a configuration is the escape hatch:
  `kubectl delete validatingwebhookconfiguration <name>`.

## Hints

<details><summary>Nudge</summary>

Before you switch to `Fail`, make sure the namespace your own program runs in
is excluded. That is the difference between an outage and an outage you cannot
fix.
</details>

<details><summary>Approach</summary>

Prove the policy rather than trusting it: stop the program with the
registration still in place, then create a matching object and see what the
API server does.
</details>

## Further reading

- [Failure policy](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#failure-policy)
- [Admission webhook good practices](https://kubernetes.io/docs/concepts/cluster-administration/admission-webhooks-good-practices/)
