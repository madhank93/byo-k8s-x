---
title: Run again after another webhook has been
concepts: [reinvocationpolicy, ordering, idempotence, mutating chains]
---

## Core concept

Mutating webhooks run in sequence, in an order you do not control. So the
object you were asked about may not be the object that gets stored: a webhook
after you can change what you just decided.

`reinvocationPolicy` is the answer to that:

```yaml
reinvocationPolicy: IfNeeded   # call me again if a later webhook changed the object
reinvocationPolicy: Never      # once is enough (the default)
```

With `IfNeeded`, the API server makes a second pass over the mutating webhooks
when the object changed after they ran. It is what lets a defaulter fix up a
field that a sidecar injector added.

Three properties of that second pass are easy to get wrong.

**It is one extra pass, not a fixed point.** Two webhooks that keep undoing
each other do not converge; the object is stored in whatever state the last
pass left it.

**You may see your own output.** The second call gives you the object *after*
your first patch was applied. A patch built as "append my container" now
appends a second one — the reason idempotence is a hard requirement rather than
good practice.

**Ordering is still not yours.** Reinvocation reduces the damage of not
controlling order; it does not give you control. Anything that truly depends on
running last belongs in a validating webhook, which sees the final object and
cannot change it.

The cost is real: `IfNeeded` can double the number of calls to every mutating
webhook in the chain, and that lands inside the same deadline stage 14 was
about.

## Go APIs

- `admissionregistrationv1.IfNeededReinvocationPolicy` and
  `NeverReinvocationPolicy`, on `MutatingWebhook.ReinvocationPolicy` (a
  pointer).
- A patch built by comparing the object you were given against the object you
  want, rather than from a fixed list of operations.

## What this stage checks

- The mutating webhook declares `reinvocationPolicy: IfNeeded`.
- Asked about a pod that opts in to injection, you patch it. Asked again about
  the result of that patch, you return **no patch at all**.
- A pod admitted through the real chain ends up with exactly one container named
  `sidecar` and its `byok8s.dev/injected` annotation intact.
- Pods with no `owner` label are still refused.

Nothing here registers a second webhook to provoke a real reinvocation. It does
not need to: a handler that is a no-op on its own output is safe under any
number of passes, and one that is not is broken whether or not a second pass
happens today.

## Hints

<details><summary>Nudge</summary>

If your handler is genuinely idempotent, reinvocation is free — the second call
produces an empty patch. Testing that is easier than reasoning about it: feed
your own output back in and check the patch is empty.
</details>

<details><summary>Approach</summary>

Count the calls. Logging the uid and the object's name on entry makes a
reinvocation visible, and shows you that it carries the same uid as the first
call.
</details>

## Further reading

- [Reinvocation policy](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#reinvocation-policy)
- [Ordering of mutating webhooks](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#ordering-of-mutating-admission-webhooks)
