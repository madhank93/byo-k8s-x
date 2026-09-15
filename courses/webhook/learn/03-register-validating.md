---
title: Register yourself with the cluster
concepts: [validatingwebhookconfiguration, cabundle, rules, failurepolicy, sideeffects]
---

## Core concept

A webhook that nobody has been told about is a web server. What makes it part
of admission is a **ValidatingWebhookConfiguration**: a cluster-scoped object
that says which requests to divert, where to send them, and who to trust.

Three fields carry that meaning.

`clientConfig` is the address. In a cluster it is usually a `service`; here it
is a `url`, because the program runs on your machine rather than in a pod. Next
to it sits `caBundle` — the certificate the API server will check the webhook's
own against. The bundle is not a formality: without it the call fails at the
handshake, and the failure reads as a timeout.

`rules` decide what is diverted, by group, version, resource and operation.
Everything a rule matches now goes through your program before it is stored,
which is why a rule that is wider than the thing you are policing is the usual
way a webhook takes a cluster down.

`failurePolicy` decides what the API server does when your program does not
answer. `Ignore` admits the request; `Fail` rejects it. Start with `Ignore` —
a `Fail` policy on a webhook you are still writing means the next crash locks
you out of your own cluster.

Two more fields are not optional, though they look it. `sideEffects` tells the
API server whether calling you changes anything outside the request, and
`admissionReviewVersions` says which versions of the payload you understand.
The API server refuses a registration that leaves either unstated.

Registering from inside the program is what makes it self-contained: start it
and it is wired in, stop it and it is not. Deleting the configuration on the
way out matters as much as creating it — a registration left behind points the
API server at a port nothing is listening on.

## Go APIs

- `admissionregistrationv1.ValidatingWebhookConfiguration`, with
  `WebhookClientConfig{URL: &url, CABundle: caPEM}`.
- `pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})` — the
  caBundle is PEM even though the certificate was created as DER.
- `RuleWithOperations{Operations: []OperationType{Create}, Rule: Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods"}}}`.
- `clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations()` —
  `Create`, `Update`, `Delete`.
- `clientcmd.NewNonInteractiveDeferredLoadingClientConfig` to load the same
  kubeconfig kubectl would.

## Hints

<details><summary>Nudge</summary>

The caBundle is PEM, not the DER `selfSigned` returns. Handing over raw DER
registers cleanly and then fails inside the API server, once per admitted
request, with "unable to parse bytes as PEM block".
</details>

<details><summary>Approach</summary>

Register just before you start serving, and `defer` the delete. `Create`
returns `AlreadyExists` after a restart: read the existing object, copy its
`resourceVersion` onto yours, and `Update`.
</details>

## Further reading

- [Dynamic admission control](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/)
- [Webhook configuration](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#webhook-configuration)
- [Avoiding operating on the kube-system namespace](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#avoiding-operating-on-the-kube-system-namespace)
