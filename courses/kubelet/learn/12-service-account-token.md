---
title: Mount the pod's service account token
concepts: [projected volume, TokenRequest, bound service account token, audience, kube-api-access]
---

## Core concept

Every pod runs as a ServiceAccount, `default` unless it names another. Its
credentials reach the containers through a volume. You do not see that
volume in the manifest you write: the ServiceAccount **admission plugin**
adds it to every pod as it is created, as a **projected** volume called
`kube-api-access-…`, mounted at `/var/run/secrets/kubernetes.io/serviceaccount`.

A projected volume merges several sources into one directory:

- **serviceAccountToken**: a token the kubelet requests from the API server
  with the **TokenRequest** API (`serviceaccounts/token`). The kubelet sets
  `boundObjectRef` to the pod, so the token is **bound** to it. The API
  server records the pod in the token and rejects the token once that pod is
  gone. `audience` and `expirationSeconds` set who the token is for and how
  long it lasts.
- **configMap**: `kube-root-ca.crt`, which every namespace has, gives
  `ca.crt`.
- **downwardAPI**: `metadata.namespace` gives `namespace`.

A token expires, so the kubelet requests a new one once 80% of its lifetime
has passed and rewrites the file. A client that rereads the file gets a
fresh token and never notices.

Before bound tokens, a pod's token was a Secret that never expired and kept
working after the pod was gone. Asking for a token per pod is what fixed
that.

## Go APIs

- `cs.CoreV1().ServiceAccounts(ns).CreateToken(ctx, name,
  &authenticationv1.TokenRequest{Spec: {Audiences, ExpirationSeconds,
  BoundObjectRef: {Kind: "Pod", APIVersion: "v1", Name, UID}}}, opts)`.
- `status.token` and `status.expirationTimestamp` on the result.

## Hints

<details><summary>Nudge</summary>

Cache tokens per pod. If you request a new token on every sync, each sync
writes a new file, and the API server signs a token every second for every
pod.
</details>

<details><summary>Approach</summary>

Turn each projection into a set of files (a token, ConfigMap or Secret keys,
downward API fields) and merge them into the one directory. Put that
directory on tmpfs, as you did for Secrets. A plain `downwardAPI` volume is a
projected volume with one source.
</details>

## Further reading

- [Service account tokens](https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/#bound-service-account-token-volume)
- [Projected volumes](https://kubernetes.io/docs/concepts/storage/projected-volumes/)
- [TokenRequest API](https://kubernetes.io/docs/reference/kubernetes-api/authentication-resources/token-request-v1/)
