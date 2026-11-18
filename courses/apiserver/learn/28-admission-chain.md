---
title: Everyone else gets a say
concepts: [admission-webhooks, admission-review, json-patch, failure-policy, optimistic-concurrency]
---

## Core concept

Authentication says who is asking. Before a write is stored, the real server
asks one more question: does anybody else object? Policy engines, sidecar
injectors and defaulting controllers all answer it, and none of them lives in
the API server. They are **admission webhooks**: HTTPS endpoints the server
calls with each write, configured with two more kinds of object.

```
POST /apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations
POST /apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations
```

Both are cluster-scoped. Each holds a list of `webhooks`, and each webhook
says where it lives and what it wants to hear about:

```yaml
webhooks:
- name: guard.admission.test
  clientConfig:
    url: https://127.0.0.1:9443/guard
    caBundle: <base64 PEM>          # the only CA its certificate is checked against
  rules:
  - operations: [CREATE, UPDATE]    # DELETE too, or "*"
    apiGroups: [""]                 # the core group is the empty string
    apiVersions: [v1]
    resources: [configmaps]         # "configmaps/status" is a different thing
  objectSelector: {matchLabels: {guarded: "yes"}}
  failurePolicy: Fail               # the default; or Ignore
  timeoutSeconds: 10                # the default
```

**The request is an AdmissionReview.** For every matching webhook the server
POSTs `admission.k8s.io/v1` `AdmissionReview` with a `request`: a fresh `uid`,
`kind` and `resource` (`{group, version, kind|resource}`), `name`,
`namespace`, `operation`, `userInfo` (what authentication decided), `dryRun`,
and two objects. `object` is what would be stored; `oldObject` is what is
stored now. A create has no `oldObject`, a delete has no `object`, and both are
`null` rather than missing.

The webhook answers with the same kind, carrying a `response`:

```json
{"uid": "<the request's uid>", "allowed": true,
 "patchType": "JSONPatch", "patch": "<base64 of a JSON patch>"}
```

A response under any other uid is no answer at all.

**Mutating first, one at a time.** Mutating webhooks run in order — by
configuration name, then in the order each lists its webhooks — and each is
sent the object as the one before it left it. Their patches are RFC 6902 JSON
patches, the same ones you applied in the patch stage. Then the server
validates the result itself (a webhook can set `spec.replicas` to `-1`; that
is still invalid), and only then do the **validating** webhooks see it. They
cannot change anything, so they can all be asked at once, and any one of them
can refuse.

**A refusal is the client's answer.** `allowed: false` stores nothing and
answers a `Status` whose message is
`admission webhook "<name>" denied the request: <its message>`, with the code
the webhook gave or 403 when it gave none.

**A webhook that fails** — nothing listening, a certificate its `caBundle`
did not sign, the wrong uid, no answer within `timeoutSeconds` — is decided by
`failurePolicy`. `Fail` refuses the request with a 500 saying
`failed calling webhook "<name>"`; `Ignore` carries on as if it were not there.

**Every write goes through it**: POST, PUT, each patch dialect, apply, and
DELETE. A patch is admitted as the whole object it produces, not as the patch.
The webhook configurations themselves are not admitted — a webhook that
refused them could never be removed. With no configurations, nothing changes.

**And never under the lock.** A webhook call can take seconds. Make it while
holding the store's lock and every reader waits for it. So the write is worked
out and admitted against what is stored, and committed only if the object is
still at the resourceVersion admission saw; if it moved, start again. That is
the optimistic loop the real server runs against etcd.

## Go APIs

- `[]byte` fields decode base64 from JSON: `caBundle` and `patch` need no
  decoding of their own.
- `x509.NewCertPool()`, `pool.AppendCertsFromPEM(caBundle)`, and
  `&http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}`.
- `http.Client{Timeout: d}` bounds the whole call, body and all.
- `sync.WaitGroup` for the validating webhooks.

## Hints

<details><summary>Nudge</summary>

Write admission as one function, `admit(obj, old) (object, error)`, built per
request, and hand it to every write path. Mutating, the server's own
validation, then validating — and the caller stores whatever it returns.
</details>

<details><summary>Approach</summary>

Read the configurations at the start of each request; the store already lists
them by name. A webhook matches when one of its rules names the operation,
group, version and resource (`*` matches any), and its objectSelector matches
the labels of the new or the old object. Build the review, POST it with a
client trusting only the caBundle, check the uid, and apply the patch with
your JSON patch code. For updates, read the stored object, run the change and
admission without the lock, then commit under the lock only if the
resourceVersion is unchanged — retry a few times, then answer 409. For a
delete, admit the stored object and remove it only at that resourceVersion.
</details>

## Further reading

- [Dynamic admission control](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/)
- [Admission controllers](https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/)
- [AdmissionReview v1](https://kubernetes.io/docs/reference/config-api/apiserver-admission.v1/)
- [RFC 6902, JSON Patch](https://www.rfc-editor.org/rfc/rfc6902)
