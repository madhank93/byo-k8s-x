---
title: Who is asking
concepts: [authentication, bearer-tokens, user-info, 401-vs-403, selfsubjectreview]
---

## Core concept

Until now this server has answered anybody who could reach the port. A real
one asks two questions of every request, in order, and they are worth keeping
apart because they fail differently:

- **Authentication: who is this?** The answer is a name, a uid and a list of
  groups. Nothing more.
- **Authorization: may they do this?** That is the next stages, and it is
  asked about the name the first question produced.

**401 is "I don't know who you are"; 403 is "I know, and no."** A client that
gets 401 should fix its credentials. One that gets 403 has working credentials
and should ask someone for a role. Mixing them up sends people
debugging the wrong thing.

**A user is not an object.** There is no `users` resource and nothing to
`kubectl get`. The server learns a user from the request, uses it, and forgets
it. Groups are where policy attaches, and two are added by the server rather
than by any authenticator:

```
system:authenticated     everyone some authenticator recognised
system:unauthenticated   everyone else — always with username system:anonymous
```

So a rule can say "any logged-in user may list namespaces" without naming
anyone.

**The simplest authenticator is a file.** The real server's
`--token-auth-file` is a CSV, one token per line:

```
alice-token-0001,alice,1001,"developers,oncall"
bob-token-0002,bob,1002
```

token, user, uid, and an optional fourth column of groups, which is one
quoted field with commas inside it. That is exactly what `encoding/csv` is
for; `strings.Split` on commas breaks it. A file that does not parse stops the
program at startup: a server that skips bad lines starts up missing users
and nobody finds out until they are locked out.

A request presents its token as `Authorization: Bearer <token>`. A known token
is a user and the request goes on as before. An unknown token, a header in
some other scheme, or no header at all is 401 `Unauthorized`, answered before
any handler runs — including the watch handler, which never starts streaming.
The one exception is `/healthz`, `/livez` and `/readyz`: what probes those
has no credentials to present. Without the flag, nothing changes — every
earlier stage still passes, and every request is anonymous.

**Compare tokens the boring way.** A loop that compares bytes and returns at
the first mismatch takes longer for a guess that shares a longer prefix with a
real token, and that difference is measurable over a network. Look the token
up in a map, or compare with `crypto/subtle`.

**Why real clusters do not use this.** The file is read once, so adding or
revoking anyone means restarting the server; the tokens never expire; and they
sit in plain text on the control-plane disk. Real clusters use client
certificates (the CN is the user, the O fields are the groups), OIDC tokens
from an identity provider, and service-account tokens the server signs itself
and can bind to a pod's lifetime. Every one of them produces the same three
things this file does — and the rest of the server only ever sees those.

**Asking the server who you are** is `kubectl auth whoami`, which POSTs a
`SelfSubjectReview` to `/apis/authentication.k8s.io/v1/selfsubjectreviews`
and gets 201 with the answer filled in:

```json
{"apiVersion":"authentication.k8s.io/v1","kind":"SelfSubjectReview",
 "metadata":{"creationTimestamp":null},
 "status":{"userInfo":{"username":"alice","uid":"1001",
   "groups":["developers","oncall","system:authenticated"]}}}
```

Nothing is stored. Discovery lists the group under `/apis` and the resource
under `/apis/authentication.k8s.io/v1`, with the single verb `create`.

## Go APIs

- `csv.NewReader(f)` with `FieldsPerRecord = -1`, then check `len(record)` is
  3 or 4 yourself. It skips blank lines on its own.
- `strings.CutPrefix(h, "Bearer ")` for the header.
- `subtle.ConstantTimeCompare` from `crypto/subtle`, if you loop instead of
  looking up.
- `context.WithValue` to hand the user from the middleware to the handler
  that answers the review.

## Hints

<details><summary>Nudge</summary>

One middleware wrapped around the whole mux: let the health paths through,
otherwise find the user or write the 401 and return. No handler changes
except the new review endpoint.
</details>

<details><summary>Approach</summary>

Parse the file into `map[string]userInfo` at startup and `log.Fatal` on any
error. With no file, the middleware puts `system:anonymous` /
`system:unauthenticated` on every request's context and calls through. With
one, it cuts the `Bearer ` prefix, looks the token up, appends
`system:authenticated` to a copy of the groups, and stores the result on the
context — or answers 401 `Unauthorized`. The review handler reads that value
and writes it into `status.userInfo` with 201. Add the group to `/apis` and
serve its resource list.
</details>

## Further reading

- [Authenticating](https://kubernetes.io/docs/reference/access-authn-authz/authentication/)
- [Static token file](https://kubernetes.io/docs/reference/access-authn-authz/authentication/#static-token-file)
- [kubectl auth whoami](https://kubernetes.io/docs/reference/access-authn-authz/authentication/#self-subject-review)
- [Controlling access to the Kubernetes API](https://kubernetes.io/docs/concepts/security/controlling-access/)
