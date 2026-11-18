---
title: Who did what, written down
concepts: [audit-log, audit-policy, audit-stages, response-writer-wrapping, concurrent-writes]
---

## Core concept

The server now knows who is asking. The audit log is where it writes that
down: one record per request, saying who asked for what and what they were
told. It is what is left after an incident, so it is graded the way it is
read then.

**Two flags, the real server's:**

```
-audit-log-path       a file to append events to; empty turns auditing off
-audit-policy-file    an audit.k8s.io/v1 Policy saying what to record
```

With a log and no policy, every request is recorded at `Metadata`. The policy
here is JSON rather than YAML. Go's standard library has no YAML parser, and
JSON is valid YAML, so the real server reads the same file.

**One event per line.** Each event is an `audit.k8s.io/v1` `Event`, written as
one JSON object on one line:

```
auditID                   the ID the client got back in the Audit-Id header
stage                     ResponseComplete; ResponseStarted too, for a watch
level                     the level the policy picked
verb                      the API's verb: get, list, watch, create, update, patch, delete
user                      whoever authentication decided; empty for a 401
objectRef                 resource, namespace, name, subresource, apiVersion, read off the path
requestURI, sourceIPs, userAgent, responseStatus.code
requestReceivedTimestamp, stageTimestamp
requestObject             at Request and above: the body as the client sent it
responseObject            at RequestResponse: the body the server answered with
```

Every response carries `Audit-Id`, refusals included, whatever the policy
says. Someone holding a failed response uses that ID to find its record.

**The policy is rules, tried in order.** The first rule whose every field
matches sets the level. A request that no rule matches is not logged.

```
level            None | Metadata | Request | RequestResponse
users            usernames
userGroups       any one of these groups
verbs            API verbs
resources        [{group, resources}]: "configmaps" is the object and none
                 of its subresources, "replicationcontrollers/scale" is one
                 subresource, "*" is any
namespaces       namespaces
nonResourceURLs  paths outside /api and /apis; a trailing * is a prefix
omitStages       stages not to write, on the policy or on one rule
```

Order matters. `{level: None, users: [carol]}` above a rule that records
everything keeps carol out of the log. Below it, the None rule never gets a
chance.

**Audit sits outside authentication.** A request that authentication refuses
still has to be in the log, so the auditing wraps the authenticator rather
than the other way round. It cannot know the user until authentication has
run, though. The usual answer is a record in the request's context that the
authenticator fills in on the way through.

**A watch is recorded twice.** It can stay open for hours, and a log that
only records it when it ends shows nothing while it is running. So a watch
gets a `ResponseStarted` event when its response begins and a
`ResponseComplete` when the client leaves, both under the same `auditID`. Its
response body is a stream with no end, and it is never recorded.

**Concurrent requests write whole lines.** Two requests finishing at once must
not splice their events together. Write each event as one write of one whole
line while holding a lock. A log with a torn line loses both events, and
probably every tool that reads it.

## Go APIs

- `os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)`.
- A `http.ResponseWriter` wrapper that remembers the code in `WriteHeader`,
  copies the body in `Write`, and implements `http.Flusher` by passing
  `Flush` through. Without that, a watch behind it never streams.
- `context.WithValue` to hand a `*record` down the chain, so the
  authenticator can fill in the user for the auditor to read afterwards.
- `json.RawMessage` to embed a body that is already JSON as an object
  rather than a string.
- `net.SplitHostPort(r.RemoteAddr)` for the source IP.

## Hints

<details><summary>Nudge</summary>

The event is written after the handler returns, by which point the request
body has been read and the response has gone out. Keep copies of both as
they pass: read the body up front and put a fresh reader back on the
request, and copy the response in your writer wrapper.
</details>

<details><summary>Approach</summary>

At startup, read the policy and open the log. Wrap the whole handler,
authentication included. In the wrapper, pick an audit ID and set the header
first. Then work out the verb and objectRef from the method and path: a GET
is `get` for a name, `list` without one, and `watch` with `?watch=true`. Put
an empty record in the context and serve. The authenticator stores the user
in that record. When the handler returns, find the first matching rule, build
the event, `json.Marshal` it, and write it with a newline under a mutex. For
a watch, write `ResponseStarted` from your `WriteHeader` the first time it is
called.
</details>

## Further reading

- [Auditing](https://kubernetes.io/docs/tasks/debug/debug-cluster/audit/)
- [Audit Event and Policy (audit.k8s.io/v1)](https://kubernetes.io/docs/reference/config-api/apiserver-audit.v1/)
- [kube-apiserver flags](https://kubernetes.io/docs/reference/command-line-tools-reference/kube-apiserver/)
- [net/http Flusher](https://pkg.go.dev/net/http#Flusher)
