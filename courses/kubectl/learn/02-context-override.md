---
title: Choose a context
concepts: [kubeconfig, contexts, precedence]
---

## Core concept

A context is a named triple: a cluster, a user, and optionally a namespace.
The kubeconfig marks one as current, and every kubectl flag that changes where
a command goes is an *override* on top of that — the same mechanism, whether it
comes from a flag, an environment variable or the file.

The failure mode worth designing for is a context that does not exist. Falling
back to the current one would be friendly and wrong: a typo in `--context prod`
must not quietly run against staging. Silently talking to the wrong cluster is
the worst thing this program could do, so an unknown context is an error.

## Go APIs

- `clientcmd.ConfigOverrides{CurrentContext: name}` — an empty string means
  "whatever the file says", so one code path covers both cases.

## Hints

<details><summary>Nudge</summary>

You already build a `ConfigOverrides` in stage 1 and pass it empty. This stage
is a field on it, not a new mechanism.
</details>

<details><summary>Implementation</summary>

```go
overrides := &clientcmd.ConfigOverrides{CurrentContext: *contextFlag}
```

Then let `ClientConfig()` fail on its own when the name is unknown — you do not
need to validate it yourself.
</details>

## Further reading

- [ConfigOverrides](https://pkg.go.dev/k8s.io/client-go/tools/clientcmd#ConfigOverrides)
