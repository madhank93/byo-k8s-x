---
title: Find the cluster
concepts: [kubeconfig, rest config, clients]
---

## Core concept

Before a client can do anything it has to answer "which server, as whom?", and
the answer is never hardcoded. A kubeconfig is a merge of files, each holding
clusters, users and the contexts that pair them; the *loading rules* say which
files, in what order. Going through them gives you a `rest.Config` — host, CA,
credentials — and going around them by reading `~/.kube/config` yourself gives
you a tool that works on your laptop and nowhere else.

`$KUBECONFIG` is the part people miss. It is colon-separated and merged left to
right, which is how anyone juggling several clusters actually works.

## Go APIs

- `clientcmd.NewDefaultClientConfigLoadingRules()` — the rules themselves.
- `clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides)` —
  "non-interactive" means it will never prompt; "deferred" means the files are
  read when you ask for the config, not now.
- `cfg.Host` — the server URL, once you have the config.

## Hints

<details><summary>Nudge</summary>

You need `k8s.io/client-go/tools/clientcmd`, and you need exactly two calls
from it before you have something with a `.Host` on it.
</details>

<details><summary>Implementation</summary>

```go
rules := clientcmd.NewDefaultClientConfigLoadingRules()
cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
cfg, err := cc.ClientConfig() // *rest.Config
```
</details>

## Further reading

- [Organizing cluster access with kubeconfig](https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/)
- [clientcmd](https://pkg.go.dev/k8s.io/client-go/tools/clientcmd)
