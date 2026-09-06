---
title: Ask the server what it is
concepts: [discovery, rest api, clients]
---

## What this stage teaches

This is the first request that leaves your machine, and it deliberately goes
through the **discovery client** rather than a typed one. `/version` belongs to
no API group and has no Kind, so there is no `Get` for it — nothing typed to
ask. Discovery is the client for the endpoints that describe the server itself:
`/version`, `/api`, `/apis`.

It doubles as a connectivity check. If credentials are wrong or the server is
unreachable, you find out here, on a call that reads nothing and needs no
permissions beyond being authenticated.

## Go you'll reach for

- `discovery.NewDiscoveryClientForConfig(cfg)` — a client built from the config
  you already resolved.
- `dc.ServerVersion()` — returns a `*version.Info`; `GitVersion` is the field
  kubectl prints.

## Hints

<details><summary>Nudge</summary>

The client you want is not `kubernetes.NewForConfig`. Ask yourself what typed
object `/version` would even return.
</details>

<details><summary>The API</summary>

```go
dc, err := discovery.NewDiscoveryClientForConfig(cfg)
v, err := dc.ServerVersion()
fmt.Printf("Server Version: %s\n", v.GitVersion)
```
</details>

## Going deeper

- [discovery](https://pkg.go.dev/k8s.io/client-go/discovery)
- [API concepts](https://kubernetes.io/docs/reference/using-api/api-concepts/)
