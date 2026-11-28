---
title: Serve kubectl logs
concepts: [kubelet API, daemonEndpoints, /containerLogs, kubectl logs --previous, tailLines]
---

## Core concept

`kubectl logs` never reaches a node by itself. It asks the API server for the
pod's `log` subresource, and the API server, which keeps no logs, turns
around and asks the pod's kubelet. To do that it needs the kubelet's address
and port:

- the address comes from the Node's `status.addresses`, the first of
  `InternalIP, ExternalIP, Hostname` (the `--kubelet-preferred-address-types`
  flag);
- the port comes from `status.daemonEndpoints.kubeletEndpoint.port`, which
  the kubelet publishes in its node status. The default is 10250; this
  course passes `--port`, because the worker's real kubelet already holds
  10250.

The kubelet serves an HTTPS API there. The API server calls
`GET /containerLogs/NAMESPACE/POD/CONTAINER` with the options as query
parameters:

- `previous=true`: the run before the current one, the log file one lower.
  This is how you read why a container in CrashLoopBackOff died.
- `tailLines=N`: only the last N lines.
- `follow`, `timestamps`, `sinceSeconds`, `limitBytes` exist too.

The same server carries `/exec`, `/attach` and `/portForward` (streamed
over SPDY or WebSockets), plus `/metrics` and `/stats`. A real kubelet
authenticates the API server's client certificate and authorizes every
call. This course's server only serves logs.

## Go APIs

- `http.NewServeMux()` with a pattern like
  `"GET /containerLogs/{ns}/{pod}/{container}"` and `r.PathValue("pod")`.
- `crypto/ecdsa`, `crypto/x509` for a self-signed serving certificate,
  then `srv.ServeTLS`.
- `corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port}}`
  in the status you post.

## Hints

<details><summary>Nudge</summary>

You already keep one log file per run, numbered by restart count. "Current"
is the highest number, and "previous" is one below it.
</details>

<details><summary>Approach</summary>

Start the server before the first status post, so the port you publish is
already listening. Look the pod up in your cache for its UID, since the
log directory is named for it. Trim to the last N lines after you read the
file.
</details>

## Further reading

- [Kubelet authentication and authorization](https://kubernetes.io/docs/reference/access-authn-authz/kubelet-authn-authz/)
- [Control plane to node communication](https://kubernetes.io/docs/concepts/architecture/control-plane-node-communication/#control-plane-to-node)
- [kubectl logs](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_logs/)
