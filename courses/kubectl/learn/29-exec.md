---
title: Run a command inside
concepts: [spdy, streaming, subresources, rbac]
---

## What this stage teaches

Exec needs several **independent byte streams at once** — stdin, stdout, stderr
and a terminal-resize channel — multiplexed over a single connection. Ordinary
HTTP/1.1 gives you one request and one response body, so the connection is
*upgraded*: SPDY (and on newer clusters, WebSocket) provides the multiplexing.

That is why this is the first call you cannot make with a typed client method.
You build the request URL by hand off the REST client, attach the exec options
as query parameters through the parameter codec, and hand the URL to an
executor that performs the upgrade.

```
  POST /api/v1/namespaces/{ns}/pods/{pod}/exec?command=sh&stdout=true&stderr=true
       Connection: Upgrade
       Upgrade: SPDY/3.1
             │
             ▼   apiserver proxies to the kubelet, kubelet to the runtime
       ┌── stream 0: stdin  ──┐
       ├── stream 1: stdout ──┤   one connection, four streams
       ├── stream 2: stderr  ─┤
       └── stream 3: resize ──┘
```

`pods/exec` being its own address is what lets RBAC grant "may exec" without
granting "may edit pods" — and, read the other way, why granting exec is close
to granting root inside that container.

One CLI detail belongs here too: the command for the container must be kept
away from *your* flag parser. `--` is the convention, and everything after it
is passed through untouched.

## Go you'll reach for

- `cs.CoreV1().RESTClient().Post().Resource("pods").Namespace(ns).Name(pod).SubResource("exec")`.
- `.VersionedParams(&corev1.PodExecOptions{...}, scheme.ParameterCodec)` —
  turns the options struct into query parameters.
- `remotecommand.NewSPDYExecutor(cfg, "POST", req.URL())`.
- `exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: os.Stdout, Stderr: os.Stderr})`.

## Hints

<details><summary>Nudge</summary>

There is no `Exec()` method on the pods client. Work out the URL you need
first, then find what builds it.
</details>

<details><summary>Approach</summary>

Build the request off `RESTClient()`, set `Stdout` and `Stderr` true in
`PodExecOptions` (leave `Stdin` false — no TTY needed here), then let the SPDY
executor run it. Scan `os.Args` yourself for `--`; the `flag` package will not
hand it to you.
</details>

<details><summary>The API</summary>

```go
req := cs.CoreV1().RESTClient().Post().
    Resource("pods").Namespace(ns).Name(pod).SubResource("exec").
    VersionedParams(&corev1.PodExecOptions{
        Container: container, Command: command, Stdout: true, Stderr: true,
    }, scheme.ParameterCodec)

exec, err := remotecommand.NewSPDYExecutor(cfg, "POST", req.URL())
err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
    Stdout: os.Stdout, Stderr: os.Stderr,
})
```
</details>

## Going deeper

- [remotecommand](https://pkg.go.dev/k8s.io/client-go/tools/remotecommand)
- [Get a shell to a running container](https://kubernetes.io/docs/tasks/debug/debug-application/get-shell-running-container/)
