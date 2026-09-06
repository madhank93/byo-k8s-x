---
title: Stream a container's logs
concepts: [subresources, streaming, kubelet]
---

## What this stage teaches

There is no Log object to Get. `pods/log` is a subresource that returns a
**stream of bytes**, and with `--follow` it never ends — which is why its
client-go signature could not have been `(string, error)`.

`GetLogs` hands you a `*rest.Request` you have to `Stream(ctx)`; what comes
back is an `io.ReadCloser` to copy to stdout and to close. That is the shape of
every open-ended endpoint in the API.

This is also the first request that does not end at the apiserver. The
apiserver proxies it to the **kubelet** on the node running the pod, which
reads the container runtime's log files. So logs need a real node, they fail
differently when the node is unreachable, and they are subject to the runtime's
rotation — `--follow` on a busy container can skip lines that rotated away.

The container name is optional only when the pod has one container. The
apiserver refuses to guess with more, so a program that never passes it works
until the day someone adds a sidecar.

## Go you'll reach for

- `cs.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{Container: c, Follow: f})`.
- `req.Stream(ctx)` → `io.ReadCloser`; `defer stream.Close()`.
- `io.Copy(os.Stdout, stream)` — no line buffering of your own.
- `PodLogOptions` also has `Previous`, `SinceSeconds`, `TailLines`,
  `Timestamps`, all worth a look.

## Hints

<details><summary>Nudge</summary>

`GetLogs` does not perform a request. Look at what it returns and what you have
to call on it.
</details>

<details><summary>Approach</summary>

`io.Copy` straight to stdout. Do not accumulate the stream into a string first
— with `--follow` that call never returns.
</details>

<details><summary>The API</summary>

```go
req := cs.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{
    Container: container, Follow: follow,
})
stream, err := req.Stream(ctx)
defer stream.Close()
_, err = io.Copy(os.Stdout, stream)
```
</details>

## Going deeper

- [PodLogOptions](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#PodLogOptions)
- [Logging architecture](https://kubernetes.io/docs/concepts/cluster-administration/logging/)
