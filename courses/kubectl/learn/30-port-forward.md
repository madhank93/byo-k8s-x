---
title: Forward a local port
concepts: [spdy, streaming, networking, goroutines]
---

## Core concept

Port-forward is the same upgrade as exec put to a different use: a local
listener whose connections are tunnelled, one stream pair per forwarded port,
through the apiserver to the pod.

```
  localhost:8080 ──► your process ──► apiserver ──► kubelet ──► pod:80
                     (SPDY stream pair per connection)
```

Because the apiserver does the proxying, this reaches a pod with no Service, no
Ingress and no network route from your machine. That is what makes it the
debugging tool of choice, and equally why it is not a way to expose anything:
the tunnel lives and dies with your process and every byte crosses the control
plane.

The Go shape is the lesson. `ForwardPorts()` blocks forever by design — it is a
server — so the *caller* decides when it stops, by closing a channel. That
inversion appears all over client-go, and it comes with two more channels: one
signalling ready (the port is bound; anything you print before this is a lie)
and the ordinary Go pattern of a signal handler that closes the stop channel so
Ctrl-C tears the tunnel down instead of killing the process mid-stream.

## Go APIs

- `spdy.RoundTripperFor(cfg)` then `spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", req.URL())`.
- `portforward.New(dialer, []string{"8080:80"}, stopCh, readyCh, os.Stdout, os.Stderr)`.
- `signal.Notify(sig, os.Interrupt)` and a goroutine that closes `stopCh`.
- The subresource in the URL is `portforward`, on `pods`.

## Hints

<details><summary>Nudge</summary>

Two channels go into the constructor and neither is optional. Ask what each
one is for before writing any of it.
</details>

<details><summary>Approach</summary>

Build the request the way stage 29 did, but dial it yourself with
`spdy.NewDialer` rather than handing it to an executor. Print the "forwarding"
line from a goroutine waiting on `readyCh`; printing it before the port is
bound tells the user to connect to something that is not listening yet.
</details>

<details><summary>Implementation</summary>

```go
req := cs.CoreV1().RESTClient().Post().
    Resource("pods").Namespace(ns).Name(pod).SubResource("portforward")

transport, upgrader, err := spdy.RoundTripperFor(cfg)
dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", req.URL())

stopCh, readyCh := make(chan struct{}), make(chan struct{})
fw, err := portforward.New(dialer, []string{ports}, stopCh, readyCh, os.Stdout, os.Stderr)
return fw.ForwardPorts() // blocks until stopCh closes
```
</details>

## Further reading

- [portforward](https://pkg.go.dev/k8s.io/client-go/tools/portforward)
- [Use port forwarding to access applications](https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-in-a-cluster/)
