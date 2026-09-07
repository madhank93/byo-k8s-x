---
title: Shut down cleanly
concepts: [SIGTERM, graceful shutdown, queue drain, releasing the lease]
---

## Core concept

A controller is stopped constantly — every rollout, every node drain, every
scale-down. The kubelet sends SIGTERM and then waits out the termination grace
period before SIGKILL, and what you do with that window is the difference
between a handover nobody notices and a minute of downtime.

There are three things to do, in order:

1. **Stop taking new work.** Shut the queue down; the worker finishes the key
   it holds and then returns.
2. **Finish what is in flight.** A pass killed midway can leave a Deployment
   created and its Service missing — recoverable, because the next pass repairs
   it, but only after somebody notices.
3. **Release the lease.** `ReleaseOnCancel` clears the holder so a standby
   takes over in seconds instead of waiting out the full lease duration.

And then exit 0. A non-zero exit on an ordinary shutdown is what turns a clean
rollout into a `CrashLoopBackOff` and a page.

## Go APIs

- `signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)` — the context that
  ends everything.
- `queue.ShutDown()` and a `sync.WaitGroup` for the workers.
- `leaderelection.LeaderElectionConfig{ReleaseOnCancel: true}`.
- `srv.Shutdown(ctx)` for the metrics and probe server.

## Hints

<details><summary>Nudge</summary>

Bound the wait. A worker stuck on an API call that never returns must not stop
the process from exiting before SIGKILL arrives.
</details>

<details><summary>Approach</summary>

Print something. "shutting down" in the logs is how an operator tells a
deliberate stop from a crash, and it costs one line.
</details>

<details><summary>Implementation</summary>

```go
<-ctx.Done()
fmt.Println("shutting down")
queue.ShutDown()
workers.Wait()
```
</details>

## Further reading

- [Pod termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination)
- [Graceful shutdown in Go services](https://pkg.go.dev/net/http#Server.Shutdown)
