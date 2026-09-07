---
title: Expose the loop's metrics
concepts: [metrics, prometheus exposition, queue depth, observability]
---

## Core concept

A controller that is quietly failing looks exactly like a controller with
nothing to do. The three numbers that tell them apart are how many passes have
run, how many failed, and how deep the queue is — and none of them can be
inferred from outside the process.

The exposition format is plain text over HTTP, which is worth writing by hand
once: a `# HELP` line, a `# TYPE` line, then `name{labels} value`. There is no
magic in a metrics endpoint, and knowing that makes the libraries less
mysterious.

What to measure follows from what breaks:

- **A counter of passes** — flat when it should not be means the queue is
  stuck or the informer is disconnected.
- **A counter of failures** — climbing steadily means retries are looping.
- **Queue depth** — growing means the worker cannot keep up, which no amount of
  retrying fixes.

Counters only go up, and that is the point: rate is computed by the scraper, so
a restart is visible rather than hidden by a reset average.

## Go APIs

- `http.NewServeMux()` and `http.Server{Addr, Handler}` on its own goroutine.
- `srv.Shutdown(ctx)` when the program stops.
- `queue.Len()` for depth.
- `sync/atomic` counters, or a mutex — handlers run on the HTTP server's
  goroutines, not yours.

## Hints

<details><summary>Nudge</summary>

Make the address a flag with a default. A hardcoded port is the thing that
stops two of these running on one machine.
</details>

<details><summary>Implementation</summary>

```go
mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
    fmt.Fprintf(w, "# HELP byok8s_reconcile_total Passes over a Website key.\n")
    fmt.Fprintf(w, "# TYPE byok8s_reconcile_total counter\n")
    fmt.Fprintf(w, "byok8s_reconcile_total %d\n", passes.Load())
})
```
</details>

## Further reading

- [Prometheus exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/)
- [Kubernetes instrumentation conventions](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/instrumentation.md)
