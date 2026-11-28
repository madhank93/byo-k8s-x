---
title: Turn counters into rates
concepts: [prometheus text format, counters, rates, metrics/resource]
---

## Core concept

metrics-server does not read the Summary API any more. It reads the kubelet's
lighter `/metrics/resource` endpoint, which speaks the Prometheus text
format:

```
container_cpu_usage_seconds_total{container="app",namespace="ns",pod="burner"} 12.82 1732300000123
container_memory_working_set_bytes{container="app",namespace="ns",pod="burner"} 1.2e+06 1732300000123
```

Name, labels, value, and a timestamp in milliseconds — the moment the kubelet
took the sample.

Memory is a **gauge**: the value is the answer. CPU is a **counter**: total
CPU-seconds since the container started. A counter on its own says nothing
about how busy a container is now. You need two samples, and the rate is

```
(cpu₂ − cpu₁) / (t₂ − t₁)
```

in cores. Use the kubelet's timestamps, not your own clock. The kubelet
samples on its own schedule, every ten seconds or so, so reading twice a
second apart usually returns the **same** sample twice — a zero division, or
a rate from samples you did not take when you think you did. Read until every
container's timestamp has moved.

## Go APIs

- `.AbsPath("/api/v1/nodes", node, "proxy/metrics/resource")`.
- `strings.IndexByte`/`LastIndexByte` for `{` and `}`; `strings.Cut` on `=`
  inside them; `strings.Fields` after them.
- `strconv.ParseFloat` for the value — the kubelet writes `1.2e+06`.

## Hints

<details><summary>Nudge</summary>

Two reads are not two samples. Compare the timestamps.
</details>

<details><summary>Approach</summary>

Parse a scrape into a map keyed `namespace/pod/container` holding the counter,
its timestamp and the working set. Scrape again every couple of seconds until
every key's timestamp has advanced, then divide the differences.
</details>

## Further reading

- [Prometheus text exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/#text-based-format)
- [metrics-server: how it scrapes](https://github.com/kubernetes-sigs/metrics-server/blob/master/FAQ.md)
