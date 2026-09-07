---
title: Humanise the age
concepts: [output formats, time]
---

## Core concept

The API stores a creation timestamp; a person reading a hundred rows wants
"2d". The rule kubectl follows is one unit — the largest that fits — with no
decimals and no second term: `45s`, `13m`, `5h`, `2d`. It is lossy on purpose.
`51h13m6s` is more precise and less useful, and it makes the column wide enough
to wrap.

Age is derived at print time, not stored. Two runs a minute apart show
different values for the same object, which is the correct behaviour and worth
being deliberate about: you are rendering `now - creationTimestamp`, so the
clock skew between you and the cluster is visible here.

## Go APIs

- `obj.GetCreationTimestamp().Time` — a `metav1.Time` wraps `time.Time`.
- `time.Since(t)` and a `switch` on the thresholds.
- `int(d.Hours()/24)` — integer division truncates, which is what you want:
  47 hours is "1d", not "2d".

## Hints

<details><summary>Nudge</summary>

Four cases, largest unit first, each printing a single integer and a suffix.
</details>

<details><summary>Approach</summary>

```go
switch {
case d < time.Minute: // seconds
case d < time.Hour:   // minutes
case d < 24*time.Hour:// hours
default:              // days
}
```

Real kubectl gets fuzzier past a certain age ("2y64d"); one unit is enough
here, and the tester only cares that a timestamp is not what you printed.
</details>

## Further reading

- [metav1.Time](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Time)
