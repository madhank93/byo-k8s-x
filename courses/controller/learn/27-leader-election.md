---
title: Elect a leader
concepts: [leader election, leases, high availability, fencing]
---

## Core concept

Run two copies of this controller and they will both reconcile every Website:
two writers, conflicting updates, children created twice under different names
if you are unlucky. Controllers are almost always run with replicas for
availability, so the standard answer is **leader election**: several instances
run, exactly one does the work, and the rest wait to take over.

The lock is an ordinary object — a `Lease` in `coordination.k8s.io`. The holder
writes its identity and renews the lease on a timer; the others watch, and when
the renewal stops for longer than the lease duration, one of them takes it. No
new infrastructure, no consensus protocol of your own: the API server's
consistency is the consensus.

Three durations, and they must be ordered: **lease > renew > retry**. The
leader must have several chances to renew before anyone can claim the lease is
expired, or a slow API call becomes a failover.

Understand the guarantee honestly. This is a lease, not a fence: a leader that
is paused by a long GC pause can believe it still holds the lease while another
has taken it. Everything you write must be idempotent anyway — which, if you
have followed the course, it already is.

## Go APIs

- `resourcelock.New(resourcelock.LeasesResourceLock, ns, name, coreClient, coordinationClient, resourcelock.ResourceLockConfig{Identity: id})`.
- `leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{Lock, LeaseDuration, RenewDeadline, RetryPeriod, ReleaseOnCancel, Callbacks})`.
- `OnStartedLeading(ctx)` — start the loop here, and only here.
- `OnStoppedLeading()` — stop immediately; you are no longer the leader.

## Hints

<details><summary>Nudge</summary>

The identity has to be unique per process. A hostname alone is not, if two
copies run on one machine — add the PID.
</details>

<details><summary>Approach</summary>

Everything that acts on the cluster moves inside `OnStartedLeading`. Anything
that does not act — the health endpoint especially — stays outside, so a
standby still answers probes.
</details>

<details><summary>Implementation</summary>

```go
leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
    Lock:            lock,
    LeaseDuration:   15 * time.Second,
    RenewDeadline:   10 * time.Second,
    RetryPeriod:     2 * time.Second,
    ReleaseOnCancel: true,
    Callbacks: leaderelection.LeaderCallbacks{
        OnStartedLeading: func(ctx context.Context) { run(ctx) },
        OnStoppedLeading: func() { fmt.Println("lost leadership") },
    },
})
```
</details>

## Further reading

- [client-go leaderelection](https://pkg.go.dev/k8s.io/client-go/tools/leaderelection)
- [Lease API](https://kubernetes.io/docs/concepts/architecture/leases/)
