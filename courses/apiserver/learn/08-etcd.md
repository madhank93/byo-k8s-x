---
title: The part that survives a restart
concepts: [persistence, registry-keys, atomic-write, monotonic-version]
---

## Core concept

The API server keeps nothing. Everything it answers for lives in etcd, and the
process in front of it can be killed at any moment and replaced without anyone
noticing. That property is what makes a control plane rolling-upgradeable, and
it is the contract this stage asks for:

**nothing the server answered for may go missing because it stopped.**

A 201 for an object that is not there after a restart was a lie when it was
sent.

```
$ ./app -addr 127.0.0.1:8080 -data /var/lib/byok8s   # create alpha, stop
$ ./app -addr 127.0.0.1:9090 -data /var/lib/byok8s   # alpha is still there
```

The address is not what makes it the same server. The data is.

**Keys, not rows.** etcd is one flat key space, and Kubernetes lays objects out
in it by path:

```
/registry/configmaps/default/settings
/registry/configmaps/kube-system/settings
/registry/namespaces/default
```

There are no indexes and no joins — the *only* fast question is "everything
under this prefix", which is exactly what a list is. Every scoping rule in
Kubernetes falls out of that one shape: a namespaced list is a prefix, a
cluster-wide list is a shorter prefix, and the resource name is a segment.
Adopting the layout now is what makes the next few stages small.

**Write to the disk before you believe it yourself.** The real server writes to
etcd and its own caches follow from the watch that comes back — it never trusts
its memory ahead of the store. Doing the same here means the order is: build the
new state, persist it, and only then swap it in. A failed write is then a 500 and
an unchanged store, rather than an object the server thinks it has.

**A torn file is worse than no file.** Write beside the real one and `rename`
onto it: on every filesystem worth using, that swap is atomic. A process killed
mid-write then loses the last write instead of the whole store. The real
equivalent is etcd's WAL and its fsync; the property being bought is the same.

**The counter is part of the data.** This is the one people leave out, and it is
the worst one to leave out, because a single run never shows it. `resourceVersion`
must be written down with the objects. A store that comes back at 1 hands out
numbers clients are already holding — every watcher with a bookmark is then
waiting for changes it has already seen, and no error anywhere says so. The
numbers only ever go up, across restarts included.

## Go APIs

- `flag.String("data", "", …)`. Empty means "keep it in memory", which is what
  the earlier stages were, and it keeps those stages gradeable.
- `os.WriteFile` to `store.json.tmp`, then `os.Rename`. `0o600`: a store holds
  Secrets in a real cluster.
- `os.ReadFile` plus `errors.Is(err, fs.ErrNotExist)` — a missing file is an
  empty cluster on a first run, not a failure.
- `maps.Clone` to build the next state without mutating the current one.
- One struct with `Version` and `Objects` for the file. Marshalling the map
  directly and keeping the version elsewhere is two writes that can disagree.

## Hints

<details><summary>Nudge</summary>

Two new methods and one changed one: read the file at startup, and make every
mutation go through a single place that writes before it swaps.
</details>

<details><summary>Approach</summary>

`load()` reads `store.json` into `{version, objects}`, treating a missing file
as empty. `write(next, version)` marshals the snapshot, writes the temp file,
renames it, and only then assigns `s.objects, s.version = next, version`.
`create`, `update` and `remove` each clone the map, apply their change to the
clone, and hand it to `write` with `s.version+1`. Then the handlers report a
failed write as 500 `InternalError` rather than folding it into the conflict
they already handle.
</details>

## Further reading

- [etcd's data model](https://etcd.io/docs/latest/learning/data_model/)
- [Kubernetes components: etcd](https://kubernetes.io/docs/concepts/architecture/#etcd)
- [Operating etcd for Kubernetes](https://kubernetes.io/docs/tasks/administer-cluster/configure-upgrade-etcd/)
