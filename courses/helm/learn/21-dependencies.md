---
title: Package and pin dependencies
concepts: [dependency resolution, lock files and digests, archives, .helmignore]
---

## Core concept

Until now charts/ was filled by hand. Chart.yaml's `dependencies` say what
belongs there, and three commands keep the two in step:

```
app dependency list DIR
app dependency update DIR
app dependency build DIR
app package [-d OUT] [--version V] [--app-version V] [-u] DIR
```

Only two kinds of dependency need to work: `repository: file://../lib`, a
chart directory relative to this one, and no repository at all, a chart
already **unpacked** in `charts/<name>/`. Chart repositories over HTTP need
`helm repo add`, which is out of scope.

**Archives are charts too.** `load` must accept a `.tgz`, both as the chart
argument (`template x out/app-1.0.0.tgz`) and inside charts/. An archive is
gzip over tar; every entry sits under one directory, the chart's name, which
loading drops.

**update** picks a version for each dependency and writes `Chart.lock`:

```yaml
dependencies:
- name: lib
  repository: file://../lib
  version: 0.1.0
- name: pkgd
  repository: ""
  version: '>=1.0.0'
digest: sha256:dfdb154cd4e47160f57d5a560a95269ba539c33699bd45a6c3b124e5345ca234
generated: "2026-11-21T16:44:52.118234-06:00"
```

A `file://` dependency locks the version that chart has, refused unless it
satisfies the constraint (`^0.1.0` → `0.1.0`); one with no repository locks
its constraint as written. Each `file://` chart is packaged into
`charts/<name>-<version>.tgz`, and then every *other* archive in charts/ is
deleted, unless it is a no-repository dependency's. The output:

```
Saving 2 charts
Dependency pkgd did not declare a repository. Assuming it exists in the charts directory
Deleting outdated charts
```

The **digest** is how build knows the lock still matches Chart.yaml:
`sha256` of Go's `json.Marshal` of a two-element array, Chart.yaml's
dependencies then the locked ones, in the `Dependency` struct you already
have. `encoding/json` escapes `>` as `>`, and that is in the hash. If
the new digest equals the old lock's, the file is **not** rewritten, so
`generated` does not move.

**build** with no lock is an update. With one, it recomputes the digest and
refuses any difference, even a constraint that is still satisfied:

```
Error: the lock file (Chart.lock) is out of sync with the dependencies file (Chart.yaml). Please update the dependencies
```

Otherwise it fetches the **locked** versions as update does. A `file://`
chart that has since moved off its locked version stops it, with
`Save error occurred:  <err>` on stdout first (two spaces: that is
`Println` with two arguments).

The other errors, each exit 1:

| | stderr |
|---|---|
| no version satisfies | `Error: can't get a valid version for 1 subchart(s): "lib" (repository "file://../lib", version "^0.2.0"). Make sure a matching chart version exists in the repo, or change the version constraint in Chart.yaml` |
| no repository, not unpacked | `Error: directory app/charts/pkgd not found` |
| locked version gone | `Error: can't get a valid version for dependency lib` |

**list** prints a table, every cell padded, then a blank line:

```
NAME	VERSION	REPOSITORY   	STATUS  
lib 	~0.1   	file://../lib	ok      
pkgd	>=1.0.0	             	unpacked

```

VERSION is the constraint. STATUS looks for `charts/<name>-*.tgz` first:
`ok`, or `wrong version` if that archive's version fails the constraint;
then for an unpacked chart of that name, `unpacked`; else `missing`. Then,
for each chart in charts/ that Chart.yaml does not name:
`WARNING: "app/charts/pkgd-1.2.3.tgz" is not in Chart.yaml.` A chart with
no dependencies prints only `WARNING: no dependencies at app/charts`.

**package** refuses a chart whose charts/ lacks a declared dependency
(`Error: found in Chart.yaml, but missing in charts/ directory: lib`);
`-u` runs a silent update first. It writes `OUT/<name>-<version>.tgz` and
prints `Successfully packaged chart and saved it to: <that path>`. Inside:

- regular files only, mode `0644`, uid and gid 0, no user or group names;
  every entry's time is the moment of packaging, so archives repeat byte
  for byte only within the same second;
- in order: `Chart.yaml` and `Chart.lock`, both **re-serialized** from what
  was loaded (keys sorted), `values.yaml` exactly as written, the schema,
  the templates, the other files, then each subchart the same way under
  `app/charts/<name>/`, **unpacked**, even if it came from an archive;
- `--version` and `--app-version` change Chart.yaml's fields first;
- the gzip header has comment `Helm` and the extra field
  `+aHR0cHM6Ly95b3V0dS5iZS96OVV6MWljandyTQo=`.

**.helmignore** in the chart's root applies whenever a directory is
loaded: one pattern per line, `#` comments; a pattern without a slash
matches the base name, one with a slash the whole path, and a trailing
slash only directories. `.helmignore` itself is packed.

## Go APIs

- `archive/tar` with `compress/gzip`; `gzip.Writer` has `Comment` and
  `Extra` fields for the header.
- `semver.NewConstraint(c).Check(semver.NewVersion(v))` from
  `github.com/Masterminds/semver/v3`, which Helm uses.
- `crypto/sha256` and `encoding/json` for the digest; `sigs.k8s.io/yaml`
  sorts keys and quotes `'>=1.0.0'` as Helm's lock does.
- `filepath.Match` for .helmignore; `filepath.IsLocal` to refuse an archive
  entry that climbs out with `..`.

## Hints

<details><summary>Nudge</summary>

Write the archive writer first: update needs it to fill charts/, and
package is the same function pointed at another directory. For loading, an
archive unpacked to a temporary directory is a directory you can already
load.
</details>

<details><summary>Approach</summary>

Give the chart two more fields, the raw values.yaml and the parsed lock.
`resolve` builds the lock, `fetch` fills charts/ from a list of locked
dependencies; update is resolve then fetch then write, build is check the
digest then fetch.
</details>

## Further reading

- [helm dependency](https://helm.sh/docs/helm/helm_dependency/)
- [Helm's dependency manager](https://github.com/helm/helm/blob/v3.18.6/pkg/downloader/manager.go)
- [How Helm saves an archive](https://github.com/helm/helm/blob/v3.18.6/pkg/chartutil/save.go)
