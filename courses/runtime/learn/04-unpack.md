---
title: Apply the layers
concepts: [layer tarball, whiteout, opaque directory, path traversal, hard link]
---

## Core concept

A layer is a gzipped tar of the changes one build step made. Applied in order,
bottom first, onto an empty directory, the layers add up to the image's
filesystem. Adding and replacing files is what tar already does. Deleting is
not, so layers encode it with marker files called **whiteouts**:

- `dir/.wh.NAME` — `NAME` existed in a lower layer and is now gone. Delete it,
  and do not create the marker.
- `dir/.wh..wh..opq` — an **opaque** directory: everything lower layers put in
  `dir` is gone, but what this layer puts there stays. Empty the directory
  before this layer's entries for it land.

Each entry also carries what a plain copy would lose: mode bits, owner
uid/gid, symlink targets, and hard links (a `TypeLink` entry naming another
path in the image, which must become a second name for the *same* inode).

A layer is also untrusted input written as root. An entry named
`../../etc/cron.d/x` must not land outside the destination, and nor must an
entry written *through* a symlink an earlier entry planted
(`a -> /etc`, then `a/passwd`). Clean every name against `/` before joining it
to the destination, and refuse to write beneath a symlink.

This stage: `unpack [--root DIR] REF DEST` pulls the image and applies its
layers into `DEST`.

## Go APIs

- `compress/gzip.NewReader`, `archive/tar.NewReader`, `tr.Next()`.
- `hdr.Typeflag` (`tar.TypeDir`, `TypeReg`, `TypeSymlink`, `TypeLink`),
  `hdr.Mode`, `hdr.Uid`, `hdr.Gid`, `hdr.Linkname`.
- `filepath.Clean("/" + name)` then `filepath.Join(dest, …)`.
- `os.Symlink`, `os.Link`, `os.Lchown`, `os.Lstat`, `os.RemoveAll`.
- `syscall.Chmod` keeps the setuid and sticky bits `os.Chmod` would translate.

## Hints

<details><summary>Nudge</summary>

Handle whiteouts before anything else for each entry, and remember a later
layer may replace a file with a directory or a directory with a symlink:
remove what is there first.
</details>

<details><summary>Approach</summary>

For each layer, for each entry: clean the name; check no parent inside `dest`
is a symlink; `MkdirAll` the parent. If the base name is `.wh..wh..opq`, empty
the parent; if it starts with `.wh.`, `RemoveAll` the named sibling; otherwise
remove anything already at the target (unless both are directories), create it
by type, then `Lchown` and chmod.
</details>

<details><summary>Implementation</summary>

A hard link's target is a path inside the image, so join it the same way:
`os.Link(filepath.Join(dest, filepath.Clean("/"+hdr.Linkname)), target)`.
Set mode after owner — `chown` clears setuid bits. Never chmod a symlink: its
mode means nothing on Linux, and chmod follows it to whatever it points at.
</details>

## Further reading

- [OCI image layer spec: whiteouts](https://github.com/opencontainers/image-spec/blob/main/layer.md#whiteouts)
- [archive/tar](https://pkg.go.dev/archive/tar)
- [Zip Slip, the path traversal class](https://security.snyk.io/research/zip-slip-vulnerability)
