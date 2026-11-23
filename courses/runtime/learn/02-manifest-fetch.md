---
title: Resolve a tag to a manifest
concepts: [image manifest, image index, digest, content addressing, Accept header]
---

## Core concept

An image is three kinds of JSON and some tarballs, all addressed by digest
(`sha256:` and the hex hash of the exact bytes):

- An **image manifest** names one **config** blob and an ordered list of
  **layer** blobs.
- An **image index** (Docker calls it a manifest list) names one manifest per
  platform — `linux/amd64`, `linux/arm64`, … — so one tag works on every
  machine.
- The config and layers are blobs, fetched later.

`GET /v2/<repo>/manifests/<tag-or-digest>` returns either a manifest or an
index, and the registry decides which by the `Accept` header: list every media
type you understand, or it may refuse to serve the one it has.

```
application/vnd.oci.image.index.v1+json
application/vnd.oci.image.manifest.v1+json
application/vnd.docker.distribution.manifest.list.v2+json
application/vnd.docker.distribution.manifest.v2+json
```

If you get an index, pick the entry whose `platform` is `linux` and this
machine's architecture — not the first entry — and fetch that manifest by its
digest.

Digests are the reason this is safe. A tag can move; a digest cannot, because
it *is* the content. Whenever you fetch something by digest, hash what came
back and compare. If it does not match, the registry (or anything between you
and it) sent something else, and you stop.

This stage: `manifest REF` prints

```
manifest sha256:…   the image manifest's own digest
config sha256:…
layer sha256:…      one line per layer, bottom first
```

REF may be `HOST/REPO:TAG` or `HOST/REPO@sha256:…`.

## Go APIs

- `req.Header.Set("Accept", …)`; `resp.Header.Get("Content-Type")` tells you
  what came back when the JSON's `mediaType` is missing.
- `crypto/sha256.Sum256` and `encoding/hex` for a digest.
- `runtime.GOARCH` is the architecture your program was built for — the node's.

## Hints

<details><summary>Nudge</summary>

Read the body into memory before decoding it: you need the exact bytes to hash,
and a decoded-then-re-encoded struct hashes differently.
</details>

<details><summary>Approach</summary>

One `fetchManifest(ref)` that GETs, reads the body, hashes it, checks the hash
when `ref` is a digest, and decodes into a struct with `mediaType`, `config`,
`layers` and `manifests`. Call it with the tag; if the result is an index,
find the linux/GOARCH entry and call it again with that digest.
</details>

## Further reading

- [OCI image manifest](https://github.com/opencontainers/image-spec/blob/main/manifest.md)
- [OCI image index](https://github.com/opencontainers/image-spec/blob/main/image-index.md)
- [Descriptors and digests](https://github.com/opencontainers/image-spec/blob/main/descriptor.md)
