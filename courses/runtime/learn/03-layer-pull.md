---
title: Pull blobs into a store
concepts: [content-addressable storage, blob, atomic rename, redirect]
---

## Core concept

A runtime keeps what it pulled, so the next container from the same image
starts without the network. The store is content-addressed: each blob lives
at a path made from its digest,

```
<root>/blobs/sha256/<hex>
```

which buys two things. A blob already at its path never needs fetching again,
and two images that share a base layer share the file. containerd's content
store is laid out the same way.

That only works if nothing unverified ever reaches its final name. The pattern
is the same one every package manager uses:

1. stream the download into a temporary file in the same directory, hashing as
   you write;
2. compare the hash with the digest;
3. only then `rename` it into place. A rename within one filesystem is atomic,
   so the final path either does not exist or holds verified content.

A crash or a corrupt download leaves a temporary file, never a lie.

Blobs come from `GET /v2/<repo>/blobs/<digest>`, with the same token. Real
registries rarely serve the bytes themselves: they answer with a redirect to
storage (S3, a CDN), and a client has to follow it. This registry does too.

This stage: `pull [--root DIR] REF` stores the config blob and every layer.
Pulling the same image again fetches nothing, and a blob whose bytes do not
hash to its digest is an error and never stored.

## Go APIs

- `os.CreateTemp(dir, ".partial-")`, `os.Rename`, `defer os.Remove(tmp)`.
- `io.Copy(io.MultiWriter(file, hash), resp.Body)` hashes while writing.
- `flag.NewFlagSet` for a subcommand's `--root`.
- `http.Client` follows redirects for you, keeping headers on the same host.

## Hints

<details><summary>Nudge</summary>

Check whether `blobs/sha256/<hex>` exists before you ask the registry for
anything — that is the whole cache.
</details>

<details><summary>Approach</summary>

`pull` = resolve the manifest as in stage 2, then for the config and each layer:
skip if present; otherwise download to a temp file beside the final path while
hashing, compare, rename. Keep the function that does this — every later stage
starts by pulling.
</details>

## Further reading

- [OCI distribution spec: pulling blobs](https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pulling-blobs)
- [containerd content store](https://github.com/containerd/containerd/blob/main/docs/content-flow.md)
- [rename(2)](https://man7.org/linux/man-pages/man2/rename.2.html)
