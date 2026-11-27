---
title: Delete it, and remember the rest
concepts: [DeleteVolume, ListVolumes, durable state, atomic writes]
---

## Core concept

`DeleteVolume` removes a volume, and is idempotent the other way round from
create: deleting an id that is already gone — or that the driver never made
— is success. The CO may have deleted it on an earlier attempt whose answer
it never saw.

`ListVolumes` reports every volume the driver has, a page at a time if there
are many. A CO uses it to reconcile: what does storage hold that the cluster
no longer knows about?

Both only work if the driver's memory is on disk. The tester restarts your
driver between calls; afterwards the list must be exactly what it was, and
`CreateVolume` of an existing name must still return the volume it made.
Write each record so a crash mid-write cannot leave half a file: write a
temporary file beside it and rename it into place. A rename on one
filesystem is atomic.

This stage: `DeleteVolume`, `ListVolumes` advertised as `LIST_VOLUMES`, and
volumes that survive a restart.

## Go APIs

- `os.RemoveAll`, which is already nil for a missing path; `os.Remove` and
  `os.IsNotExist` for the record.
- `filepath.Glob` over the records.
- `os.Rename`.

## Hints

<details><summary>Nudge</summary>

`ListVolumes` may answer every volume in one page with an empty
`next_token`; paging only matters when there are thousands.
</details>

<details><summary>Approach</summary>

Delete the data, then the record, ignoring "does not exist" for both. List by
globbing `volumes/*.json` under `--root` and loading each one.
</details>

## Further reading

- [DeleteVolume](https://github.com/container-storage-interface/spec/blob/master/spec.md#deletevolume)
- [ListVolumes](https://github.com/container-storage-interface/spec/blob/master/spec.md#listvolumes)
