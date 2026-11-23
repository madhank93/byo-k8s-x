---
title: Agree with kustomize
concepts: [byte-for-byte output, YAML 1.1 vs 1.2, JSON round trip]
---

## Core concept

Every stage so far compared your output with kustomize's *as data*: parse
both, compare the trees. This one compares the bytes. Two outputs that mean
the same thing can still differ — in key order, in quoting, in how a number
or a date is written — and tools that diff, hash or commit rendered manifests
see every one of those differences.

kustomize's output path is: decode each document, carry it as a map, encode
it to JSON and back out to YAML. Matching it means matching both halves.

**Decoding.** kustomize reads YAML 1.2. In YAML 1.1 — what older Go YAML
libraries speak — `yes`, `no`, `on`, `off`, `y` and `n` are booleans, so a key
written `on:` comes out as `"true":`. In 1.2 only `true` and `false` are. Both
read `0123` as the octal 83 and `0x1F` as 31. An unquoted `2024-01-01` decodes
to a timestamp, and the JSON step turns that into `"2024-01-01T00:00:00Z"`.

**Encoding.** Through JSON every number is a float: `1.50` prints as `1.5`,
`1.0` as `1`, and a number too big to be exact as `1e+20`. Keys come out
sorted. A string is quoted only when YAML would otherwise read it as
something else — `"on"`, `"1"`, `""`, `"a: b"`, `"#x"` — and a string with
newlines is written as a block scalar.

Order you already have: kustomize's own kind ranking first, webhooks last,
then group, version and kind, then namespace and name. Documents are joined
by `---` lines, and an empty build prints nothing at all.

If your program has decoded with a YAML 1.1 library all along, this stage is
where it shows. The fix is small and sits in one place: decode resource and
patch documents as YAML 1.2, round-trip them through JSON, and keep encoding
as before.

## Go APIs

- `go.yaml.in/yaml/v3` decodes YAML 1.2; `encoding/json` turns its
  `time.Time` values into RFC 3339 strings.
- v3 decodes a mapping with a non-string key to `map[any]any`, which JSON
  cannot encode: convert the keys to strings first.
- `sigs.k8s.io/yaml.Marshal` encodes through JSON, as kustomize's output does.

## Hints

<details><summary>Nudge</summary>

Build the same tree with both tools and `diff` the output. Every difference
is either a decoding or an encoding choice.
</details>

<details><summary>Approach</summary>

Decode each document with yaml v3 into `any`, stringify map keys, then
`json.Marshal` and `json.Unmarshal` into your map type. Everything downstream
stays as it was.
</details>

## Further reading

- [YAML 1.2: what changed from 1.1](https://yaml.org/spec/1.2.2/#16-changes-to-the-specification)
- [The Norway problem](https://hitchdev.com/strictyaml/why/implicit-typing-removed/)
- [sigs.k8s.io/yaml](https://github.com/kubernetes-sigs/yaml)
