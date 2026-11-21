---
title: Remember the release
concepts: [release records, Secrets storage driver, gzip, base64]
---

## Core concept

An install that only creates objects cannot be listed, upgraded or rolled
back: nothing remembers what it sent. Helm 3 remembers in the cluster, one
**Secret per revision**, in the release's namespace:

```yaml
apiVersion: v1
kind: Secret
type: helm.sh/release.v1
metadata:
  name: sh.helm.release.v1.web.v1      # sh.helm.release.v1.NAME.vREVISION
  labels:
    name: web
    owner: helm
    status: deployed
    version: "1"
    modifiedAt: "1795241294"           # Unix seconds
data:
  release: <base64 of: gzip of: the release as JSON>
```

The labels are how Helm finds revisions without decoding anything
(`owner=helm,name=web`), so they must be exactly these. `data.release` is
encoded twice over: the release JSON is gzipped and base64-encoded into a
string, and that string is the Secret's value, which the API then
base64-encodes again on the wire like any Secret data.

The JSON uses Helm's field names. This is the part the real `helm` reads,
so get it right now and stage 22 gets it for free:

```json
{
  "name": "web",
  "namespace": "shop",
  "version": 1,
  "info": {
    "first_deployed": "2026-11-21T00:08:14.442978-06:00",
    "last_deployed":  "2026-11-21T00:08:14.442978-06:00",
    "deleted": "",
    "description": "Install complete",
    "status": "deployed",
    "notes": "Thanks for installing web.\n"
  },
  "chart": {
    "metadata": { "name": "web", "version": "1.0.0", "appVersion": "2.1", "apiVersion": "v2" },
    "lock": null,
    "templates": [ { "name": "templates/cm.yaml", "data": "<base64>" } ],
    "values": { "msg": "hello" },
    "schema": null,
    "files": null
  },
  "config": { "msg": "hi" },
  "manifest": "---\n# Source: web/templates/cm.yaml\n..."
}
```

- `chart` is the **top chart only**: its own `Chart.yaml` fields, its
  templates and other files as `{name, data}` with the bytes in base64
  (what Go's `encoding/json` does with a `[]byte`), and its own defaults.
  Helm does not store subcharts.
- `config` is the **user's** values, `-f` and `--set` merged, before the
  chart's defaults.
- `manifest` is exactly what `template` prints: the release's record of
  what it sent, without the ownership marks added at create time.
- Times are Go's `time.Time` JSON, RFC 3339 with nanoseconds, except the
  zero time, which Helm writes as `""` (`deleted` until an uninstall).
  `time.Time` refuses to unmarshal `""`, and a record the real `helm`
  wrote always has one, so wrap it in a type of your own whose JSON
  methods turn `""` into the zero time and back.

The grader checks this from both sides: it decodes your Secret, and it runs
the real `helm status`, `helm list` and `helm get values` against it. It also
installs a release with the real `helm` and expects yours to see that the
name is taken. Have helm 3 on your PATH (`mise install` provisions the
version the course is graded with) and do the same while you work:

```
helm -n shop status web
```

The install itself becomes a small state machine:

1. If any revision of the name exists, whatever its status, refuse with
   `cannot re-use a name that is still in use`.
2. Render and run the ownership check. A refusal here records nothing.
3. Write revision 1 as `pending-install` (description
   `Initial install underway`) *before* creating objects.
4. Create the objects. If one fails, rewrite the record as `failed`, with
   description `Release "web" failed: <the error>`, and exit 1.
5. Otherwise rewrite it as `deployed`, `Install complete`, and print the
   summary from the record.

## Go APIs

- `kubernetes.NewForConfig` gives a typed clientset; `CoreV1().Secrets(ns)`
  creates, updates and lists Secrets, with `metav1.ListOptions{LabelSelector: "owner=helm,name=web"}`.
- `compress/gzip` and `encoding/base64` (`StdEncoding`) for the encoding.
- Struct tags such as `json:"first_deployed,omitempty"` give Helm's names.

## Hints

<details><summary>Nudge</summary>

Write the decoder too, and read revisions back by label rather than by
Secret name: the next stages need both, and step 1 already needs the read.
</details>

<details><summary>Approach</summary>

One `Record` struct with Helm's JSON, one function each way between a
record and its Secret, and a `history(name)` that lists by label, decodes
and sorts by version. Then `install` follows the five steps above.
</details>

## Further reading

- [Helm's release record](https://github.com/helm/helm/blob/v3.18.6/pkg/release/release.go)
- [The Secrets storage driver](https://github.com/helm/helm/blob/v3.18.6/pkg/storage/driver/secrets.go)
