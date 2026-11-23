---
title: Get a pull token
concepts: [OCI distribution, bearer token, WWW-Authenticate, docker config.json]
---

## Core concept

A registry speaks the OCI distribution API: plain HTTP under `/v2/`. Before
it hands over anything it wants a token, and it does not tell you up front
where to get one. You ask, it refuses, and the refusal is the instructions:

```
GET /v2/
401 Unauthorized
WWW-Authenticate: Bearer realm="http://HOST/auth/…/token",service="byok8s-registry"
```

The **realm** is a token service, often on another host entirely (Docker Hub's
is `auth.docker.io`). You call it with the `service` from the challenge and a
**scope** naming what you want to do:

```
GET <realm>?service=byok8s-registry&scope=repository:public/demo:pull
200 {"token": "…", "access_token": "…", "expires_in": 300}
```

and from then on every registry request carries `Authorization: Bearer
<token>`. A token is scoped to one repository: a token for `public/demo` does
not open `private/demo`.

Public repositories hand a token to anyone. Private ones want to know who is
asking, so the token request itself carries HTTP Basic credentials — the
username and password from `config.json` in `$DOCKER_CONFIG` (or
`~/.docker`), where `docker login` stores them:

```json
{"auths": {"HOST:PORT": {"auth": "base64(user:password)"}}}
```

No file, or no entry for this registry, means an anonymous request. A wrong
password gets a 401 from the realm, and that is an error, not a token.

This stage: `auth REF` prints the token for REF's repository on its last line
of stdout. `REF` is `HOST:PORT/REPO:TAG`; the registry is plain HTTP.

## Go APIs

- `net/http`: `http.Get`, `http.NewRequest`, `req.SetBasicAuth`.
- `resp.Header.Get("WWW-Authenticate")`, then pull `key="value"` pairs out of
  it (`regexp` is enough).
- `net/url.Values` to build the realm's query string.
- `encoding/base64.StdEncoding` for the `auth` field.

## Hints

<details><summary>Nudge</summary>

Do not hard-code the token URL. Everything you need — realm and service —
comes from the 401 that `GET /v2/` answers with.
</details>

<details><summary>Approach</summary>

Parse REF into host and repository (split on the first `/`, then the last
`:` for the tag). `GET http://HOST/v2/`; on 401 read realm and service; build
`scope=repository:REPO:pull`; look up credentials for HOST in config.json and
attach them as Basic auth if found; decode `token` (or `access_token`) from
the JSON. Return an error — exit 1, message on stderr — on any non-200.
</details>

## Further reading

- [OCI distribution spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md)
- [Docker registry token authentication](https://distribution.github.io/distribution/spec/auth/token/)
- [docker login and config.json](https://docs.docker.com/reference/cli/docker/login/)
