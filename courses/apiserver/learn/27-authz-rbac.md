---
title: May they do this
concepts: [rbac, authorization, roles, bindings, escalation-prevention, access-review]
---

## Core concept

Authentication answered *who is asking*. Every user it names can still do
everything. This stage asks the second question — *may they do this* — and
answers it the way every real cluster does: from Roles and bindings stored in
the server itself, as ordinary objects in `rbac.authorization.k8s.io/v1`.

```
-authorization-mode   AlwaysAllow (the default) or RBAC
```

Without the flag nothing changes, and every earlier stage still passes.

**A request is a handful of attributes.** Before any handler runs, the server
reads off the method and URL who is asking, which verb, and at what:

```
GET    /api/v1/namespaces/dev/configmaps              list    configmaps
GET    /api/v1/namespaces/dev/configmaps?watch=true   watch   configmaps
GET    /api/v1/namespaces/dev/configmaps/settings     get     configmaps  "settings"
PATCH  /api/v1/namespaces/dev/replicationcontrollers/web/scale
                                                      patch   replicationcontrollers/scale "web"
GET    /api/v1/configmaps                             list    configmaps  (cluster scope)
GET    /apis                                          get     path "/apis" (non-resource)
```

The group is `""` for `/api/v1` and the path segment after `/apis/` otherwise.
POST is `create`, PUT `update`, DELETE `delete` (or `deletecollection` with no
name). The body is not an attribute.

**Rules grant; nothing denies.** A Role holds rules, each a set of `verbs`,
`apiGroups` and `resources` (optionally narrowed by `resourceNames`), or of
`nonResourceURLs`. A rule allows a request when every list matches, `*` matching
anything. A subresource is its own entry — `replicationcontrollers/scale` — so
a rule for the controller does not reach its scale, and back. A rule with
`resourceNames` never covers a request that names no object: a list would hand
back every object, not just the named ones.

**Bindings decide where.** A binding names a role (`roleRef`) and who gets it
(`subjects`: a `User` by name, or a `Group` the user is in):

```
ClusterRoleBinding → ClusterRole   everywhere, cluster scope included
RoleBinding        → Role          in the binding's namespace
RoleBinding        → ClusterRole   that role's rules, in the binding's namespace only
```

The request is allowed if any rule from any binding that applies allows it,
and refused otherwise: `403 Forbidden`, with a message in the real server's
words, because that is what people paste into a search:

```
configmaps "settings" is forbidden: User "bob" cannot get resource "configmaps" in API group "" in the namespace "prod"
```

An unknown token is still `401` — there is nobody to ask the policy about.

**Read the policy on every request.** A binding counts from the next request
after it is written, and stops the moment it is deleted. A copy taken at
startup is a server that keeps letting people in after they were revoked.

**Bootstrapping.** Two things exist before any binding does. The group
`system:masters` is allowed everything — that is how the first administrator
creates the first roles. And at startup the server creates two ClusterRoles,
bound to `system:authenticated`: `system:discovery` (GET on `/api`, `/api/*`,
`/apis`, `/apis/*` and the like) and `system:basic-user` (create on
`selfsubjectreviews` and `selfsubjectaccessreviews`). Without them a user with
no bindings cannot even discover that they are allowed nothing. Health is still
answered before authentication and authorization.

**No escalation.** Permission to create Roles must not be permission to do
anything. So a write of a Role or ClusterRole — create, update, patch or apply
— is refused (403) unless the writer already holds every permission the
stored object grants, in that namespace. A binding is refused unless the writer
holds every permission of the role it refers to. A wildcard is only covered by
the same wildcard. Two verbs are the way round it, for whoever is trusted to
hand out what they do not use: `escalate` on roles, and `bind` on the role a
binding refers to.

**May I?** `POST /apis/authorization.k8s.io/v1/selfsubjectaccessreviews` with
`spec.resourceAttributes` (or `nonResourceAttributes`) asks about a request
without making it. The answer is the same object with `status.allowed` set —
the decision the real request would get. That is all `kubectl auth can-i` is.
Under AlwaysAllow the answer is always yes.

## Go APIs

- `strings.Split(strings.Trim(r.URL.Path, "/"), "/")` to read the attributes
  off the path.
- `slices.Contains` for `verbs`, `apiGroups` and the subject's groups.
- `json.Marshal` then `json.Unmarshal` into a typed struct is the short way
  to read rules out of a stored `map[string]any`.

## Hints

<details><summary>Nudge</summary>

Authorization is middleware between authentication and the mux: it sees the
user, and decides before any handler can store anything. Escalation cannot be
decided there — it is a question about the body — so it runs where the object
is about to be stored, on every path that stores a Role or binding.
</details>

<details><summary>Approach</summary>

Write one `decide(user, attributes) bool`: if the user is in `system:masters`,
yes. Otherwise list every ClusterRoleBinding, and when the request has a
namespace every RoleBinding in it; for each that names the user or one of
their groups, load its role and test each rule. The middleware and the access
review both call it. For escalation, break the new object's rules into single
permissions — one verb, group, resource and name — and refuse if any one is
not allowed by the writer's own rules in that namespace. Escalation checks run
inside a store write, so make sure `decide` does not take a lock the write
already holds.
</details>

## Further reading

- [Using RBAC authorization](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)
- [Privilege escalation prevention](https://kubernetes.io/docs/reference/access-authn-authz/rbac/#privilege-escalation-prevention-and-bootstrapping)
- [Authorization overview: request attributes](https://kubernetes.io/docs/reference/access-authn-authz/authorization/#review-your-request-attributes)
- [Checking API access](https://kubernetes.io/docs/reference/access-authn-authz/authorization/#checking-api-access)
