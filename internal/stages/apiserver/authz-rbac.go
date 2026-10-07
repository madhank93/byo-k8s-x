package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/madhank93/byo-k8s-x/internal/kube"
)

func init() { register(Stage{Slug: "authz-rbac", Run: stageAuthzRBAC}) }

// The token file this stage hands the program: admin is in system:masters,
// alice is in developers, bob is in no group of his own.
const rbacTokens = `admin-token-0000,admin,1000,system:masters
alice-token-0001,alice,1001,developers
bob-token-0002,bob,1002
`

const (
	rbacAPI    = "/apis/rbac.authorization.k8s.io/v1"
	rbacSSAR   = "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews"
	rbacMerge  = "application/merge-patch+json"
	rbacAdmin  = "Bearer admin-token-0000"
	rbacAlice  = "Bearer alice-token-0001"
	rbacBob    = "Bearer bob-token-0002"
	rbacDevCM  = "/api/v1/namespaces/dev/configmaps"
	rbacProdCM = "/api/v1/namespaces/prod/configmaps"
)

// rbacCaller is one user of the server under test, by name and token.
type rbacCaller struct {
	srv        *server
	name, auth string
}

// stageAuthzRBAC checks the server decides every request from roles and
// bindings it stores, and that nobody can use those objects to give
// themselves more than they already have.
//
// Authentication said who is asking. This stage is the second question: may
// they do this. The answer is computed from objects in the store, read again
// on every request, so policy changes are writes like any other.
func stageAuthzRBAC(ctx context.Context, _ *kube.Env, bin string) error {
	if err := rbacAlwaysAllow(ctx, bin); err != nil {
		return err
	}
	if err := rbacAnonymous(ctx, bin); err != nil {
		return err
	}

	dir, err := os.MkdirTemp("", "byok8s-apiserver-*")
	if err != nil {
		return fmt.Errorf("make a directory for the token file: %w", err)
	}
	defer os.RemoveAll(dir)
	file := filepath.Join(dir, "tokens.csv")
	if err := os.WriteFile(file, []byte(rbacTokens), 0o600); err != nil {
		return fmt.Errorf("write the token file: %w", err)
	}
	srv, cleanup, err := serve(ctx, bin, "-token-auth-file", file, "-authorization-mode", "RBAC")
	if err != nil {
		return fmt.Errorf("%w\n\nthis stage passes -token-auth-file <path> -authorization-mode RBAC: the health endpoints still answer with no token and no role", err)
	}
	defer cleanup()
	admin := rbacCaller{srv, "admin", rbacAdmin}
	alice := rbacCaller{srv, "alice", rbacAlice}
	bob := rbacCaller{srv, "bob", rbacBob}

	steps := []func(context.Context, rbacCaller, rbacCaller, rbacCaller) error{
		rbacBootstrap, rbacNamespaceScope, rbacVerbs, rbacResourceNames, rbacSubresource,
		rbacClusterBinding, rbacEscalation, rbacReviews,
	}
	for _, step := range steps {
		if err := step(ctx, admin, alice, bob); err != nil {
			return err
		}
	}
	return nil
}

// rbacAlwaysAllow checks that without the flag nothing is refused, and the
// review endpoint is there anyway, answering yes.
func rbacAlwaysAllow(ctx context.Context, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if _, _, err := srv.names(ctx, "/api/v1/namespaces/default/configmaps"); err != nil {
		return fmt.Errorf("%w\n\nthis program was started without -authorization-mode: the default is AlwaysAllow, and every earlier stage still has to pass", err)
	}

	groups, err := srv.getJSON(ctx, "/apis")
	if err != nil {
		return err
	}
	found := false
	list, _ := groups["groups"].([]any)
	for _, g := range list {
		if m, ok := g.(map[string]any); ok && m["name"] == "authorization.k8s.io" {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("GET /apis lists no group named authorization.k8s.io: kubectl auth can-i finds selfsubjectaccessreviews through discovery, and a group that is not listed is one it concludes the server does not have")
	}
	resources, err := srv.getJSON(ctx, "/apis/authorization.k8s.io/v1")
	if err != nil {
		return err
	}
	items, _ := resources["resources"].([]any)
	var entry map[string]any
	for _, r := range items {
		if m, ok := r.(map[string]any); ok && m["name"] == "selfsubjectaccessreviews" {
			entry = m
		}
	}
	verbs, _ := entry["verbs"].([]any)
	if entry == nil || entry["kind"] != "SelfSubjectAccessReview" || entry["namespaced"] != false || !contains(verbs, "create") {
		return fmt.Errorf("GET /apis/authorization.k8s.io/v1 has selfsubjectaccessreviews as %v, and it is kind SelfSubjectAccessReview, not namespaced, with the verb create", entry)
	}

	review, err := rbacCaller{srv: srv, name: "anybody"}.review(ctx, map[string]any{
		"resourceAttributes": map[string]any{"namespace": "default", "verb": "delete", "resource": "configmaps"},
	})
	if err != nil {
		return err
	}
	if review["allowed"] != true {
		return fmt.Errorf("with no -authorization-mode a review of deleting configmaps answered status %v: AlwaysAllow allows everything, and a review is the same decision the server makes on the request itself", review)
	}
	return nil
}

// rbacAnonymous checks that with RBAC on and no authenticator, the anonymous
// user is refused with 403: the server knows who is asking — nobody — and
// nobody has been granted anything.
func rbacAnonymous(ctx context.Context, bin string) error {
	srv, cleanup, err := serve(ctx, bin, "-authorization-mode", "RBAC")
	if err != nil {
		return fmt.Errorf("%w\n\nthis stage passes -authorization-mode RBAC; health is answered before authorization, so a server with no bindings is still healthy", err)
	}
	defer cleanup()
	anon := rbacCaller{srv: srv, name: "system:anonymous"}
	return anon.denied(ctx, http.MethodGet, "/api/v1/namespaces/default/configmaps", nil,
		`configmaps is forbidden: User "system:anonymous" cannot list resource "configmaps" in API group "" in the namespace "default"`,
		"with RBAC on and no authenticator every request is system:anonymous, in system:unauthenticated, and no binding grants that group anything — 403, not 401: the server has decided who is asking, and the answer is no")
}

// rbacBootstrap checks the administrator can do anything with no bindings,
// and a user with none can do nothing but discovery and self reviews.
func rbacBootstrap(ctx context.Context, admin, alice, bob rbacCaller) error {
	for _, ns := range []string{"dev", "prod"} {
		if err := admin.allowed(ctx, http.MethodPost, "/api/v1/namespaces", nil,
			map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": ns}},
			"admin is in system:masters, which is allowed everything before a single binding exists: it is how the first administrator of a cluster creates the first roles"); err != nil {
			return err
		}
		if err := admin.allowed(ctx, http.MethodPost, "/api/v1/namespaces/"+ns+"/configmaps", nil,
			rbacConfigMap("settings"), "admin is in system:masters"); err != nil {
			return err
		}
	}
	if err := admin.allowed(ctx, http.MethodPost, rbacDevCM, nil, rbacConfigMap("other"), "admin is in system:masters"); err != nil {
		return err
	}

	if err := bob.denied(ctx, http.MethodGet, rbacDevCM, nil,
		`configmaps is forbidden: User "bob" cannot list resource "configmaps" in API group "" in the namespace "dev"`,
		"bob has no bindings yet: authentication says who he is, and with RBAC on that grants nothing by itself. The message is the one the real server writes — kind, name, user, verb, resource, group and namespace — because it is what a person debugging a 403 searches for"); err != nil {
		return err
	}
	if err := bob.denied(ctx, http.MethodGet, rbacDevCM+"/settings", nil,
		`configmaps "settings" is forbidden: User "bob" cannot get resource "configmaps" in API group "" in the namespace "dev"`,
		"bob has no bindings: a request naming one object is the verb get, and the name goes in front of the message"); err != nil {
		return err
	}
	if err := bob.denied(ctx, http.MethodGet, rbacAPI+"/clusterroles", nil,
		`clusterroles.rbac.authorization.k8s.io is forbidden: User "bob" cannot list resource "clusterroles" in API group "rbac.authorization.k8s.io" at the cluster scope`,
		"a cluster-scoped resource is in no namespace: the message says so, and a resource in a named group is qualified by it"); err != nil {
		return err
	}

	// Discovery and self reviews, for anyone signed in.
	for _, path := range []string{"/api", "/api/v1", "/apis", rbacAPI} {
		if err := bob.allowed(ctx, http.MethodGet, path, nil, nil,
			"discovery is open to every authenticated user: the server creates a system:discovery ClusterRole (GET on /api, /api/*, /apis, /apis/* and the like, as nonResourceURLs) bound to the group system:authenticated when it starts with RBAC on — without it, a user cannot find out what exists, and kubectl fails before it asks anything"); err != nil {
			return err
		}
	}
	if err := bob.allowed(ctx, http.MethodPost, authnWhoamiPath, nil,
		map[string]any{"apiVersion": "authentication.k8s.io/v1", "kind": "SelfSubjectReview"},
		"asking who you are is open to every authenticated user: the server's bootstrap system:basic-user ClusterRole grants create on selfsubjectreviews and selfsubjectaccessreviews to system:authenticated"); err != nil {
		return err
	}

	// 401 is still 401: an unknown token is not a user to refuse.
	nobody := rbacCaller{srv: bob.srv, name: "an unknown token", auth: "Bearer nobody-knows-this"}
	res, body, err := nobody.send(ctx, http.MethodGet, rbacDevCM, "", nil)
	if err != nil {
		return err
	}
	if err := wantStatus("GET "+rbacDevCM+" with an unknown token", res, body, http.StatusUnauthorized, "Unauthorized"); err != nil {
		return fmt.Errorf("%w\n\nauthentication runs first: a token nobody knows is 401 whatever the policy says, because there is nobody to ask the policy about", err)
	}

	// A refused write must not have happened.
	if err := bob.denied(ctx, http.MethodPost, rbacDevCM, rbacConfigMap("bobs"),
		`User "bob" cannot create resource "configmaps" in API group "" in the namespace "dev"`, "bob has no bindings"); err != nil {
		return err
	}
	res, body, err = admin.send(ctx, http.MethodGet, rbacDevCM+"/bobs", "", nil)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusNotFound {
		return fmt.Errorf("GET %s/bobs as admin answered %d after bob's create of it was refused: authorization runs before the handler, so a refused write stores nothing\nthe body was:\n%s",
			rbacDevCM, res.StatusCode, tail(string(body)))
	}
	return nil
}

// rbacNamespaceScope checks a RoleBinding grants a ClusterRole in its own
// namespace only, takes effect at once, and stops at once when deleted.
func rbacNamespaceScope(ctx context.Context, admin, alice, bob rbacCaller) error {
	if err := admin.allowed(ctx, http.MethodPost, rbacAPI+"/clusterroles", nil,
		rbacRole("ClusterRole", "configmap-reader", rbacRule([]string{""}, []string{"configmaps"}, []string{"get", "list", "watch"})),
		"admin is in system:masters"); err != nil {
		return err
	}
	if err := admin.allowed(ctx, http.MethodPost, rbacAPI+"/namespaces/dev/rolebindings", nil,
		rbacBinding("RoleBinding", "bob-reads", "ClusterRole", "configmap-reader", rbacSubject("User", "bob")),
		"admin is in system:masters"); err != nil {
		return err
	}

	const granted = "a RoleBinding in dev grants bob the ClusterRole configmap-reader (get, list, watch configmaps) in dev. Policy is read from the store on every request, so a binding counts from the next request on — no restart, no cache that has not caught up"
	if err := bob.allowed(ctx, http.MethodGet, rbacDevCM, nil, nil, granted); err != nil {
		return err
	}
	if err := bob.allowed(ctx, http.MethodGet, rbacDevCM+"/settings", nil, nil, granted); err != nil {
		return err
	}
	if err := bob.watchAllowed(ctx, rbacDevCM+"?watch=true", granted); err != nil {
		return err
	}

	const scoped = "the binding that grants bob configmap-reader is a RoleBinding in dev: a RoleBinding to a ClusterRole grants that role's rules in the binding's namespace and nowhere else, which is how one role is defined once and handed out namespace by namespace"
	if err := bob.denied(ctx, http.MethodGet, rbacProdCM+"/settings", nil,
		`configmaps "settings" is forbidden: User "bob" cannot get resource "configmaps" in API group "" in the namespace "prod"`, scoped); err != nil {
		return err
	}
	if err := bob.denied(ctx, http.MethodGet, "/api/v1/configmaps", nil,
		`configmaps is forbidden: User "bob" cannot list resource "configmaps" in API group "" at the cluster scope`,
		scoped+" — and a list across every namespace is a request at the cluster scope, which no RoleBinding reaches"); err != nil {
		return err
	}
	if err := bob.watchDenied(ctx, rbacProdCM+"?watch=true",
		`User "bob" cannot watch resource "configmaps" in API group "" in the namespace "prod"`, scoped); err != nil {
		return err
	}

	res, body, err := admin.send(ctx, http.MethodDelete, rbacAPI+"/namespaces/dev/rolebindings/bob-reads", "", nil)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("DELETE the RoleBinding bob-reads as admin answered %d\nthe body was:\n%s", res.StatusCode, tail(string(body)))
	}
	return bob.denied(ctx, http.MethodGet, rbacDevCM+"/settings", nil,
		`configmaps "settings" is forbidden: User "bob" cannot get resource "configmaps" in API group "" in the namespace "dev"`,
		"the RoleBinding that granted bob configmap-reader in dev has just been deleted: revoking access is deleting a binding, and it has to count on the very next request — a server that decides from a copy of the policy it read earlier keeps letting him in")
}

// rbacVerbs checks verbs are matched one by one: list and watch are separate
// permissions on the same URL, and reading is not writing.
func rbacVerbs(ctx context.Context, admin, alice, bob rbacCaller) error {
	if err := admin.allowed(ctx, http.MethodPost, rbacAPI+"/namespaces/prod/roles", nil,
		rbacRole("Role", "list-only", rbacRule([]string{""}, []string{"configmaps"}, []string{"list"})),
		"admin is in system:masters"); err != nil {
		return err
	}
	if err := admin.allowed(ctx, http.MethodPost, rbacAPI+"/namespaces/prod/rolebindings", nil,
		rbacBinding("RoleBinding", "alice-lists", "Role", "list-only", rbacSubject("User", "alice")),
		"admin is in system:masters"); err != nil {
		return err
	}
	const listOnly = "alice holds the Role list-only in prod, whose one rule is the verb list on configmaps"
	if err := alice.allowed(ctx, http.MethodGet, rbacProdCM, nil, nil, listOnly); err != nil {
		return err
	}
	if err := alice.watchDenied(ctx, rbacProdCM+"?watch=true",
		`User "alice" cannot watch resource "configmaps" in API group "" in the namespace "prod"`,
		listOnly+": a watch is the same URL as a list with ?watch=true on it, and it is the verb watch — a separate permission, because following a collection for ever is more than reading it once. The decision is made before the stream opens"); err != nil {
		return err
	}
	if err := alice.denied(ctx, http.MethodGet, rbacProdCM+"/settings", nil,
		`configmaps "settings" is forbidden: User "alice" cannot get resource "configmaps" in API group "" in the namespace "prod"`,
		listOnly+": GET of one object by name is the verb get, not list"); err != nil {
		return err
	}
	if err := alice.denied(ctx, http.MethodDelete, rbacProdCM+"/settings", nil,
		`configmaps "settings" is forbidden: User "alice" cannot delete resource "configmaps" in API group "" in the namespace "prod"`,
		listOnly); err != nil {
		return err
	}
	_, _, err := admin.getAs(ctx, rbacProdCM+"/settings", "a DELETE alice was refused must have deleted nothing")
	return err
}

// rbacResourceNames checks a rule narrowed to named objects grants those
// objects and nothing that does not name one.
func rbacResourceNames(ctx context.Context, admin, alice, bob rbacCaller) error {
	if err := admin.allowed(ctx, http.MethodPost, rbacAPI+"/namespaces/dev/roles", nil,
		rbacRole("Role", "settings-only", rbacRule([]string{""}, []string{"configmaps"}, []string{"get", "list", "update"}, "settings")),
		"admin is in system:masters"); err != nil {
		return err
	}
	if err := admin.allowed(ctx, http.MethodPost, rbacAPI+"/namespaces/dev/rolebindings", nil,
		rbacBinding("RoleBinding", "alice-settings", "Role", "settings-only", rbacSubject("User", "alice")),
		"admin is in system:masters"); err != nil {
		return err
	}
	const named = "alice holds the Role settings-only in dev: get, list and update on configmaps, with resourceNames [settings]"
	if err := alice.allowed(ctx, http.MethodGet, rbacDevCM+"/settings", nil, nil, named); err != nil {
		return err
	}
	if err := alice.denied(ctx, http.MethodGet, rbacDevCM+"/other", nil,
		`configmaps "other" is forbidden: User "alice" cannot get resource "configmaps" in API group "" in the namespace "dev"`,
		named+": resourceNames narrows the rule to those objects, and other is not one of them"); err != nil {
		return err
	}
	return alice.denied(ctx, http.MethodGet, rbacDevCM, nil,
		`configmaps is forbidden: User "alice" cannot list resource "configmaps" in API group "" in the namespace "dev"`,
		named+": the rule has list, but a list names no object, so a rule limited to named objects never covers it — it would hand back every object in the namespace, not just settings")
}

// rbacSubresource checks a subresource is its own entry in a rule, in both
// directions, and that a Group subject grants to everyone in the group.
func rbacSubresource(ctx context.Context, admin, alice, bob rbacCaller) error {
	rc := map[string]any{
		"apiVersion": "v1", "kind": "ReplicationController",
		"metadata": map[string]any{"name": "web"},
		"spec": map[string]any{
			"replicas": 1, "selector": map[string]any{"app": "web"},
			"template": map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "web"}}},
		},
	}
	rcs := "/api/v1/namespaces/dev/replicationcontrollers"
	if err := admin.allowed(ctx, http.MethodPost, rcs, nil, rc, "admin is in system:masters"); err != nil {
		return err
	}
	if err := admin.allowed(ctx, http.MethodPost, rbacAPI+"/namespaces/dev/roles", nil,
		rbacRole("Role", "scaler", rbacRule([]string{""}, []string{"replicationcontrollers/scale"}, []string{"get", "patch"})),
		"admin is in system:masters"); err != nil {
		return err
	}
	if err := admin.allowed(ctx, http.MethodPost, rbacAPI+"/namespaces/dev/rolebindings", nil,
		rbacBinding("RoleBinding", "developers-scale", "Role", "scaler", rbacSubject("Group", "developers")),
		"admin is in system:masters"); err != nil {
		return err
	}
	if err := admin.allowed(ctx, http.MethodPost, rbacAPI+"/namespaces/dev/roles", nil,
		rbacRole("Role", "rc-reader", rbacRule([]string{""}, []string{"replicationcontrollers"}, []string{"get"})),
		"admin is in system:masters"); err != nil {
		return err
	}
	if err := admin.allowed(ctx, http.MethodPost, rbacAPI+"/namespaces/dev/rolebindings", nil,
		rbacBinding("RoleBinding", "bob-rc", "Role", "rc-reader", rbacSubject("User", "bob")),
		"admin is in system:masters"); err != nil {
		return err
	}

	const scaler = "the Role scaler in dev grants get and patch on replicationcontrollers/scale, and it is bound to the Group developers — alice is in developers, so it is hers without naming her"
	if err := alice.allowed(ctx, http.MethodGet, rcs+"/web/scale", nil, nil, scaler); err != nil {
		return err
	}
	if err := alice.allowed(ctx, http.MethodPatch, rcs+"/web/scale", map[string]string{"Content-Type": rbacMerge},
		map[string]any{"spec": map[string]any{"replicas": 3}}, scaler); err != nil {
		return err
	}
	if err := alice.denied(ctx, http.MethodGet, rcs+"/web", nil,
		`replicationcontrollers "web" is forbidden: User "alice" cannot get resource "replicationcontrollers" in API group "" in the namespace "dev"`,
		scaler+": a rule for replicationcontrollers/scale grants the scale and nothing else — not the controller, whose template and selector are more than a replica count"); err != nil {
		return err
	}
	if err := bob.denied(ctx, http.MethodGet, rcs+"/web/scale", nil,
		`replicationcontrollers "web" is forbidden: User "bob" cannot get resource "replicationcontrollers/scale" in API group "" in the namespace "dev"`,
		"bob is not in developers, and his own Role rc-reader grants get on replicationcontrollers: the subresource is a resource of its own in a rule, written replicationcontrollers/scale, so permission on the controller does not reach its scale — the message names it that way too"); err != nil {
		return err
	}
	return bob.allowed(ctx, http.MethodGet, rcs+"/web", nil, nil, "bob holds the Role rc-reader in dev: get on replicationcontrollers")
}

// rbacClusterBinding checks a ClusterRoleBinding grants everywhere at once,
// including across namespaces.
func rbacClusterBinding(ctx context.Context, admin, alice, bob rbacCaller) error {
	if err := admin.allowed(ctx, http.MethodPost, rbacAPI+"/clusterrolebindings", nil,
		rbacBinding("ClusterRoleBinding", "bob-reads-everywhere", "ClusterRole", "configmap-reader", rbacSubject("User", "bob")),
		"admin is in system:masters"); err != nil {
		return err
	}
	const everywhere = "a ClusterRoleBinding grants bob configmap-reader: a ClusterRoleBinding applies in every namespace and at the cluster scope"
	for _, path := range []string{rbacProdCM + "/settings", "/api/v1/configmaps"} {
		if err := bob.allowed(ctx, http.MethodGet, path, nil, nil, everywhere); err != nil {
			return err
		}
	}
	res, body, err := admin.send(ctx, http.MethodDelete, rbacAPI+"/clusterrolebindings/bob-reads-everywhere", "", nil)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("DELETE the ClusterRoleBinding bob-reads-everywhere as admin answered %d\nthe body was:\n%s", res.StatusCode, tail(string(body)))
	}
	return bob.denied(ctx, http.MethodGet, rbacProdCM+"/settings", nil,
		`configmaps "settings" is forbidden: User "bob" cannot get resource "configmaps" in API group "" in the namespace "prod"`,
		"the ClusterRoleBinding bob-reads-everywhere has just been deleted, and with it bob's only grant in prod")
}

// rbacEscalation checks nobody can write a role or a binding that grants more
// than they hold, unless they hold escalate or bind.
func rbacEscalation(ctx context.Context, admin, alice, bob rbacCaller) error {
	roles, bindings := rbacAPI+"/namespaces/dev/roles", rbacAPI+"/namespaces/dev/rolebindings"
	if err := admin.allowed(ctx, http.MethodPost, roles, nil,
		rbacRole("Role", "rbac-editor",
			rbacRule([]string{"rbac.authorization.k8s.io"}, []string{"roles", "rolebindings"}, []string{"create", "get", "update", "patch"}),
			rbacRule([]string{""}, []string{"configmaps"}, []string{"get", "list"})),
		"admin is in system:masters"); err != nil {
		return err
	}
	if err := admin.allowed(ctx, http.MethodPost, bindings, nil,
		rbacBinding("RoleBinding", "alice-edits-rbac", "Role", "rbac-editor", rbacSubject("User", "alice")),
		"admin is in system:masters"); err != nil {
		return err
	}
	const editor = "alice holds the Role rbac-editor in dev: create, get, update and patch on roles and rolebindings, and get and list on configmaps"

	if err := alice.allowed(ctx, http.MethodPost, roles, nil,
		rbacRole("Role", "cm-viewer", rbacRule([]string{""}, []string{"configmaps"}, []string{"get"})),
		editor+": a Role granting get on configmaps grants nothing she does not already hold, so she may create it"); err != nil {
		return err
	}

	const escalate = editor + ". Permission to write Roles is not permission to write any Role: if it were, it would be permission to do anything, one Role and one RoleBinding away. A Role may be created or changed only by someone who already holds every permission in it — checked against the object being stored, after the URL check has passed — and the refusal is a 403 Forbidden"
	attempts := []struct {
		what string
		role map[string]any
	}{
		{"delete on configmaps", rbacRole("Role", "cm-deleter", rbacRule([]string{""}, []string{"configmaps"}, []string{"get", "delete"}))},
		{"every verb on configmaps (\"*\")", rbacRole("Role", "cm-star", rbacRule([]string{""}, []string{"configmaps"}, []string{"*"}))},
		{"get on configmaps in every API group (\"*\")", rbacRole("Role", "cm-any-group", rbacRule([]string{"*"}, []string{"configmaps"}, []string{"get"}))},
	}
	for _, a := range attempts {
		if err := alice.escalationRefused(ctx, http.MethodPost, roles, nil, a.role,
			fmt.Sprintf("%s: the Role she tried to create grants %s, which she does not hold", escalate, a.what)); err != nil {
			return err
		}
		name := metaField(a.role, "name")
		res, body, err := admin.send(ctx, http.MethodGet, roles+"/"+name, "", nil)
		if err != nil {
			return err
		}
		if res.StatusCode != http.StatusNotFound {
			return fmt.Errorf("GET the Role %s as admin answered %d after alice's create of it was refused: a refused write stores nothing\nthe body was:\n%s", name, res.StatusCode, tail(string(body)))
		}
	}

	// The same check on every write that stores a Role, not just create.
	grown := rbacRole("Role", "cm-viewer", rbacRule([]string{""}, []string{"configmaps"}, []string{"get", "delete"}))
	if err := alice.escalationRefused(ctx, http.MethodPut, roles+"/cm-viewer", nil, grown,
		escalate+": an update that adds delete to her own Role cm-viewer is the same escalation as a create would be"); err != nil {
		return err
	}
	patch := map[string]any{"rules": []any{rbacRule([]string{""}, []string{"configmaps"}, []string{"get", "delete"})}}
	if err := alice.escalationRefused(ctx, http.MethodPatch, roles+"/cm-viewer", map[string]string{"Content-Type": rbacMerge}, patch,
		escalate+": a merge patch that adds delete to cm-viewer is the same escalation, and the check is on the object the patch produces"); err != nil {
		return err
	}
	stored, _, err := admin.getAs(ctx, roles+"/cm-viewer", "")
	if err != nil {
		return err
	}
	if raw, _ := json.Marshal(stored["rules"]); strings.Contains(string(raw), "delete") {
		return fmt.Errorf("the Role cm-viewer now has rules %s after alice's update and patch adding delete were refused: a refused write stores nothing", raw)
	}

	// Bindings: no binding to a role whose permissions she does not hold.
	if err := admin.allowed(ctx, http.MethodPost, rbacAPI+"/clusterroles", nil,
		rbacRole("ClusterRole", "configmap-writer", rbacRule([]string{""}, []string{"configmaps"}, []string{"create", "update", "delete"})),
		"admin is in system:masters"); err != nil {
		return err
	}
	if err := alice.escalationRefused(ctx, http.MethodPost, bindings, nil,
		rbacBinding("RoleBinding", "bob-writes", "ClusterRole", "configmap-writer", rbacSubject("User", "bob")),
		editor+": she may create RoleBindings, but binding the ClusterRole configmap-writer hands out create, update and delete on configmaps, which she does not hold. Creating a binding needs either every permission in the role it refers to, or the verb bind on that role"); err != nil {
		return err
	}
	if err := alice.allowed(ctx, http.MethodPost, bindings, nil,
		rbacBinding("RoleBinding", "bob-views", "Role", "cm-viewer", rbacSubject("User", "bob")),
		editor+": binding the Role cm-viewer to bob hands out get on configmaps, which she holds"); err != nil {
		return err
	}
	if err := bob.allowed(ctx, http.MethodGet, rbacDevCM+"/other", nil, nil, "alice bound the Role cm-viewer (get on configmaps) to bob in dev"); err != nil {
		return err
	}

	// escalate is the way round it, for whoever is trusted to grant what
	// they do not use.
	if err := admin.allowed(ctx, http.MethodPost, roles, nil,
		rbacRole("Role", "escalator", rbacRule([]string{"rbac.authorization.k8s.io"}, []string{"roles"}, []string{"escalate"})),
		"admin is in system:masters"); err != nil {
		return err
	}
	if err := admin.allowed(ctx, http.MethodPost, bindings, nil,
		rbacBinding("RoleBinding", "alice-escalates", "Role", "escalator", rbacSubject("User", "alice")),
		"admin is in system:masters"); err != nil {
		return err
	}
	return alice.allowed(ctx, http.MethodPost, roles, nil,
		rbacRole("Role", "cm-deleter", rbacRule([]string{""}, []string{"configmaps"}, []string{"get", "delete"})),
		editor+", and now the Role escalator in dev too, whose one rule is the verb escalate on roles: escalate is what lets a user create a Role holding permissions they do not have themselves")
}

// rbacReviews checks a SelfSubjectAccessReview answers with the decision the
// request itself would get.
func rbacReviews(ctx context.Context, admin, alice, bob rbacCaller) error {
	cases := []struct {
		spec map[string]any
		want bool
		why  string
	}{
		{map[string]any{"resourceAttributes": map[string]any{"namespace": "dev", "verb": "get", "resource": "configmaps", "name": "settings"}},
			true, "alice holds get on configmaps in dev"},
		{map[string]any{"resourceAttributes": map[string]any{"namespace": "dev", "verb": "list", "resource": "configmaps"}},
			true, "alice holds list on configmaps in dev, through the Role rbac-editor"},
		{map[string]any{"resourceAttributes": map[string]any{"namespace": "prod", "verb": "delete", "resource": "configmaps", "name": "settings"}},
			false, "alice holds nothing but list on configmaps in prod"},
		{map[string]any{"resourceAttributes": map[string]any{"namespace": "dev", "verb": "patch", "resource": "replicationcontrollers", "subresource": "scale", "name": "web"}},
			true, "alice is in developers, and the Group binding developers-scale grants patch on replicationcontrollers/scale in dev"},
		{map[string]any{"resourceAttributes": map[string]any{"namespace": "dev", "verb": "patch", "resource": "replicationcontrollers", "name": "web"}},
			false, "the scale rule grants the subresource, not the controller"},
		{map[string]any{"resourceAttributes": map[string]any{"verb": "list", "group": "rbac.authorization.k8s.io", "resource": "clusterroles"}},
			false, "alice has no cluster-wide grant on clusterroles"},
		{map[string]any{"nonResourceAttributes": map[string]any{"path": "/apis", "verb": "get"}},
			true, "every authenticated user may GET discovery paths, through system:discovery"},
	}
	for _, c := range cases {
		status, err := alice.review(ctx, c.spec)
		if err != nil {
			return err
		}
		if status["allowed"] != c.want {
			spec, _ := json.Marshal(c.spec)
			return fmt.Errorf("a SelfSubjectAccessReview from alice asking %s answered status %v, and the answer is allowed: %v — %s. kubectl auth can-i is this review, and it is only useful if it gives the decision the real request would get",
				spec, status, c.want, c.why)
		}
	}
	return nil
}

// review sends a SelfSubjectAccessReview and returns its status.
func (c rbacCaller) review(ctx context.Context, spec map[string]any) (map[string]any, error) {
	body := map[string]any{"apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview", "spec": spec}
	res, raw, err := c.send(ctx, http.MethodPost, rbacSSAR, "", body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("POST %s as %s answered %d rather than 201: a review is a create, answered with status filled in — and every authenticated user may create one, through the bootstrap system:basic-user ClusterRole\nthe body was:\n%s",
			rbacSSAR, c.name, res.StatusCode, tail(string(raw)))
	}
	obj, err := decode(raw)
	if err != nil {
		return nil, fmt.Errorf("the reply to POST %s is not JSON (%w)\nthe body was:\n%s", rbacSSAR, err, tail(string(raw)))
	}
	if obj["kind"] != "SelfSubjectAccessReview" || obj["apiVersion"] != "authorization.k8s.io/v1" {
		return nil, fmt.Errorf("the reply to POST %s has kind %v, apiVersion %v, and it is the SelfSubjectAccessReview that was sent, from authorization.k8s.io/v1", rbacSSAR, obj["kind"], obj["apiVersion"])
	}
	status, ok := obj["status"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("the SelfSubjectAccessReview has no status: status.allowed is the answer\nthe body was:\n%s", tail(string(raw)))
	}
	if _, ok := status["allowed"].(bool); !ok {
		return nil, fmt.Errorf("the SelfSubjectAccessReview's status.allowed is %v, and it is true or false\nthe body was:\n%s", status["allowed"], tail(string(raw)))
	}
	return status, nil
}

// allowed sends a request as this caller and insists it is not refused.
func (c rbacCaller) allowed(ctx context.Context, method, path string, headers map[string]string, body any, why string) error {
	res, raw, err := c.send(ctx, method, path, headers["Content-Type"], body)
	if err != nil {
		return err
	}
	if res.StatusCode >= 200 && res.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("%s %s as %s answered %d, and it is allowed: %s\nthe body was:\n%s", method, path, c.name, res.StatusCode, why, tail(string(raw)))
}

// denied sends a request as this caller and insists it is refused with 403
// Forbidden and the message the real server writes.
func (c rbacCaller) denied(ctx context.Context, method, path string, body any, message, why string) error {
	res, raw, err := c.send(ctx, method, path, "", body)
	if err != nil {
		return err
	}
	what := fmt.Sprintf("%s %s as %s", method, path, c.name)
	if res.StatusCode != http.StatusForbidden {
		return fmt.Errorf("%s answered %d, and it is refused with 403: %s\nthe body was:\n%s", what, res.StatusCode, why, tail(string(raw)))
	}
	if err := wantStatus(what, res, raw, http.StatusForbidden, "Forbidden"); err != nil {
		return err
	}
	status, _ := decode(raw)
	if got, _ := status["message"].(string); !strings.Contains(got, message) {
		return fmt.Errorf("%s was refused with the message %q, and it says %q: the message is the request's attributes in the words the real server uses, which is what a person searches for when they hit it",
			what, got, message)
	}
	return nil
}

// escalationRefused insists a write of a role or binding is refused as an
// escalation: 403 Forbidden, for what the object grants.
func (c rbacCaller) escalationRefused(ctx context.Context, method, path string, headers map[string]string, body any, why string) error {
	res, raw, err := c.send(ctx, method, path, headers["Content-Type"], body)
	if err != nil {
		return err
	}
	what := fmt.Sprintf("%s %s as %s", method, path, c.name)
	if res.StatusCode != http.StatusForbidden {
		return fmt.Errorf("%s answered %d, and it is refused with 403: %s\nthe body was:\n%s", what, res.StatusCode, why, tail(string(raw)))
	}
	return wantStatus(what, res, raw, http.StatusForbidden, "Forbidden")
}

// getAs reads one object as this caller, which has to succeed.
func (c rbacCaller) getAs(ctx context.Context, path, why string) (map[string]any, []byte, error) {
	res, raw, err := c.send(ctx, http.MethodGet, path, "", nil)
	if err != nil {
		return nil, nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("GET %s as %s answered %d rather than 200: %s\nthe body was:\n%s", path, c.name, res.StatusCode, why, tail(string(raw)))
	}
	obj, err := decode(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("GET %s did not answer JSON (%w)", path, err)
	}
	return obj, raw, nil
}

// watchAllowed opens a watch as this caller and insists it starts.
func (c rbacCaller) watchAllowed(ctx context.Context, path, why string) error {
	res, body, err := c.openWatch(ctx, path)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s as %s answered %d, and the watch is allowed: %s\nthe body was:\n%s", path, c.name, res.StatusCode, why, tail(string(body)))
	}
	return nil
}

// watchDenied opens a watch as this caller and insists it is refused before
// the stream starts.
func (c rbacCaller) watchDenied(ctx context.Context, path, message, why string) error {
	res, body, err := c.openWatch(ctx, path)
	if err != nil {
		return err
	}
	what := fmt.Sprintf("GET %s as %s", path, c.name)
	if res.StatusCode == http.StatusOK {
		return fmt.Errorf("%s answered 200 and started streaming, and it is refused with 403: %s. A watch is authorized once, as the verb watch, before the stream opens", what, why)
	}
	if err := wantStatus(what, res, body, http.StatusForbidden, "Forbidden"); err != nil {
		return fmt.Errorf("%w\n\n%s", err, why)
	}
	status, _ := decode(body)
	if got, _ := status["message"].(string); !strings.Contains(got, message) {
		return fmt.Errorf("%s was refused with the message %q, and it says %q: a request with ?watch=true is the verb watch", what, got, message)
	}
	return nil
}

// openWatch opens a watch and returns once the status is in: a 200 is closed
// straight away, and anything else is read whole.
func (c rbacCaller) openWatch(ctx context.Context, path string) (*http.Response, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.srv.url+path, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Authorization", c.auth)
	res, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("GET %s as %s: %w\n\na watch answers its status as soon as it is open, or is refused straight away\nthe program said:\n%s", path, c.name, err, tail(c.srv.p.Stdout()))
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		return res, nil, nil
	}
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<16))
	return res, body, nil
}

// send makes one request as this caller. contentType defaults to JSON.
func (c rbacCaller) send(ctx context.Context, method, path, contentType string, body any) (*http.Response, []byte, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, nil, fmt.Errorf("encode the request body: %w", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.srv.url+path, reader)
	if err != nil {
		return nil, nil, err
	}
	if body != nil {
		if contentType == "" {
			contentType = "application/json"
		}
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept", "application/json")
	if c.auth != "" {
		req.Header.Set("Authorization", c.auth)
	}
	res, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("%s %s as %s: %w\nthe program said:\n%s", method, path, c.name, err, tail(c.srv.p.Stdout()))
	}
	defer res.Body.Close()
	got, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, nil, fmt.Errorf("read the response body: %w", err)
	}
	return res, got, nil
}

func rbacConfigMap(name string) map[string]any {
	return map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": name}, "data": map[string]any{"k": "v"}}
}

func rbacRule(groups, resources, verbs []string, names ...string) map[string]any {
	rule := map[string]any{"apiGroups": groups, "resources": resources, "verbs": verbs}
	if len(names) > 0 {
		rule["resourceNames"] = names
	}
	return rule
}

func rbacRole(kind, name string, rules ...map[string]any) map[string]any {
	return map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": kind, "metadata": map[string]any{"name": name}, "rules": rules}
}

func rbacSubject(kind, name string) map[string]any {
	return map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": kind, "name": name}
}

func rbacBinding(kind, name, roleKind, role string, subjects ...map[string]any) map[string]any {
	return map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1", "kind": kind, "metadata": map[string]any{"name": name},
		"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": roleKind, "name": role},
		"subjects": subjects,
	}
}
