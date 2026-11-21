// Your diff and safe apply.
//
// Stage 1: `objects -f FILE` reads a multi-document manifest and prints one
// Kind/name line per object.
// Stage 2: `get -f FILE` says which of those objects the cluster already has.
// Stage 3: `patch -f FILE` computes the three-way merge patch client-side
// apply would send.
// Stage 4: `diff -f FILE` lists the field-level changes, and exits 0 for none,
// 1 for some, 2 when it cannot tell.
// Stage 5: `diff --unified` renders the same comparison as diff(1) over YAML.
// Stage 6: the would-be object comes from a server-side apply dry-run, not a
// local merge.
// Stage 7: an object the server refuses is reported with its reason, and the
// rest of the diff still runs.
// Stage 8: `owners -f FILE` reads managedFields: who owns each field, and how.
// Stage 9: `apply -f FILE` applies for real, letting go of fields it drops.
// Stage 10: a field another manager owns is a conflict, named with its owner;
// apply dry-runs everything first and writes nothing if anything is refused.
// Stage 11: --force-conflicts takes the contested fields, and says which.
// Stage 12: apply --set labels what it writes; `prune` lists the set's members
// the manifest no longer names.
// Stage 13: apply --prune deletes them, guarded: never one marked
// prune: disabled, never after a refused apply, never from an empty manifest.
// Stage 14: `drift -f FILE` names the fields someone else has changed since
// this tool applied them, and who.
// Stage 15: `plan -f FILE` says what apply would do to each object, prune
// included, and totals it, without writing anything.
// Stage 16: `plan --out PLAN` also writes the plan down: each change with the
// UID and resourceVersion it was planned against.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/jsonmergepatch"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
	"sigs.k8s.io/yaml"
)

// opts are the flags every command accepts; each command reads the ones it
// needs.
type opts struct {
	file    string
	unified bool
	force   bool
	set     string
	prune   bool
	out     string
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: app <objects> -f FILE")
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	var o opts
	fs.StringVar(&o.file, "f", "", "manifest file")
	fs.BoolVar(&o.unified, "unified", false, "diff: render as a unified diff of YAML")
	fs.BoolVar(&o.force, "force-conflicts", false, "take fields other managers own")
	fs.BoolVar(&o.prune, "prune", false, "apply: delete members of --set the manifest no longer names")
	fs.StringVar(&o.set, "set", "", "apply set: label applied objects as members, prune within it")
	fs.StringVar(&o.out, "out", "", "plan: write the plan to this file")
	fs.Parse(os.Args[2:])

	code, err := run(cmd, o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		if code == 0 {
			code = 2
		}
	}
	os.Exit(code)
}

func run(cmd string, opt opts) (int, error) {
	file := opt.file
	if (cmd == "prune" || opt.prune) && opt.set == "" {
		return 2, fmt.Errorf("prune needs --set: without it nothing marks which objects are this manifest's")
	}
	switch cmd {
	case "objects":
		objs, err := load(file)
		if err != nil {
			return 2, err
		}
		for _, o := range objs {
			fmt.Println(ref(o))
		}
		return 0, nil
	case "get":
		c, objs, err := setup(file, opt.set)
		if err != nil {
			return 2, err
		}
		for _, o := range objs {
			live, err := c.live(o)
			if err != nil {
				return 2, err
			}
			state := "absent"
			if live != nil {
				state = "present"
			}
			fmt.Println(ref(o), state)
		}
		return 0, nil
	case "patch":
		c, objs, err := setup(file, opt.set)
		if err != nil {
			return 2, err
		}
		for _, o := range objs {
			live, err := c.live(o)
			if err != nil {
				return 2, err
			}
			if live == nil {
				fmt.Println(ref(o), "create")
				continue
			}
			p, err := threeWay(o, live)
			if err != nil {
				return 2, fmt.Errorf("%s: %w", ref(o), err)
			}
			fmt.Println(ref(o), string(p))
		}
		return 0, nil
	case "diff":
		c, objs, err := setup(file, opt.set)
		if err != nil {
			return 2, err
		}
		c.force = opt.force
		var pairs [][2]*unstructured.Unstructured
		refused := false
		for _, o := range objs {
			live, err := c.live(o)
			if err != nil {
				return 2, err
			}
			after, err := c.after(o)
			if refusal(err) {
				refused = true
				report(o, err)
				pairs = append(pairs, [2]*unstructured.Unstructured{nil, nil})
				continue
			}
			if err != nil {
				return 2, fmt.Errorf("%s: %w", ref(o), err)
			}
			pairs = append(pairs, [2]*unstructured.Unstructured{live, after})
		}
		if opt.unified {
			code, err := unified(pairs)
			if refused && err == nil {
				code = 2
			}
			return code, err
		}
		changed := false
		for i, o := range objs {
			live, after := pairs[i][0], pairs[i][1]
			if d := fieldDiff(live, after); len(d) > 0 {
				changed = true
				fmt.Println(ref(o))
				for _, l := range d {
					fmt.Println("  " + l)
				}
			}
		}
		switch {
		case refused:
			return 2, nil
		case changed:
			return 1, nil
		}
		return 0, nil
	case "apply":
		c, objs, err := setup(file, opt.set)
		if err != nil {
			return 2, err
		}
		if opt.prune && len(objs) == 0 {
			return 2, fmt.Errorf("refusing to prune from an empty manifest: it would delete every member of set %q", opt.set)
		}
		var taken []string
		if opt.force {
			if taken, err = c.takes(objs); err != nil {
				return 2, err
			}
			c.force = true
		}
		if !c.preflight(objs) {
			return 1, fmt.Errorf("nothing applied: the server refused part of this change")
		}
		for _, t := range taken {
			fmt.Println(t)
		}
		for _, o := range objs {
			if _, err := c.apply(o, false); err != nil {
				return 1, fmt.Errorf("%s: %w", ref(o), err)
			}
			fmt.Println(ref(o), "applied")
		}
		if opt.prune {
			return c.pruneSet(objs, opt.set)
		}
		return 0, nil
	case "prune":
		c, objs, err := setup(file, opt.set)
		if err != nil {
			return 2, err
		}
		left, err := c.leftovers(objs, opt.set)
		if err != nil {
			return 2, err
		}
		for _, o := range left {
			if pruneDisabled(o) {
				fmt.Println(ref(o), "kept: prune disabled")
				continue
			}
			fmt.Println(ref(o))
		}
		return 0, nil
	case "drift":
		c, objs, err := setup(file, opt.set)
		if err != nil {
			return 2, err
		}
		code := 0
		for _, o := range objs {
			live, err := c.live(o)
			if err != nil {
				return 2, err
			}
			if live == nil {
				continue
			}
			ds, err := drift(o, live, c.manager)
			if err != nil {
				return 2, fmt.Errorf("%s: %w", ref(o), err)
			}
			for _, d := range ds {
				fmt.Printf("%s: drift: %s\n", ref(o), d)
				code = 1
			}
		}
		return code, nil
	case "plan":
		c, objs, err := setup(file, opt.set)
		if err != nil {
			return 2, err
		}
		if opt.prune && len(objs) == 0 {
			return 2, fmt.Errorf("refusing to prune from an empty manifest: it would delete every member of set %q", opt.set)
		}
		c.force = opt.force
		changes, refused, err := c.plan(objs, opt)
		if err != nil {
			return 2, err
		}
		count := map[string]int{}
		for _, ch := range changes {
			fmt.Println(ref(ch.obj), ch.action)
			count[ch.action]++
		}
		fmt.Printf("%d to create, %d to update, %d to delete, %d unchanged\n",
			count["create"], count["update"], count["delete"], count["unchanged"])
		switch {
		case refused:
			return 2, nil
		case opt.out != "":
			if err := writePlan(opt.out, opt, changes); err != nil {
				return 2, err
			}
		}
		switch {
		case len(changes) > count["unchanged"]:
			return 1, nil
		}
		return 0, nil
	case "owners":
		c, objs, err := setup(file, opt.set)
		if err != nil {
			return 2, err
		}
		for _, o := range objs {
			live, err := c.live(o)
			if err != nil {
				return 2, err
			}
			if live == nil {
				fmt.Println(ref(o), "absent")
				continue
			}
			owners, err := ownership(live)
			if err != nil {
				return 2, fmt.Errorf("%s: %w", ref(o), err)
			}
			fmt.Println(ref(o))
			for _, w := range owners {
				fmt.Printf("  %s %s %s\n", w.path, w.manager, w.operation)
			}
		}
		return 0, nil
	}
	return 2, fmt.Errorf("unknown command %q", cmd)
}

// setLabel marks an object as a member of a named apply set. Membership is what
// makes pruning safe: only an object this tool labelled can be its leftover.
const setLabel = "byok8s.dev/set"

// leftovers lists the set's members, in the namespaces this manifest touches,
// that the manifest no longer names, sorted by Kind/name.
//
// A removed object's kind need not appear in the manifest any more, so every
// listable namespaced kind is searched. A kind whose API is down is skipped
// with a warning: its leftovers go unseen, so they are kept, never deleted.
// ponytail: one list call per kind per namespace; kubectl's ApplySet records
// the set's kinds on a parent object instead, worth it once the API is large.
func (c *client) leftovers(objs []*unstructured.Unstructured, set string) ([]*unstructured.Unstructured, error) {
	keep := map[string]bool{}
	nss := map[string]bool{c.ns: true}
	for _, o := range objs {
		keep[key(o)] = true
		if o.GetNamespace() != "" {
			nss[o.GetNamespace()] = true
		}
	}
	lists, err := c.disc.ServerPreferredNamespacedResources()
	var partial *discovery.ErrGroupDiscoveryFailed
	switch {
	case errors.As(err, &partial):
		for gv, e := range partial.Groups {
			fmt.Fprintf(os.Stderr, "warning: cannot look for leftovers in %s: %v\n", gv, e)
		}
	case err != nil:
		return nil, err
	}
	lists = discovery.FilteredBy(discovery.SupportsAllVerbs{Verbs: []string{"list", "delete"}}, lists)
	seen := map[types.UID]bool{}
	var out []*unstructured.Unstructured
	for _, l := range lists {
		gv, err := schema.ParseGroupVersion(l.GroupVersion)
		if err != nil {
			return nil, err
		}
		for _, r := range l.APIResources {
			for ns := range nss {
				items, err := c.dyn.Resource(gv.WithResource(r.Name)).Namespace(ns).List(c.ctx,
					metav1.ListOptions{LabelSelector: setLabel + "=" + set})
				if apierrors.IsServiceUnavailable(err) || apierrors.IsNotFound(err) || apierrors.IsMethodNotSupported(err) {
					fmt.Fprintf(os.Stderr, "warning: cannot look for leftovers among %s: %v\n", gv.WithResource(r.Name).GroupResource(), err)
					break
				}
				if err != nil {
					return nil, fmt.Errorf("list %s: %w", r.Name, err)
				}
				for i := range items.Items {
					o := &items.Items[i]
					if keep[key(o)] || seen[o.GetUID()] {
						continue
					}
					seen[o.GetUID()] = true
					out = append(out, o)
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return ref(out[i]) < ref(out[j]) })
	return out, nil
}

// pruneAnnotation set to "disabled" keeps an object out of every prune.
const pruneAnnotation = "byok8s.dev/prune"

func pruneDisabled(o *unstructured.Unstructured) bool {
	return o.GetAnnotations()[pruneAnnotation] == "disabled"
}

// pruneSet deletes the set's leftovers. Each delete is conditional on the UID
// and resourceVersion the plan saw, so an object recreated or changed since —
// relabelled into another set, say — is left alone rather than deleted blind.
func (c *client) pruneSet(objs []*unstructured.Unstructured, set string) (int, error) {
	left, err := c.leftovers(objs, set)
	if err != nil {
		return 1, err
	}
	code := 0
	for _, o := range left {
		if pruneDisabled(o) {
			fmt.Println(ref(o), "kept: prune disabled")
			continue
		}
		r, err := c.resource(o)
		if err != nil {
			return 1, err
		}
		uid, rv := o.GetUID(), o.GetResourceVersion()
		err = r.Delete(c.ctx, o.GetName(), metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv},
		})
		switch {
		case err == nil:
			fmt.Println(ref(o), "pruned")
		case apierrors.IsNotFound(err):
		case apierrors.IsConflict(err):
			fmt.Println(ref(o), "kept: changed since the plan")
			code = 1
		default:
			return 1, fmt.Errorf("%s: %w", ref(o), err)
		}
	}
	return code, nil
}

// change is what an apply would do to one object: create, update, leave it
// unchanged, or delete it as a leftover of the set. uid and rv are the live
// object's as the plan saw it, empty for a create.
type change struct {
	action  string
	obj     *unstructured.Unstructured
	uid, rv string
}

// planFile is a plan as written to disk: the options it was made with, and
// every change in the order apply makes them, deletes last. Applying it later
// is safe only while each object still has the UID and resourceVersion here.
type planFile struct {
	Set     string       `json:"set,omitempty"`
	Prune   bool         `json:"prune"`
	Force   bool         `json:"force"`
	Changes []planChange `json:"changes"`
}

type planChange struct {
	Action          string         `json:"action"`
	UID             string         `json:"uid,omitempty"`
	ResourceVersion string         `json:"resourceVersion,omitempty"`
	Object          map[string]any `json:"object"`
}

// writePlan writes the changes down. An object to apply is stored whole, as it
// would be sent; one to delete only by apiVersion, kind, namespace and name.
func writePlan(path string, opt opts, changes []change) error {
	p := planFile{Set: opt.set, Prune: opt.prune, Force: opt.force, Changes: []planChange{}}
	for _, ch := range changes {
		obj := ch.obj.Object
		if ch.action == "delete" {
			id := &unstructured.Unstructured{}
			id.SetAPIVersion(ch.obj.GetAPIVersion())
			id.SetKind(ch.obj.GetKind())
			id.SetNamespace(ch.obj.GetNamespace())
			id.SetName(ch.obj.GetName())
			obj = id.Object
		}
		p.Changes = append(p.Changes, planChange{ch.action, ch.uid, ch.rv, obj})
	}
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// plan works out every change an apply with these options would make, from a
// dry-run of each object and, with --prune, the set's leftovers. Refusals are
// reported as diff reports them, and leave that object out of the plan.
func (c *client) plan(objs []*unstructured.Unstructured, opt opts) ([]change, bool, error) {
	var out []change
	refused := false
	for _, o := range objs {
		live, err := c.live(o)
		if err != nil {
			return nil, false, err
		}
		after, err := c.after(o)
		if refusal(err) {
			refused = true
			report(o, err)
			continue
		}
		if err != nil {
			return nil, false, fmt.Errorf("%s: %w", ref(o), err)
		}
		ch := change{action: "unchanged", obj: o}
		switch {
		case live == nil:
			ch.action = "create"
		// Same content is still a write if apply would change what byok8s
		// owns: an object another client made, say, is taken over.
		case len(fieldDiff(live, after)) > 0, applied(live, c.manager) != applied(after, c.manager):
			ch.action = "update"
		}
		if live != nil {
			ch.uid, ch.rv = string(live.GetUID()), live.GetResourceVersion()
		}
		out = append(out, ch)
	}
	if !opt.prune {
		return out, refused, nil
	}
	left, err := c.leftovers(objs, opt.set)
	if err != nil {
		return nil, false, err
	}
	for _, o := range left {
		if pruneDisabled(o) {
			fmt.Println(ref(o), "kept: prune disabled")
			continue
		}
		out = append(out, change{action: "delete", obj: o, uid: string(o.GetUID()), rv: o.GetResourceVersion()})
	}
	return out, refused, nil
}

// applied is the set of fields manager has applied to o, as the server
// encodes it, or "" if it has applied none.
func applied(o *unstructured.Unstructured, manager string) string {
	for _, mf := range o.GetManagedFields() {
		if mf.Manager == manager && mf.Operation == metav1.ManagedFieldsOperationApply && mf.FieldsV1 != nil {
			return string(mf.FieldsV1.Raw)
		}
	}
	return ""
}

// key identifies an object across versions of its kind.
func key(o *unstructured.Unstructured) string {
	return o.GroupVersionKind().GroupKind().String() + "/" + o.GetNamespace() + "/" + o.GetName()
}

// owner is one field one manager claims.
type owner struct{ path, manager, operation string }

// ownership decodes managedFields into one entry per leaf field per manager,
// sorted by path. Each entry's fieldsV1 is a set of paths encoded as a tree:
// "f:data" is a map key, "k:{...}" a list item by its key, "." the node itself.
func ownership(live *unstructured.Unstructured) ([]owner, error) {
	var out []owner
	for _, mf := range live.GetManagedFields() {
		if mf.FieldsV1 == nil {
			continue
		}
		set := &fieldpath.Set{}
		if err := set.FromJSON(bytes.NewReader(mf.FieldsV1.Raw)); err != nil {
			return nil, fmt.Errorf("managedFields of %s: %w", mf.Manager, err)
		}
		set.Leaves().Iterate(func(p fieldpath.Path) {
			out = append(out, owner{p.String(), mf.Manager, string(mf.Operation)})
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].path != out[j].path {
			return out[i].path < out[j].path
		}
		return out[i].manager < out[j].manager
	})
	return out, nil
}

// drift lists the fields the manifest sets whose live value differs and that
// manager no longer owns: someone else wrote them since. A differing field the
// manager still owns is a change the manifest is making, not drift. A field
// missing from live has no owner left to name, so it reads as a new field.
// ponytail: lists compare whole, so owning any element hides drift in another.
func drift(desired, live *unstructured.Unstructured, manager string) ([]string, error) {
	owners, err := ownership(live)
	if err != nil {
		return nil, err
	}
	have := leaves(live)
	var out []string
	for p, want := range leaves(desired) {
		got, ok := have[p]
		if !ok || got == want {
			continue
		}
		mine, by := false, ""
		for _, w := range owners {
			if w.path != p && !strings.HasPrefix(w.path, p+".") && !strings.HasPrefix(w.path, p+"[") {
				continue
			}
			if w.manager == manager {
				mine = true
			} else if by == "" {
				by = w.manager
			}
		}
		if mine || by == "" {
			continue
		}
		out = append(out, fmt.Sprintf("%s %s -> %s by %q", p, want, got, by))
	}
	sort.Strings(out)
	return out, nil
}

// client is everything a command needs to address an arbitrary object: a
// dynamic client, a mapper from kinds to resources, and the namespace the
// kubeconfig's context selects.
type client struct {
	ctx    context.Context
	dyn    dynamic.Interface
	disc   discovery.DiscoveryInterface
	mapper meta.RESTMapper
	ns     string

	// manager is the field manager every apply, real or dry-run, is made as.
	manager string
	// force takes fields other managers own instead of stopping at them.
	force bool
}

// setup loads the manifest and connects, the way every cluster command starts.
func setup(file, set string) (*client, []*unstructured.Unstructured, error) {
	objs, err := load(file)
	if err != nil {
		return nil, nil, err
	}
	if set != "" {
		for _, o := range objs {
			l := o.GetLabels()
			if l == nil {
				l = map[string]string{}
			}
			l[setLabel] = set
			o.SetLabels(l)
		}
	}
	kc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{})
	cfg, err := kc.ClientConfig()
	if err != nil {
		return nil, nil, err
	}
	ns, _, err := kc.Namespace()
	if err != nil {
		return nil, nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))
	c := &client{ctx: context.Background(), dyn: dyn, disc: dc, mapper: mapper, ns: ns, manager: "byok8s"}
	for _, o := range objs {
		if _, err := c.resource(o); err != nil {
			return nil, nil, err
		}
	}
	return c, objs, nil
}

// resource resolves an object to the endpoint that serves it, and fills in the
// namespace: the manifest's if it names one, the context's otherwise. A
// cluster-scoped kind never gets one.
func (c *client) resource(o *unstructured.Unstructured) (dynamic.ResourceInterface, error) {
	gvk := o.GroupVersionKind()
	m, err := c.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, fmt.Errorf("%s: the server has no %s: %w", ref(o), gvk, err)
	}
	if m.Scope.Name() != meta.RESTScopeNameNamespace {
		o.SetNamespace("")
		return c.dyn.Resource(m.Resource), nil
	}
	if o.GetNamespace() == "" {
		o.SetNamespace(c.ns)
	}
	return c.dyn.Resource(m.Resource).Namespace(o.GetNamespace()), nil
}

// live fetches the object as the cluster has it, or nil if it does not exist.
func (c *client) live(o *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	r, err := c.resource(o)
	if err != nil {
		return nil, err
	}
	l, err := r.Get(c.ctx, o.GetName(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ref(o), err)
	}
	return l, nil
}

// after is the object as it would be once the manifest is applied, as the
// server itself computes it: a server-side apply with dryRun=All runs
// defaulting, admission and the ownership merge, and stops before storage.
func (c *client) after(o *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	return c.apply(o, true)
}

// preflight dry-runs every object and reports each refusal. Applying only once
// every object would be accepted is what keeps a refused change from landing
// half-done.
func (c *client) preflight(objs []*unstructured.Unstructured) bool {
	ok := true
	for _, o := range objs {
		if _, err := c.apply(o, true); err != nil {
			report(o, err)
			ok = false
		}
	}
	return ok
}

// report prints why the server refused an object. A conflict is listed field by
// field with the manager that owns it, because that is the question the user
// has to answer: take the field, or leave it to them.
func report(o *unstructured.Unstructured, err error) {
	if cs := conflicts(err); cs != nil {
		for _, c := range cs {
			fmt.Printf("%s: conflict: %s owned by %q\n", ref(o), c.field, c.manager)
		}
		return
	}
	fmt.Printf("%s: rejected: %s\n", ref(o), err)
}

// conflict is one field another manager owns.
type conflict struct{ field, manager string }

// conflicts lists the fields a 409 from server-side apply names, or nil when err
// is not such a conflict.
func conflicts(err error) []conflict {
	var status apierrors.APIStatus
	if !apierrors.IsConflict(err) || !errors.As(err, &status) || status.Status().Details == nil {
		return nil
	}
	var out []conflict
	for _, cause := range status.Status().Details.Causes {
		if cause.Type != metav1.CauseTypeFieldManagerConflict {
			continue
		}
		// The message reads: conflict with "manager" [using v1]: .path
		_, rest, _ := strings.Cut(cause.Message, `"`)
		manager, _, _ := strings.Cut(rest, `"`)
		out = append(out, conflict{cause.Field, manager})
	}
	return out
}

// takes dry-runs each object unforced and names every field a forced apply
// will take from another manager, so taking one is never silent.
func (c *client) takes(objs []*unstructured.Unstructured) ([]string, error) {
	var out []string
	for _, o := range objs {
		_, err := c.apply(o, true)
		for _, cf := range conflicts(err) {
			out = append(out, fmt.Sprintf("%s: took %s from %q", ref(o), cf.field, cf.manager))
		}
		if err != nil && !refusal(err) {
			return nil, fmt.Errorf("%s: %w", ref(o), err)
		}
	}
	return out, nil
}

// refusal reports whether err is the server answering no — admission,
// validation, a conflict — rather than failing to answer at all.
func refusal(err error) bool {
	var status apierrors.APIStatus
	return errors.As(err, &status)
}

// apply server-side applies one object as this client's field manager.
func (c *client) apply(o *unstructured.Unstructured, dryRun bool) (*unstructured.Unstructured, error) {
	r, err := c.resource(o)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(o.Object)
	if err != nil {
		return nil, err
	}
	po := metav1.PatchOptions{FieldManager: c.manager, Force: &c.force}
	if dryRun {
		po.DryRun = []string{metav1.DryRunAll}
	}
	return r.Patch(c.ctx, o.GetName(), types.ApplyPatchType, body, po)
}

// fieldDiff lists what changes between two versions of an object, one line per
// leaf field, sorted by path. Lists are compared whole: telling which element
// moved needs the list's merge key, and a whole-list line is never wrong.
func fieldDiff(before, after *unstructured.Unstructured) []string {
	b, a := leaves(before), leaves(after)
	var out []string
	for p, av := range a {
		bv, ok := b[p]
		switch {
		case !ok:
			out = append(out, fmt.Sprintf("+ %s: %s", p, av))
		case bv != av:
			out = append(out, fmt.Sprintf("~ %s: %s -> %s", p, bv, av))
		}
	}
	for p, bv := range b {
		if _, ok := a[p]; !ok {
			out = append(out, fmt.Sprintf("- %s: %s", p, bv))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i][2:] < out[j][2:] })
	return out
}

// unified writes each side's objects as YAML into two directories and lets
// diff(1) compare them, as kubectl diff does. A missing object has no file, and
// -N treats it as empty.
func unified(pairs [][2]*unstructured.Unstructured) (int, error) {
	dir, err := os.MkdirTemp("", "byok8s-diff-")
	if err != nil {
		return 2, err
	}
	defer os.RemoveAll(dir)
	for side, name := range []string{"live", "merged"} {
		if err := os.Mkdir(filepath.Join(dir, name), 0o755); err != nil {
			return 2, err
		}
		for _, p := range pairs {
			o := p[side]
			if o == nil {
				continue
			}
			o = quiet(o)
			y, err := yaml.Marshal(o.Object)
			if err != nil {
				return 2, err
			}
			f := strings.Join([]string{strings.ReplaceAll(o.GetAPIVersion(), "/", "."), o.GetKind(), o.GetNamespace(), o.GetName()}, ".")
			if err := os.WriteFile(filepath.Join(dir, name, f), y, 0o644); err != nil {
				return 2, err
			}
		}
	}
	cmd := exec.Command("diff", "-u", "-N", "live", "merged")
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, nil
	case errors.As(err, &exit) && exit.ExitCode() == 1:
		return 1, nil
	}
	return 2, fmt.Errorf("diff: %w", err)
}

// quiet is a copy without the noise fields.
func quiet(o *unstructured.Unstructured) *unstructured.Unstructured {
	c := o.DeepCopy()
	for _, n := range noise {
		unstructured.RemoveNestedField(c.Object, n...)
	}
	return c
}

// noise is server bookkeeping that changes on every write and says nothing
// about what the write meant.
var noise = [][]string{
	{"metadata", "managedFields"},
	{"metadata", "resourceVersion"},
}

// leaves flattens an object into path → JSON value. A nil object has none.
func leaves(o *unstructured.Unstructured) map[string]string {
	out := map[string]string{}
	if o == nil {
		return out
	}
	c := quiet(o)
	var walk func(path string, v any)
	walk = func(path string, v any) {
		if m, ok := v.(map[string]any); ok && len(m) > 0 {
			for k, child := range m {
				walk(path+"."+k, child)
			}
			return
		}
		js, _ := json.Marshal(v)
		out[path] = string(js)
	}
	walk("", c.Object)
	return out
}

// lastApplied is where client-side apply keeps what it sent last time.
const lastApplied = "kubectl.kubernetes.io/last-applied-configuration"

// threeWay is the JSON merge patch that takes live to desired. Changes come from
// comparing live with desired; deletions only from comparing last-applied with
// desired, so a field someone else added is left alone while a field this
// manifest used to set and no longer does is removed.
func threeWay(desired, live *unstructured.Unstructured) ([]byte, error) {
	original := []byte("{}")
	if a := live.GetAnnotations()[lastApplied]; a != "" {
		original = []byte(a)
	}
	modified, err := json.Marshal(withoutLastApplied(desired).Object)
	if err != nil {
		return nil, err
	}
	current, err := json.Marshal(withoutLastApplied(live).Object)
	if err != nil {
		return nil, err
	}
	var orig map[string]any
	if err := json.Unmarshal(original, &orig); err != nil {
		return nil, fmt.Errorf("unreadable %s annotation: %w", lastApplied, err)
	}
	original, err = json.Marshal(withoutLastApplied(&unstructured.Unstructured{Object: orig}).Object)
	if err != nil {
		return nil, err
	}
	return jsonmergepatch.CreateThreeWayJSONMergePatch(original, modified, current)
}

// withoutLastApplied is a copy with the annotation removed, so the bookkeeping
// never shows up as a change of its own.
func withoutLastApplied(o *unstructured.Unstructured) *unstructured.Unstructured {
	c := o.DeepCopy()
	unstructured.RemoveNestedField(c.Object, "metadata", "annotations", lastApplied)
	if a, ok, _ := unstructured.NestedMap(c.Object, "metadata", "annotations"); ok && len(a) == 0 {
		unstructured.RemoveNestedField(c.Object, "metadata", "annotations")
	}
	return c
}

// load reads every object in a manifest. YAML and JSON documents may be mixed,
// and empty or comment-only documents are skipped, as kubectl skips them.
func load(file string) ([]*unstructured.Unstructured, error) {
	if file == "" {
		return nil, fmt.Errorf("which file? (-f)")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var out []*unstructured.Unstructured
	for i := 1; ; i++ {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, fmt.Errorf("%s document %d: %w", file, i, err)
		}
		if len(m) == 0 {
			continue
		}
		o := &unstructured.Unstructured{Object: m}
		if o.GetAPIVersion() == "" || o.GetKind() == "" || o.GetName() == "" {
			return nil, fmt.Errorf("%s document %d: apiVersion, kind and metadata.name are all required", file, i)
		}
		out = append(out, o)
	}
}

func ref(o *unstructured.Unstructured) string { return o.GetKind() + "/" + o.GetName() }
