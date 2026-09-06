// Your kubectl.
//
// Stage 1: print the API server, found through the standard loading rules.
// Stage 2: let --context choose which cluster that is.
// Stage 3: `version` asks the server what it is.
// Stage 4: `get pods` lists the namespace the context names.
// Stage 5: -n overrides that namespace.
// Stage 6: print an aligned table.
// Stage 7: humanise the age.
// Stage 8: -o json.
// Stage 9: -o yaml.
// Stage 10: -A looks in every namespace.
// Stage 11: get one object by name.
// Stage 12: api-resources asks the server what it serves.
// Stage 13: any spelling of a resource resolves to the same thing.
// Stage 14: get anything, through the dynamic client.
// Stage 15: -o wide adds the columns that resource can offer.
// Stage 16: -l filters by label, on the server.
// Stage 17: --field-selector filters on the object itself.
// Stage 18: print rows in a stable order.
// Stage 19: -w keeps printing as things change.
// Stage 20: delete an object.
// Stage 21: create from a manifest.
// Stage 22: apply, which is create-or-update and records who owns what.
// Stage 23: a conflict is a question, and --force-conflicts is the answer.
// Stage 24: patch changes a field without sending the object.
// Stage 25: scale, through a subresource of its own.
// Stage 26: describe one object.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"
)

func main() {
	fs := flag.NewFlagSet("byok8s", flag.ContinueOnError)
	kctx := fs.String("context", "", "the kubeconfig context to use")
	namespace := fs.String("namespace", "", "the namespace to work in")
	fs.StringVar(namespace, "n", "", "the namespace to work in (shorthand)")
	output := fs.String("output", "", "output format: json or yaml")
	fs.StringVar(output, "o", "", "output format (shorthand)")
	selector := fs.String("selector", "", "label selector, e.g. app=web")
	fs.StringVar(selector, "l", "", "label selector (shorthand)")
	fieldSelector := fs.String("field-selector", "", "field selector, e.g. metadata.name=web")
	watch := fs.Bool("watch", false, "keep printing as objects change")
	fs.BoolVar(watch, "w", false, "keep printing as objects change (shorthand)")
	replicas := fs.Int("replicas", -1, "how many replicas to scale to")
	patchBody := fs.String("patch", "", "a merge patch")
	fs.StringVar(patchBody, "p", "", "a merge patch (shorthand)")
	force := fs.Bool("force-conflicts", false, "take ownership of fields another manager owns")
	filename := fs.String("filename", "", "a manifest to send")
	fs.StringVar(filename, "f", "", "a manifest to send (shorthand)")
	allNS := fs.Bool("all-namespaces", false, "list across every namespace")
	fs.BoolVar(allNS, "A", false, "list across every namespace (shorthand)")

	args, err := parseInterspersed(fs, os.Args[1:])
	if err != nil {
		os.Exit(1)
	}

	cfg, ns, err := resolve(*kctx, *namespace)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loading kubeconfig:", err)
		os.Exit(1)
	}

	switch arg(args, 0) {
	case "version":
		err = printVersion(cfg)
	case "api-resources":
		err = apiResources(cfg)
	case "create":
		err = create(cfg, ns, *filename)
	case "apply":
		err = apply(cfg, ns, *filename, *force)
	case "describe":
		err = describe(cfg, ns, arg(args, 1), arg(args, 2))
	case "scale":
		err = scale(cfg, ns, arg(args, 1), arg(args, 2), *replicas)
	case "patch":
		err = patch(cfg, ns, arg(args, 1), arg(args, 2), *patchBody)
	case "delete":
		err = deleteObject(cfg, ns, arg(args, 1), arg(args, 2))
	case "get":
		if *allNS {
			// The empty namespace is not a fallback here, it is the request:
			// a list whose path carries no namespace is a cluster-wide list.
			ns = ""
		}
		err = get(cfg, ns, arg(args, 1), arg(args, 2), *output, *selector, *fieldSelector, *allNS, *watch)
	default:
		fmt.Println(cfg.Host)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// parseInterspersed parses flags that appear anywhere, returning the
// positional arguments in order.
//
// Go's flag package stops at the first non-flag argument, so "get pods -n web"
// would leave -n unparsed and silently use the wrong namespace. kubectl allows
// flags before, after and between its arguments, and a tool that only accepts
// them first is quietly wrong rather than loudly unsupported. Parsing in a
// loop — take a flag run, take one positional, repeat — gets that behaviour in
// a few lines.
func parseInterspersed(fs *flag.FlagSet, argv []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(argv); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return positional, nil
		}
		positional = append(positional, fs.Arg(0))
		argv = fs.Args()[1:]
	}
}

func arg(args []string, i int) string {
	if i < len(args) {
		return args[i]
	}
	return ""
}

// resolve returns the connection and the namespace to act in.
//
// The loading rules are the whole point: $KUBECONFIG first (colon separated,
// merged in order), then ~/.kube/config. Reading the file yourself would work
// on your machine and nowhere else.
//
// The namespace has its own precedence, and the middle step is the one people
// forget: the --namespace flag, then whatever the *context* carries, then
// "default". Skipping the context's namespace is why a tool works for its
// author, whose context is unset, and surprises everyone else.
func resolve(kctx, namespace string) (*rest.Config, string, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{CurrentContext: kctx}
	if namespace != "" {
		overrides.Context.Namespace = namespace
	}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides)

	cfg, err := cc.ClientConfig()
	if err != nil {
		return nil, "", err
	}
	// Namespace() applies that precedence for us and reports "default" when
	// nothing else set one.
	ns, _, err := cc.Namespace()
	if err != nil {
		return nil, "", err
	}
	return cfg, ns, nil
}

// printVersion reports what the API server is running.
//
// This goes through the discovery client rather than a typed one: /version
// belongs to no group and has no Kind, so there is nothing typed to ask.
func printVersion(cfg *rest.Config) error {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return err
	}
	v, err := dc.ServerVersion()
	if err != nil {
		return err
	}
	fmt.Printf("Server Version: %s\n", v.GitVersion)
	return nil
}

func get(cfg *rest.Config, ns, resource, name, output, selector, fieldSelector string, allNS, watch bool) error {
	gvr, err := mapResource(cfg, resource)
	if err != nil {
		return err
	}
	list, err := fetch(cfg, gvr, ns, name, selector, fieldSelector)
	if err != nil {
		return err
	}
	if output == "json" || output == "yaml" {
		data, err := json.MarshalIndent(list, "", "    ")
		if err != nil {
			return err
		}
		if output == "json" {
			fmt.Println(string(data))
			return nil
		}
		// The object is identical; only the serializer changes. This converts
		// through JSON on purpose, so the API types' json struct tags are the
		// ones honoured — marshalling straight to YAML with a generic library
		// would emit Go field names and lose the API's shape.
		y, err := yaml.JSONToYAML(data)
		if err != nil {
			return err
		}
		fmt.Print(string(y))
		return nil
	}
	if output != "" && output != "wide" {
		return fmt.Errorf("unknown output format %q", output)
	}

	// tabwriter does the column alignment kubectl's printer does: write the
	// cells separated by tabs and let it choose a width that fits the widest
	// one in each column. Computing widths by hand works until a name is long.
	wide := output == "wide"

	// The API returns items in whatever order etcd hands them over, which is
	// stable enough to look sorted and not stable enough to rely on. Sorting
	// by name here is what makes two runs of the same command diffable, and
	// what makes a script that greps line 3 mean anything.
	sort.Slice(list.Items, func(i, j int) bool {
		if a, b := list.Items[i].GetNamespace(), list.Items[j].GetNamespace(); a != b {
			return a < b
		}
		return list.Items[i].GetName() < list.Items[j].GetName()
	})

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	var header []string
	if allNS {
		// Rows can now come from anywhere, so the namespace stops being
		// context and becomes data.
		header = append(header, "NAMESPACE")
	}
	header = append(header, "NAME", "AGE")
	if wide {
		header = append(header, wideHeaders(gvr)...)
	}
	fmt.Fprintln(w, strings.Join(header, "\t"))

	for _, o := range list.Items {
		var row []string
		if allNS {
			row = append(row, o.GetNamespace())
		}
		row = append(row, o.GetName(), age(o.GetCreationTimestamp().Time))
		if wide {
			row = append(row, wideValues(gvr, o)...)
		}
		fmt.Fprintln(w, strings.Join(row, "\t"))
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if !watch {
		return nil
	}
	return streamChanges(cfg, gvr, ns, selector, fieldSelector, list.GetResourceVersion(), allNS, wide)
}

// streamChanges prints objects as they change, starting exactly where the list
// ended.
//
// The resourceVersion of the *list* is the seam. Starting a watch without it
// would replay from wherever the server felt like, showing objects already
// printed or missing ones changed in the gap; passing it means the stream
// begins at the instant the snapshot was taken. List-then-watch from the
// list's own version is the pattern every informer is built on.
func streamChanges(cfg *rest.Config, gvr schema.GroupVersionResource, ns, selector, fieldSelector, since string, allNS, wide bool) error {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	w, err := dyn.Resource(gvr).Namespace(ns).Watch(context.Background(), metav1.ListOptions{
		LabelSelector:   selector,
		FieldSelector:   fieldSelector,
		ResourceVersion: since,
	})
	if err != nil {
		return err
	}
	defer w.Stop()

	out := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	for event := range w.ResultChan() {
		obj, ok := event.Object.(*unstructured.Unstructured)
		if !ok {
			// An Error event carries a Status, not the object — a watch that
			// assumes otherwise panics on the first expiry.
			continue
		}
		row := []string{}
		if allNS {
			row = append(row, obj.GetNamespace())
		}
		row = append(row, obj.GetName(), age(obj.GetCreationTimestamp().Time))
		if wide {
			row = append(row, wideValues(gvr, *obj)...)
		}
		fmt.Fprintln(out, strings.Join(row, "\t"))
		if err := out.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// create sends a manifest to the server.
//
// Three things have to happen before the object can be addressed, and the
// order matters. The YAML is decoded to an unstructured object, which carries
// its own apiVersion and kind — that is what makes a manifest self-describing
// and why the file needs no flags saying what it holds. Those two are then
// mapped to a resource, since the API is addressed by resource rather than by
// kind. Finally the namespace: what the manifest says wins, and what the
// context selected fills the gap, because a manifest that names a namespace
// means it.
func create(cfg *rest.Config, ns, filename string) error {
	if filename == "" {
		return fmt.Errorf("create: which file? (-f)")
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	obj := &unstructured.Unstructured{}
	if err := yaml.Unmarshal(data, &obj.Object); err != nil {
		return fmt.Errorf("parsing %s: %w", filename, err)
	}
	gvk := obj.GroupVersionKind()
	if gvk.Kind == "" {
		return fmt.Errorf("%s has no kind", filename)
	}

	mapper, err := newMapper(cfg)
	if err != nil {
		return err
	}
	mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return fmt.Errorf("the server has no %s: %w", gvk.Kind, err)
	}

	target := ns
	if in := obj.GetNamespace(); in != "" {
		target = in
	}
	obj.SetNamespace(target)

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	created, err := dyn.Resource(mapping.Resource).Namespace(target).
		Create(context.Background(), obj, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	fmt.Printf("%s \"%s\" created\n", mapping.Resource.Resource, created.GetName())
	return nil
}

// apply sends the manifest as a declaration of intent rather than an
// instruction.
//
// The difference from create is not "create if missing". An apply says "these
// fields should look like this, and I am the one saying so" — the server
// records the field manager against every field the request set, and a later
// apply that omits a field it used to set means "I no longer manage this",
// so the server removes it. That is why apply can converge state that a
// create-or-update loop cannot: it knows the difference between a field
// someone else set and a field you removed.
//
// The patch type carries the whole mechanism; the body is the manifest itself,
// unchanged.
func apply(cfg *rest.Config, ns, filename string, force bool) error {
	if filename == "" {
		return fmt.Errorf("apply: which file? (-f)")
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	obj := &unstructured.Unstructured{}
	if err := yaml.Unmarshal(data, &obj.Object); err != nil {
		return fmt.Errorf("parsing %s: %w", filename, err)
	}
	gvk := obj.GroupVersionKind()
	if gvk.Kind == "" {
		return fmt.Errorf("%s has no kind", filename)
	}

	mapper, err := newMapper(cfg)
	if err != nil {
		return err
	}
	mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return fmt.Errorf("the server has no %s: %w", gvk.Kind, err)
	}

	target := ns
	if in := obj.GetNamespace(); in != "" {
		target = in
	}
	obj.SetNamespace(target)

	body, err := json.Marshal(obj.Object)
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	applied, err := dyn.Resource(mapping.Resource).Namespace(target).Patch(
		context.Background(), obj.GetName(), types.ApplyPatchType, body,
		// The field manager is an identity, not a label: it is what the server
		// records ownership against, so two tools sharing a name share fields
		// and neither can tell.
		metav1.PatchOptions{FieldManager: "byok8s", Force: &force})
	if err != nil {
		// A conflict is the server saying another manager owns a field this
		// apply is setting. It is a question, not a fault: forcing is
		// sometimes right and sometimes stamps on a controller that will
		// immediately set it back, so the choice belongs to whoever ran the
		// command rather than to this function.
		if apierrors.IsConflict(err) {
			return fmt.Errorf("%w\n\nanother field manager owns a field this manifest sets."+
				"\nre-run with --force-conflicts to take ownership", err)
		}
		return err
	}
	fmt.Printf("%s \"%s\" applied\n", mapping.Resource.Resource, applied.GetName())
	return nil
}

// describe renders one object as labelled lines.
//
// It answers a different question from get. A table compares many objects, so
// every row has to fit one line and the columns are chosen in advance;
// describe explains one object, so it can spend vertical space and show the
// fields that only matter when you are looking at exactly this thing.
//
// Real kubectl has a describer per kind, hand-written, because "the useful
// fields" is a judgement rather than a rule. This one covers pods and prints
// the shared metadata for anything else.
func describe(cfg *rest.Config, ns, resource, name string) error {
	if name == "" {
		return fmt.Errorf("describe: which %s?", resource)
	}
	gvr, err := mapResource(cfg, resource)
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	obj, err := dyn.Resource(gvr).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	fmt.Fprintf(w, "Name:\t%s\n", obj.GetName())
	fmt.Fprintf(w, "Namespace:\t%s\n", obj.GetNamespace())
	if labels := obj.GetLabels(); len(labels) > 0 {
		fmt.Fprintf(w, "Labels:\t%s\n", joinMap(labels))
	} else {
		fmt.Fprintf(w, "Labels:\t<none>\n")
	}
	if gvr.Group == "" && gvr.Resource == "pods" {
		node, _, _ := unstructured.NestedString(obj.Object, "spec", "nodeName")
		phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
		ip, _, _ := unstructured.NestedString(obj.Object, "status", "podIP")
		fmt.Fprintf(w, "Node:\t%s\n", orNone(node))
		fmt.Fprintf(w, "Status:\t%s\n", orNone(phase))
		fmt.Fprintf(w, "IP:\t%s\n", orNone(ip))
	}
	return w.Flush()
}

func joinMap(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, k+"="+m[k])
	}
	return strings.Join(pairs, ",")
}

// scale sets the replica count through /scale.
//
// The subresource is a separate endpoint on the same object, and it exists so
// that "may change the replica count" can be granted without "may change the
// pod template" — RBAC is written against resources, and a subresource is
// addressable in its own right. It also means an autoscaler and a deploy
// pipeline write to different endpoints and stop fighting over the parent.
//
// Scale has a shared shape across Deployments, StatefulSets and anything else
// that implements it, so this works without knowing which kind it is.
func scale(cfg *rest.Config, ns, resource, name string, replicas int) error {
	if name == "" || replicas < 0 {
		return fmt.Errorf("scale: need a name and --replicas")
	}
	gvr, err := mapResource(cfg, resource)
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	// The patch goes to the scale subresource, so the body is a Scale object
	// rather than a Deployment — spec.replicas here is Scale's field, not the
	// Deployment's, even though setting it moves the same number.
	body := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
	if _, err := dyn.Resource(gvr).Namespace(ns).Patch(
		context.Background(), name, types.MergePatchType, []byte(body),
		metav1.PatchOptions{}, "scale"); err != nil {
		return err
	}
	fmt.Printf("%s \"%s\" scaled\n", gvr.Resource, name)
	return nil
}

// patch changes named fields and leaves the rest alone.
//
// A merge patch is the smallest of the write verbs: the body names only what
// changes, so nothing has to be read first and there is no resourceVersion to
// conflict on. That is its appeal and its danger — two patches to different
// fields never conflict, which is what you want, and a patch that races a
// controller silently wins, which is not always.
//
// The type matters. A JSON merge patch replaces whole values, so patching one
// element of a list replaces the list; a strategic merge patch knows the
// API's own merge keys and can add a container to a pod without dropping the
// others. Strategic only works on built-in types, because the merge keys come
// from their Go struct tags — which is why a CRD accepts merge and not
// strategic.
func patch(cfg *rest.Config, ns, resource, name, body string) error {
	if name == "" || body == "" {
		return fmt.Errorf("patch: need a name and a patch (-p)")
	}
	gvr, err := mapResource(cfg, resource)
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	patched, err := dyn.Resource(gvr).Namespace(ns).Patch(
		context.Background(), name, types.MergePatchType, []byte(body), metav1.PatchOptions{})
	if err != nil {
		return err
	}
	fmt.Printf("%s \"%s\" patched\n", gvr.Resource, patched.GetName())
	return nil
}

// deleteObject removes one object.
//
// Delete is asynchronous: the call returns once the server has accepted the
// request and marked the object, not once it is gone. For a pod with a grace
// period the object stays visible, now carrying a deletionTimestamp, until its
// containers stop. Reporting success here means "accepted", which is what
// kubectl means too.
func deleteObject(cfg *rest.Config, ns, resource, name string) error {
	if name == "" {
		return fmt.Errorf("delete: which %s?", resource)
	}
	gvr, err := mapResource(cfg, resource)
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	if err := dyn.Resource(gvr).Namespace(ns).Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		return err
	}
	fmt.Printf("%s \"%s\" deleted\n", gvr.Resource, name)
	return nil
}

// wideHeaders and wideValues are the extra columns -o wide adds.
//
// They are per-resource by nature: a Pod has a node and an IP, a Service has a
// cluster IP and ports, and a CRD has whatever its author declared. Real
// kubectl asks the server for these — the API can return a Table with the
// columns already chosen, which is how it prints resources it has never seen.
// Deciding here keeps the program readable, at the cost of only knowing about
// the resources it has been taught.
func wideHeaders(gvr schema.GroupVersionResource) []string {
	if gvr.Group == "" && gvr.Resource == "pods" {
		return []string{"NODE", "IP"}
	}
	return nil
}

func wideValues(gvr schema.GroupVersionResource, o unstructured.Unstructured) []string {
	if gvr.Group != "" || gvr.Resource != "pods" {
		return nil
	}
	// Nested lookups report "found" separately from "wrong type", and an
	// unscheduled pod has neither field — so absent becomes <none> rather
	// than an empty cell that misaligns the row.
	node, _, _ := unstructured.NestedString(o.Object, "spec", "nodeName")
	ip, _, _ := unstructured.NestedString(o.Object, "status", "podIP")
	return []string{orNone(node), orNone(ip)}
}

func orNone(s string) string {
	if s == "" {
		return "<none>"
	}
	return s
}

// mapResource turns whatever the user typed into the resource the API is
// addressed by.
//
// "po", "pods", "pod" and "Pod" are four spellings of one thing, and only the
// server knows the mapping — shortnames are declared by whoever defined the
// resource, including a CRD added at runtime. A hardcoded table would work for
// core types and break on the first custom one, which is the whole reason the
// RESTMapper exists.
func mapResource(cfg *rest.Config, resource string) (schema.GroupVersionResource, error) {
	if resource == "" {
		return schema.GroupVersionResource{}, fmt.Errorf("get: which resource?")
	}
	mapper, err := newMapper(cfg)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}

	// ParseResourceArg understands "pods", "pods.v1.", and friends; falling
	// back to a bare resource-or-kind covers "Pod" and "po".
	fullySpecified, gr := schema.ParseResourceArg(strings.ToLower(resource))
	if fullySpecified != nil {
		if m, err := mapper.ResourceFor(*fullySpecified); err == nil {
			return m, nil
		}
	}
	m, err := mapper.ResourceFor(gr.WithVersion(""))
	if err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("the server has no resource called %q", resource)
	}
	return m, nil
}

// newMapper builds the RESTMapper both the reader and the writer use.
//
// The plain mapper knows resources and kinds but not shortnames: "po" is a
// client-side convenience expanded from the shortNames discovery reports, so
// it needs the shortcut expander wrapped around it. Without this "pods" works
// and "po" does not, which is a confusing way to fail.
func newMapper(cfg *rest.Config) (meta.RESTMapper, error) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, err
	}
	groups, err := restmapper.GetAPIGroupResources(dc)
	if err != nil {
		return nil, err
	}
	return restmapper.NewShortcutExpander(
		restmapper.NewDiscoveryRESTMapper(groups), dc, func(string) {}), nil
}

// apiResources lists everything this server serves.
//
// This is the request that makes a client general. Nothing here knows what a
// Pod is: the server reports its groups, their versions, and the resources in
// each — including CRDs installed five minutes ago, which is why kubectl can
// work with resources that did not exist when it was compiled.
func apiResources(cfg *rest.Config) error {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return err
	}
	// Preferred: one version per group, the one the server would pick.
	groups, err := dc.ServerPreferredResources()
	if err != nil {
		// Partial failures are normal — an aggregated API whose backend is
		// down fails its own group and no other. Reporting what did answer
		// beats failing the whole command.
		if len(groups) == 0 {
			return err
		}
		fmt.Fprintln(os.Stderr, "warning: some groups did not respond:", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "NAME\tSHORTNAMES\tAPIVERSION\tNAMESPACED\tKIND")
	for _, list := range groups {
		if list == nil {
			continue
		}
		for _, r := range list.APIResources {
			// Subresources such as pods/log are addressed through their parent
			// and are not listed in their own right.
			if strings.Contains(r.Name, "/") {
				continue
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%t\t%s\n",
				r.Name, strings.Join(r.ShortNames, ","), list.GroupVersion, r.Namespaced, r.Kind)
		}
	}
	return w.Flush()
}

// fetch returns the objects to print, whatever they are.
//
// This is the moment the program stops being a pod client. The dynamic client
// speaks in GroupVersionResource and unstructured maps rather than Go types,
// so one code path serves pods, ConfigMaps and a CRD nobody had written when
// this was compiled. A typed client is still the better choice when the type
// is known at compile time — fields instead of map lookups — but a tool like
// this one does not know.
//
// A Get for a missing object returns NotFound rather than an empty result, and
// letting it surface is right: "there is no such pod" and "there are no pods"
// are different answers, and a script that cannot tell them apart deletes the
// wrong thing.
func fetch(cfg *rest.Config, gvr schema.GroupVersionResource, ns, name, selector, fieldSelector string) (*unstructured.UnstructuredList, error) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	ri := dyn.Resource(gvr).Namespace(ns)
	if name == "" {
		// The selector goes to the server. Listing everything and filtering
		// here would give the same answer on a toy cluster and fall over on a
		// real one — the point of a selector is that the rows you do not want
		// are never sent.
		// Labels and fields are different axes. Labels are yours to invent;
		// fields are the object's own, and only a few are indexed —
		// metadata.name and status.phase are, spec.nodeName is on some
		// versions, and asking for anything else is rejected rather than
		// silently scanned. That refusal is deliberate: an unindexed filter
		// would look fast and cost the apiserver a full scan.
		return ri.List(context.Background(), metav1.ListOptions{
			LabelSelector: selector,
			FieldSelector: fieldSelector,
		})
	}
	obj, err := ri.Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return &unstructured.UnstructuredList{Items: []unstructured.Unstructured{*obj}}, nil
}

// age renders a duration the way kubectl does: one unit, the largest that
// fits, no decimals. "2d" is more useful at a glance than "51h13m6s", and it
// keeps the column narrow enough to scan a hundred rows.
func age(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}
