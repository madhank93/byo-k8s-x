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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	case "get":
		if *allNS {
			// The empty namespace is not a fallback here, it is the request:
			// a list whose path carries no namespace is a cluster-wide list.
			ns = ""
		}
		err = get(cfg, ns, arg(args, 1), arg(args, 2), *output, *selector, *allNS)
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

func get(cfg *rest.Config, ns, resource, name, output, selector string, allNS bool) error {
	gvr, err := mapResource(cfg, resource)
	if err != nil {
		return err
	}
	list, err := fetch(cfg, gvr, ns, name, selector)
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
	return w.Flush()
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
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	groups, err := restmapper.GetAPIGroupResources(dc)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	// The plain mapper knows resources and kinds but not shortnames: "po" is
	// a client-side convenience expanded from the shortNames discovery
	// reports, so it needs the shortcut expander wrapped around it. Without
	// this "pods" works and "po" does not, which is a confusing way to fail.
	mapper := restmapper.NewShortcutExpander(
		restmapper.NewDiscoveryRESTMapper(groups), dc, func(string) {})

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
func fetch(cfg *rest.Config, gvr schema.GroupVersionResource, ns, name, selector string) (*unstructured.UnstructuredList, error) {
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
		return ri.List(context.Background(), metav1.ListOptions{LabelSelector: selector})
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
