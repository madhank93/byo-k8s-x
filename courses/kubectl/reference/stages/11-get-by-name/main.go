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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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
	case "get":
		if *allNS {
			// The empty namespace is not a fallback here, it is the request:
			// a list whose path carries no namespace is a cluster-wide list.
			ns = ""
		}
		err = get(cfg, ns, arg(args, 1), arg(args, 2), *output, *allNS)
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

func get(cfg *rest.Config, ns, resource, name, output string, allNS bool) error {
	if resource != "pods" && resource != "pod" && resource != "po" {
		return fmt.Errorf("get: unknown resource %q", resource)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	list, err := fetch(cs, ns, name)
	if err != nil {
		return err
	}
	if output == "json" || output == "yaml" {
		// The list is an API object in its own right, not a bare array — it
		// carries its own Kind, which is what lets this output be fed back to
		// the server later. The server does not send those fields on a list
		// read, so they are set here.
		list.Kind, list.APIVersion = "PodList", "v1"
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
	if output != "" {
		return fmt.Errorf("unknown output format %q", output)
	}

	// tabwriter does the column alignment kubectl's printer does: write the
	// cells separated by tabs and let it choose a width that fits the widest
	// one in each column. Computing widths by hand works until a name is long.
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	if allNS {
		// Rows can now come from anywhere, so the namespace stops being
		// context and becomes data.
		fmt.Fprintln(w, "NAMESPACE\tNAME\tAGE")
	} else {
		fmt.Fprintln(w, "NAME\tAGE")
	}
	for _, p := range list.Items {
		if allNS {
			fmt.Fprintf(w, "%s\t%s\t%s\n", p.Namespace, p.Name, age(p.CreationTimestamp.Time))
			continue
		}
		fmt.Fprintf(w, "%s\t%s\n", p.Name, age(p.CreationTimestamp.Time))
	}
	return w.Flush()
}

// fetch returns the pods to print: one when a name was given, all otherwise.
//
// A Get for a missing object returns a NotFound error rather than an empty
// result, and letting it surface is the right behaviour — "there is no such
// pod" and "there are no pods" are different answers, and a script that
// cannot tell them apart is a script that deletes the wrong thing.
//
// Wrapping the single object in a list keeps one printing path for both cases.
func fetch(cs *kubernetes.Clientset, ns, name string) (*corev1.PodList, error) {
	if name == "" {
		return cs.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{})
	}
	pod, err := cs.CoreV1().Pods(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return &corev1.PodList{Items: []corev1.Pod{*pod}}, nil
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
