// Your kubectl.
//
// Stage 1: print the API server, found through the standard loading rules.
// Stage 2: let --context choose which cluster that is.
// Stage 3: `version` asks the server what it is.
// Stage 4: `get pods` lists the namespace the context names.
// Stage 5: -n overrides that namespace.
// Stage 6: print an aligned table.
// Stage 7: humanise the age.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	fs := flag.NewFlagSet("byok8s", flag.ContinueOnError)
	kctx := fs.String("context", "", "the kubeconfig context to use")
	namespace := fs.String("namespace", "", "the namespace to work in")
	fs.StringVar(namespace, "n", "", "the namespace to work in (shorthand)")

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
		err = get(cfg, ns, arg(args, 1))
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

func get(cfg *rest.Config, ns, resource string) error {
	if resource != "pods" && resource != "pod" && resource != "po" {
		return fmt.Errorf("get: unknown resource %q", resource)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	list, err := cs.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	// tabwriter does the column alignment kubectl's printer does: write the
	// cells separated by tabs and let it choose a width that fits the widest
	// one in each column. Computing widths by hand works until a name is long.
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "NAME\tAGE")
	for _, p := range list.Items {
		fmt.Fprintf(w, "%s\t%s\n", p.Name, age(p.CreationTimestamp.Time))
	}
	return w.Flush()
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
