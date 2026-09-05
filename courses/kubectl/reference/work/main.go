// Your kubectl.
//
// Stage 1: print the API server, found through the standard loading rules.
// Stage 2: let --context choose which cluster that is.
// Stage 3: `version` asks the server what it is.
package main

import (
	"flag"
	"fmt"
	"os"

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	context := flag.String("context", "", "the kubeconfig context to use")
	flag.Parse()

	cfg, err := restConfig(*context)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loading kubeconfig:", err)
		os.Exit(1)
	}

	switch flag.Arg(0) {
	case "version":
		if err := printVersion(cfg); err != nil {
			fmt.Fprintln(os.Stderr, "asking the server its version:", err)
			os.Exit(1)
		}
	default:
		fmt.Println(cfg.Host)
	}
}

// restConfig resolves the kubeconfig the way kubectl does.
//
// The loading rules are the whole point: $KUBECONFIG first (colon separated,
// merged in order), then ~/.kube/config. Reading the file yourself would work
// on your machine and nowhere else.
//
// The overrides are the second half. An empty CurrentContext means "whatever
// the file says"; a named one must exist, and asking for a context that does
// not is an error rather than a quiet fallback — silently talking to the wrong
// cluster is the worst failure this program could have.
func restConfig(context string) (*rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{CurrentContext: context}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
}

// printVersion reports what the API server is running.
//
// This is the first request the program actually makes, and it goes through
// the discovery client rather than a typed one: /version belongs to no group
// and has no Kind, so there is nothing typed to ask. Discovery is also what
// the next stages use to learn which resources exist at all.
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
