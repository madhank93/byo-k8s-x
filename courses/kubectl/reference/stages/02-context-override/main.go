// Your kubectl.
//
// Stage 1: print the API server, found through the standard loading rules.
// Stage 2: let --context choose which cluster that is.
package main

import (
	"flag"
	"fmt"
	"os"

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

	fmt.Println(cfg.Host)
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
