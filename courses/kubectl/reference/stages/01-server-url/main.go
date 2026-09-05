// Your kubectl.
//
// Stage 1: print the API server this program would talk to, found the way
// kubectl finds it — the standard loading rules, not a hardcoded path.
package main

import (
	"fmt"
	"os"

	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	// The loading rules are the whole point: $KUBECONFIG first (colon
	// separated, merged in order), then ~/.kube/config. Reading the file
	// yourself would work on your machine and nowhere else.
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	config := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})

	restConfig, err := config.ClientConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "loading kubeconfig:", err)
		os.Exit(1)
	}

	fmt.Println(restConfig.Host)
}
