// A metrics server, and an autoscaler that reads it.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: app summary <node>")
		os.Exit(2)
	}
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{})
	cfg, err := loader.ClientConfig()
	check(err)
	cs, err := kubernetes.NewForConfig(cfg)
	check(err)
	ctx := context.Background()

	switch os.Args[1] {
	case "summary":
		s, err := fetchSummary(ctx, cs, os.Args[2])
		check(err)
		for _, p := range s.Pods {
			fmt.Printf("%s/%s %dm %d\n", p.PodRef.Namespace, p.PodRef.Name, p.CPU.UsageNanoCores/1e6, p.Memory.WorkingSetBytes)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}

// summary is the slice of the kubelet's Summary API this program reads.
type summary struct {
	Pods []struct {
		PodRef struct{ Name, Namespace string } `json:"podRef"`
		CPU    struct {
			UsageNanoCores uint64 `json:"usageNanoCores"`
		} `json:"cpu"`
		Memory struct {
			WorkingSetBytes uint64 `json:"workingSetBytes"`
		} `json:"memory"`
	} `json:"pods"`
}

// fetchSummary reaches the kubelet through the API server's node proxy, so
// the program needs no network path to the node itself.
func fetchSummary(ctx context.Context, cs *kubernetes.Clientset, node string) (*summary, error) {
	raw, err := cs.CoreV1().RESTClient().Get().AbsPath("/api/v1/nodes", node, "proxy/stats/summary").DoRaw(ctx)
	if err != nil {
		return nil, err
	}
	var s summary
	return &s, json.Unmarshal(raw, &s)
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
