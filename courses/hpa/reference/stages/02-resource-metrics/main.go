// A metrics server, and an autoscaler that reads it.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: app summary|scrape <node>")
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
	case "scrape":
		a, err := scrape(ctx, cs, os.Args[2])
		check(err)
		b, err := later(ctx, cs, os.Args[2], a)
		check(err)
		for _, k := range sortedKeys(b) {
			u := rate(a[k], b[k])
			fmt.Printf("%s %dm %d\n", k, u.cpuMilli, u.mem)
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

// sample is one container's line in the kubelet's /metrics/resource: a
// cumulative CPU counter with the time the kubelet read it, and memory.
type sample struct {
	cpuSeconds float64
	cpuAt      int64 // milliseconds since the epoch
	mem        float64
}

type usage struct{ cpuMilli, mem int64 }

// scrape reads one node's /metrics/resource, keyed namespace/pod/container.
func scrape(ctx context.Context, cs *kubernetes.Clientset, node string) (map[string]sample, error) {
	raw, err := cs.CoreV1().RESTClient().Get().AbsPath("/api/v1/nodes", node, "proxy/metrics/resource").DoRaw(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]sample{}
	for _, line := range strings.Split(string(raw), "\n") {
		name, labels, val, at, ok := parseLine(line)
		if !ok || (name != "container_cpu_usage_seconds_total" && name != "container_memory_working_set_bytes") {
			continue
		}
		key := labels["namespace"] + "/" + labels["pod"] + "/" + labels["container"]
		s := out[key]
		if name == "container_cpu_usage_seconds_total" {
			s.cpuSeconds, s.cpuAt = val, at
		} else {
			s.mem = val
		}
		out[key] = s
	}
	return out, nil
}

// parseLine reads one Prometheus text sample: name{labels} value timestamp.
// Values may be written in exponent form, so they are floats.
func parseLine(line string) (name string, labels map[string]string, val float64, at int64, ok bool) {
	open, close := strings.IndexByte(line, '{'), strings.LastIndexByte(line, '}')
	if strings.HasPrefix(line, "#") || open < 0 || close < open {
		return "", nil, 0, 0, false
	}
	labels = map[string]string{}
	for _, kv := range strings.Split(line[open+1:close], ",") {
		k, v, found := strings.Cut(kv, "=")
		if found {
			labels[k] = strings.Trim(v, `"`)
		}
	}
	f := strings.Fields(line[close+1:])
	if len(f) < 2 {
		return "", nil, 0, 0, false
	}
	val, err1 := strconv.ParseFloat(f[0], 64)
	at, err2 := strconv.ParseInt(f[1], 10, 64)
	return line[:open], labels, val, at, err1 == nil && err2 == nil
}

// later scrapes again until the kubelet has taken a fresh sample of every
// container seen in first. The kubelet samples on its own clock, every ten
// seconds or so, and two reads of the same sample have no rate between them.
func later(ctx context.Context, cs *kubernetes.Clientset, node string, first map[string]sample) (map[string]sample, error) {
	deadline := time.Now().Add(45 * time.Second)
	for {
		time.Sleep(2 * time.Second)
		next, err := scrape(ctx, cs, node)
		if err != nil {
			return nil, err
		}
		fresh := true
		for k, a := range first {
			if b, ok := next[k]; ok && b.cpuAt <= a.cpuAt {
				fresh = false
			}
		}
		if fresh || time.Now().After(deadline) {
			for k := range next {
				if a, ok := first[k]; !ok || next[k].cpuAt <= a.cpuAt {
					delete(next, k)
				}
			}
			return next, nil
		}
	}
}

// rate is the CPU used between two samples, over the time between them as
// the kubelet measured it, not as this program waited.
func rate(a, b sample) usage {
	cores := (b.cpuSeconds - a.cpuSeconds) / (float64(b.cpuAt-a.cpuAt) / 1000)
	return usage{cpuMilli: int64(cores * 1000), mem: int64(b.mem)}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
