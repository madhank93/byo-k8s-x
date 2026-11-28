// A metrics server, and an autoscaler that reads it.
package main

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	certutil "k8s.io/client-go/util/cert"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: app summary|scrape <node> | app pods | app serve --port P --external-host H")
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
		got, err := collect(ctx, cs, []string{os.Args[2]})
		check(err)
		for _, k := range sortedKeys(got) {
			fmt.Printf("%s %dm %d\n", k, got[k].cpuMilli, got[k].mem)
		}
	case "pods":
		ns, _, err := loader.Namespace()
		check(err)
		nodes, err := nodeNames(ctx, cs)
		check(err)
		got, err := collect(ctx, cs, nodes)
		check(err)
		pods := byPod(got)
		for _, k := range sortedKeys(pods) {
			if pod, ok := strings.CutPrefix(k, ns+"/"); ok {
				fmt.Printf("%s %dm %d\n", pod, pods[k].cpuMilli, pods[k].mem)
			}
		}
	case "serve":
		fs := flag.NewFlagSet("serve", flag.ExitOnError)
		port := fs.Int("port", 6443, "port to listen on")
		host := fs.String("external-host", "host.docker.internal", "the name the cluster reaches this machine by")
		fs.Parse(os.Args[2:])
		ns, _, err := loader.Namespace()
		check(err)
		check(serve(cs, cfg, ns, *port, *host))
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

// usage is a rate over a window: at is when the later sample was taken.
type usage struct {
	cpuMilli, mem int64
	at            time.Time
	window        time.Duration
}

// nodeKey is where a scrape keeps the node's own sample. A colon cannot
// appear in a namespace, so it never collides with a container's key.
const nodeKey = ":node"

// scrape reads one node's /metrics/resource, keyed namespace/pod/container,
// and the node itself under nodeKey.
func scrape(ctx context.Context, cs *kubernetes.Clientset, node string) (map[string]sample, error) {
	raw, err := cs.CoreV1().RESTClient().Get().AbsPath("/api/v1/nodes", node, "proxy/metrics/resource").DoRaw(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]sample{}
	for _, line := range strings.Split(string(raw), "\n") {
		name, labels, val, at, ok := parseLine(line)
		if !ok {
			continue
		}
		key := labels["namespace"] + "/" + labels["pod"] + "/" + labels["container"]
		switch name {
		case "container_cpu_usage_seconds_total", "container_memory_working_set_bytes":
		case "node_cpu_usage_seconds_total", "node_memory_working_set_bytes":
			key = nodeKey
		default:
			continue
		}
		s := out[key]
		if strings.HasSuffix(name, "_cpu_usage_seconds_total") {
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
	if strings.HasPrefix(line, "#") {
		return "", nil, 0, 0, false
	}
	open, close := strings.IndexByte(line, '{'), strings.LastIndexByte(line, '}')
	if open < 0 {
		// A sample with no labels has no braces either.
		line = strings.Replace(line, " ", "{} ", 1)
		open, close = strings.IndexByte(line, '{'), strings.IndexByte(line, '}')
	}
	if open <= 0 || close < open {
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
	return usage{cpuMilli: int64(cores * 1000), mem: int64(b.mem),
		at: time.UnixMilli(b.cpuAt), window: time.Duration(b.cpuAt-a.cpuAt) * time.Millisecond}
}

// collect samples every node twice, concurrently, and returns each
// container's usage keyed namespace/pod/container.
func collect(ctx context.Context, cs *kubernetes.Clientset, nodes []string) (map[string]usage, error) {
	var mu sync.Mutex
	var firstErr error
	out := map[string]usage{}
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a, err := scrape(ctx, cs, node)
			var b map[string]sample
			if err == nil {
				b, err = later(ctx, cs, node, a)
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				firstErr = fmt.Errorf("node %s: %w", node, err)
				return
			}
			for k := range b {
				key := k
				if k == nodeKey {
					key = nodeKey + "/" + node
				}
				out[key] = rate(a[k], b[k])
			}
		}()
	}
	wg.Wait()
	return out, firstErr
}

// byPod sums containers into their pod, keyed namespace/pod.
func byPod(containers map[string]usage) map[string]usage {
	pods := map[string]usage{}
	for k, u := range containers {
		pod := k[:strings.LastIndexByte(k, '/')]
		p := pods[pod]
		p.cpuMilli += u.cpuMilli
		p.mem += u.mem
		pods[pod] = p
	}
	return pods
}

const (
	group       = "metrics.k8s.io"
	version     = "v1beta1"
	serviceName = "byok8s-metrics"
)

var apiServiceName = version + "." + group

// serve answers metrics.k8s.io over TLS and registers it with the API
// server's aggregator for as long as it runs.
func serve(cs *kubernetes.Clientset, cfg *rest.Config, ns string, port int, host string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// The aggregator dials the Service and checks the certificate against
	// the Service's DNS name, so that is the name the certificate carries.
	certPEM, keyPEM, err := certutil.GenerateSelfSignedCertKey(host, nil, []string{serviceName + "." + ns + ".svc"})
	if err != nil {
		return err
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return err
	}
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		return err
	}
	st := &store{}
	go st.run(ctx, cs)
	srv := &http.Server{Handler: routes(st), TLSConfig: &tls.Config{Certificates: []tls.Certificate{pair}}}
	go srv.ServeTLS(l, "", "")

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	if err := register(ctx, cs, dyn, ns, port, host, certPEM); err != nil {
		unregister(cs, dyn, ns)
		return err
	}
	fmt.Printf("serving %s on :%d\n", apiServiceName, port)
	<-ctx.Done()

	// An APIService with nothing behind it fails discovery for every client
	// on the cluster, so it goes before the server does.
	unregister(cs, dyn, ns)
	return srv.Close()
}

var apiServicesGVR = schema.GroupVersionResource{Group: "apiregistration.k8s.io", Version: "v1", Resource: "apiservices"}

// register points the aggregator at this process: an ExternalName Service
// naming this machine, and an APIService trusting this certificate.
func register(ctx context.Context, cs *kubernetes.Clientset, dyn dynamic.Interface, ns string, port int, host string, ca []byte) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: serviceName},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeExternalName, ExternalName: host,
			Ports: []corev1.ServicePort{{Port: int32(port)}}},
	}
	cs.CoreV1().Services(ns).Delete(ctx, serviceName, metav1.DeleteOptions{})
	if _, err := cs.CoreV1().Services(ns).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create service: %w", err)
	}
	as := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiregistration.k8s.io/v1", "kind": "APIService",
		"metadata": map[string]any{"name": apiServiceName},
		"spec": map[string]any{
			"group": group, "version": version, "groupPriorityMinimum": int64(100), "versionPriority": int64(100),
			"caBundle": base64.StdEncoding.EncodeToString(ca),
			"service":  map[string]any{"namespace": ns, "name": serviceName, "port": int64(port)},
		},
	}}
	_, err := dyn.Resource(apiServicesGVR).Create(ctx, as, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		var old *unstructured.Unstructured
		if old, err = dyn.Resource(apiServicesGVR).Get(ctx, apiServiceName, metav1.GetOptions{}); err == nil {
			as.SetResourceVersion(old.GetResourceVersion())
			_, err = dyn.Resource(apiServicesGVR).Update(ctx, as, metav1.UpdateOptions{})
		}
	}
	if err != nil {
		return fmt.Errorf("register %s: %w", apiServiceName, err)
	}
	return nil
}

func unregister(cs *kubernetes.Clientset, dyn dynamic.Interface, ns string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := dyn.Resource(apiServicesGVR).Delete(ctx, apiServiceName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		fmt.Fprintln(os.Stderr, "unregister:", err)
	}
	cs.CoreV1().Services(ns).Delete(ctx, serviceName, metav1.DeleteOptions{})
}

// store holds the latest rate for every container in the cluster, keyed
// namespace/pod/container. Nothing is kept beyond the last round: metrics are
// a view of now, not a history.
type store struct {
	mu         sync.Mutex
	containers map[string]usage
}

// run scrapes every node, round after round, until ctx ends. A node that
// fails a round keeps nothing for that round rather than stopping the rest.
func (st *store) run(ctx context.Context, cs *kubernetes.Clientset) {
	for ctx.Err() == nil {
		nodes, err := nodeNames(ctx, cs)
		if err != nil {
			fmt.Fprintln(os.Stderr, "list nodes:", err)
			time.Sleep(5 * time.Second)
			continue
		}
		got, err := collect(ctx, cs, nodes)
		if err != nil {
			fmt.Fprintln(os.Stderr, "scrape:", err)
		}
		st.mu.Lock()
		st.containers = got
		st.mu.Unlock()
	}
}

// pods groups the stored containers into PodMetrics for one namespace.
func (st *store) pods(ns string) map[string]map[string]any {
	st.mu.Lock()
	defer st.mu.Unlock()
	out := map[string]map[string]any{}
	for _, k := range sortedKeys(st.containers) {
		parts := strings.SplitN(k, "/", 3)
		if parts[0] != ns {
			continue
		}
		u := st.containers[k]
		pm, ok := out[parts[1]]
		if !ok {
			pm = map[string]any{
				"kind": "PodMetrics", "apiVersion": group + "/" + version,
				"metadata":   map[string]any{"name": parts[1], "namespace": ns, "creationTimestamp": metav1.Now()},
				"timestamp":  metav1.NewTime(u.at),
				"window":     metav1.Duration{Duration: u.window},
				"containers": []any{},
			}
			out[parts[1]] = pm
		}
		pm["containers"] = append(pm["containers"].([]any), map[string]any{"name": parts[2], "usage": quantities(u)})
	}
	return out
}

// nodes is NodeMetrics for every node: the kubelet's own figure for the
// node, which counts the system daemons no pod accounts for.
func (st *store) nodes() map[string]map[string]any {
	st.mu.Lock()
	defer st.mu.Unlock()
	out := map[string]map[string]any{}
	for k, u := range st.containers {
		name, ok := strings.CutPrefix(k, nodeKey+"/")
		if !ok {
			continue
		}
		out[name] = map[string]any{
			"kind": "NodeMetrics", "apiVersion": group + "/" + version,
			"metadata":  map[string]any{"name": name, "creationTimestamp": metav1.Now()},
			"timestamp": metav1.NewTime(u.at),
			"window":    metav1.Duration{Duration: u.window},
			"usage":     quantities(u),
		}
	}
	return out
}

func quantities(u usage) map[string]any {
	return map[string]any{
		"cpu":    resource.NewMilliQuantity(u.cpuMilli, resource.DecimalSI),
		"memory": resource.NewQuantity(u.mem, resource.BinarySI),
	}
}

func list(kind string, items []any) map[string]any {
	return map[string]any{"kind": kind, "apiVersion": group + "/" + version, "metadata": map[string]any{}, "items": items}
}

// notFound is the Status body the API server would send for a missing object,
// which is what lets a client tell "no such pod" from "the server broke".
func notFound(w http.ResponseWriter, resource, name string) {
	writeJSON(w, http.StatusNotFound, metav1.Status{
		TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   metav1.StatusFailure, Reason: metav1.StatusReasonNotFound, Code: http.StatusNotFound,
		Message: fmt.Sprintf("%s %q not found", resource, name),
		Details: &metav1.StatusDetails{Name: name, Group: group, Kind: resource},
	})
}

// routes serves discovery, which the aggregator checks before it calls the
// API Available, and the metrics themselves.
func routes(st *store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /apis/"+group+"/"+version, func(w http.ResponseWriter, r *http.Request) {
		verbs := []string{"get", "list"}
		writeJSON(w, http.StatusOK, metav1.APIResourceList{
			TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
			GroupVersion: group + "/" + version,
			APIResources: []metav1.APIResource{
				{Name: "pods", Kind: "PodMetrics", Namespaced: true, Verbs: verbs},
				{Name: "nodes", Kind: "NodeMetrics", Namespaced: false, Verbs: verbs},
			},
		})
	})
	mux.HandleFunc("GET /apis/"+group+"/"+version+"/namespaces/{ns}/pods", func(w http.ResponseWriter, r *http.Request) {
		pods := st.pods(r.PathValue("ns"))
		items := []any{}
		for _, name := range sortedKeys(pods) {
			items = append(items, pods[name])
		}
		writeJSON(w, http.StatusOK, list("PodMetricsList", items))
	})
	mux.HandleFunc("GET /apis/"+group+"/"+version+"/namespaces/{ns}/pods/{name}", func(w http.ResponseWriter, r *http.Request) {
		pm, ok := st.pods(r.PathValue("ns"))[r.PathValue("name")]
		if !ok {
			notFound(w, "pods", r.PathValue("name"))
			return
		}
		writeJSON(w, http.StatusOK, pm)
	})
	mux.HandleFunc("GET /apis/"+group+"/"+version+"/nodes", func(w http.ResponseWriter, r *http.Request) {
		nodes := st.nodes()
		items := []any{}
		for _, name := range sortedKeys(nodes) {
			items = append(items, nodes[name])
		}
		writeJSON(w, http.StatusOK, list("NodeMetricsList", items))
	})
	mux.HandleFunc("GET /apis/"+group+"/"+version+"/nodes/{name}", func(w http.ResponseWriter, r *http.Request) {
		nm, ok := st.nodes()[r.PathValue("name")]
		if !ok {
			notFound(w, "nodes", r.PathValue("name"))
			return
		}
		writeJSON(w, http.StatusOK, nm)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func nodeNames(ctx context.Context, cs *kubernetes.Clientset) ([]string, error) {
	list, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var names []string
	for _, n := range list.Items {
		names = append(names, n.Name)
	}
	return names, nil
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
