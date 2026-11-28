package hpa

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// metricsAPIService is the one name the aggregator routes metrics.k8s.io by.
// Whoever serves the API — the program in stages 4-7, the grader after — owns
// it for the length of one stage and must leave the cluster without it.
const metricsAPIService = "v1beta1.metrics.k8s.io"

var apiServicesGVR = schema.GroupVersionResource{Group: "apiregistration.k8s.io", Version: "v1", Resource: "apiservices"}

// removeAPIService deletes an APIService and waits until it has left
// discovery. A registered API with nothing behind it fails discovery for every
// client on the cluster, and stalls every namespace deletion, so this runs
// both before a stage and after it.
func removeAPIService(ctx context.Context, env *kube.Env, name string) error {
	dyn, err := dynamic.NewForConfig(env.Config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	dyn.Resource(apiServicesGVR).Delete(ctx, name, metav1.DeleteOptions{})
	return kube.WaitFor(ctx, "APIService "+name+" to go", func(ctx context.Context) (bool, error) {
		_, err := dyn.Resource(apiServicesGVR).Get(ctx, name, metav1.GetOptions{})
		return notFound(err), nil
	})
}

func apiService(ctx context.Context, env *kube.Env, name string) (*unstructured.Unstructured, error) {
	dyn, err := dynamic.NewForConfig(env.Config)
	if err != nil {
		return nil, err
	}
	return dyn.Resource(apiServicesGVR).Get(ctx, name, metav1.GetOptions{})
}

func available(as *unstructured.Unstructured) (bool, string) {
	conds, _, _ := unstructured.NestedSlice(as.Object, "status", "conditions")
	for _, c := range conds {
		m, _ := c.(map[string]any)
		if m["type"] == "Available" {
			return m["status"] == "True", fmt.Sprint(m["message"])
		}
	}
	return false, "no Available condition yet"
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// serve starts `app serve` and waits until the aggregator reports its API
// Available. The returned stop shuts the program down and removes the
// registration whether or not the program did.
func serve(ctx context.Context, env *kube.Env, bin string) (*runner.Process, func(), error) {
	if err := removeAPIService(ctx, env, metricsAPIService); err != nil {
		return nil, nil, err
	}
	port, err := freePort()
	if err != nil {
		return nil, nil, err
	}
	kc, cleanupEnv, err := scoped(env)
	if err != nil {
		return nil, nil, err
	}
	p, err := runner.Start(ctx, bin, kc, "serve", "--port", strconv.Itoa(port), "--external-host", stages.ExternalHost())
	if err != nil {
		cleanupEnv()
		return nil, nil, err
	}
	stop := func() {
		p.Stop(10 * time.Second)
		cleanupEnv()
		removeAPIService(context.WithoutCancel(ctx), env, metricsAPIService)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	last := "the program has not registered " + metricsAPIService
	err = kube.WaitFor(waitCtx, "APIService "+metricsAPIService+" to be Available", func(ctx context.Context) (bool, error) {
		if done, res := p.Exited(); done {
			return false, fmt.Errorf("the program exited (%d)\nstdout:\n%s\nstderr:\n%s", res.ExitCode, tail(res.Stdout), tail(res.Stderr))
		}
		as, err := apiService(ctx, env, metricsAPIService)
		if err != nil {
			return false, nil
		}
		ok, msg := available(as)
		last = msg
		return ok, nil
	})
	if err != nil {
		stop()
		return nil, nil, fmt.Errorf("%w\nthe aggregator says: %s\nthe program said:\n%s", err, last, tail(p.Stdout()+p.Stderr()))
	}
	return p, stop, nil
}

// stageAPIService — `app serve` registers itself as metrics.k8s.io/v1beta1,
// answers discovery through the aggregator, and unregisters on SIGTERM.
func stageAPIService(ctx context.Context, env *kube.Env, bin string) error {
	p, stop, err := serve(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()

	as, err := apiService(ctx, env, metricsAPIService)
	if err != nil {
		return err
	}
	if ca, _, _ := unstructured.NestedString(as.Object, "spec", "caBundle"); ca == "" {
		return fmt.Errorf("the APIService carries no caBundle, so the aggregator is trusting the program's certificate without checking it")
	}
	if skip, _, _ := unstructured.NestedBool(as.Object, "spec", "insecureSkipTLSVerify"); skip {
		return fmt.Errorf("the APIService sets insecureSkipTLSVerify — give the aggregator a caBundle instead")
	}
	if ns, _, _ := unstructured.NestedString(as.Object, "spec", "service", "namespace"); ns != env.Namespace {
		return fmt.Errorf("the APIService routes to a Service in namespace %q; keep it in the program's own namespace, %s", ns, env.Namespace)
	}

	list, err := env.Client.Discovery().ServerResourcesForGroupVersion("metrics.k8s.io/v1beta1")
	if err != nil {
		return fmt.Errorf("discovery of metrics.k8s.io/v1beta1 through the API server: %w", err)
	}
	want := map[string]bool{"pods": true, "nodes": false}
	for name, namespaced := range want {
		i := slices.IndexFunc(list.APIResources, func(r metav1.APIResource) bool { return r.Name == name })
		if i < 0 {
			return fmt.Errorf("discovery of metrics.k8s.io/v1beta1 does not list %s", name)
		}
		r := list.APIResources[i]
		if r.Namespaced != namespaced {
			return fmt.Errorf("discovery says %s namespaced=%v; it should be %v", name, r.Namespaced, namespaced)
		}
		if !slices.Contains(r.Verbs, "get") || !slices.Contains(r.Verbs, "list") {
			return fmt.Errorf("discovery gives %s the verbs %v; clients need get and list", name, r.Verbs)
		}
	}

	// A program that leaves its registration behind when it stops breaks
	// discovery for every client on the cluster.
	p.Stop(10 * time.Second)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := kube.WaitFor(waitCtx, "the program to remove its APIService on SIGTERM", func(ctx context.Context) (bool, error) {
		_, err := apiService(ctx, env, metricsAPIService)
		return notFound(err), nil
	}); err != nil {
		return fmt.Errorf("%w — an APIService with nothing behind it fails discovery cluster-wide, so a server that registers one removes it on the way out", err)
	}
	return nil
}

// metricsList is PodMetricsList or NodeMetricsList, read loosely so a wrong
// shape is reported rather than silently zeroed.
type metricsItem struct {
	Kind     string            `json:"kind"`
	Metadata metav1.ObjectMeta `json:"metadata"`
	// Timestamp and Window are what tell a client how old a sample is.
	Timestamp  metav1.Time       `json:"timestamp"`
	Window     metav1.Duration   `json:"window"`
	Usage      map[string]string `json:"usage"`
	Containers []struct {
		Name  string            `json:"name"`
		Usage map[string]string `json:"usage"`
	} `json:"containers"`
}

type metricsList struct {
	Kind  string        `json:"kind"`
	Items []metricsItem `json:"items"`
}

func getMetrics(ctx context.Context, env *kube.Env, path, selector string, into any) error {
	req := env.Client.Discovery().RESTClient().Get().AbsPath("/apis/metrics.k8s.io/v1beta1" + path)
	if selector != "" {
		req = req.Param("labelSelector", selector)
	}
	raw, err := req.DoRaw(ctx)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("GET %s: cannot decode the answer: %w\n%s", path, err, raw)
	}
	return nil
}

// podSum adds up a PodMetrics item's containers.
func podSum(it metricsItem) (cpu, mem int64, err error) {
	for _, c := range it.Containers {
		cq, err1 := resource.ParseQuantity(c.Usage["cpu"])
		mq, err2 := resource.ParseQuantity(c.Usage["memory"])
		if err1 != nil || err2 != nil {
			return 0, 0, fmt.Errorf("pod %s container %s: usage %v is not cpu and memory quantities", it.Metadata.Name, c.Name, c.Usage)
		}
		cpu += cq.MilliValue()
		mem += mq.Value()
	}
	return cpu, mem, nil
}

// stagePodMetrics — the program serves PodMetrics for list and get, scraped
// from every node on a timer.
func stagePodMetrics(ctx context.Context, env *kube.Env, bin string) error {
	nodes, err := workers(ctx, env)
	if err != nil {
		return err
	}
	if err := seedRunning(ctx, env,
		fixturePod(env, "fill", nodes[0], nil, ctr{name: "app", fillMiB: 40}),
		fixturePod(env, "burner", nodes[1], nil, ctr{name: "app", burnMilli: 200})); err != nil {
		return err
	}
	_, stop, err := serve(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()

	var list metricsList
	var last string
	waitCtx, cancel := context.WithTimeout(ctx, 75*time.Second)
	defer cancel()
	err = kube.WaitFor(waitCtx, "PodMetrics for fill and burner", func(ctx context.Context) (bool, error) {
		list = metricsList{}
		if err := getMetrics(ctx, env, "/namespaces/"+env.Namespace+"/pods", "", &list); err != nil {
			if ctx.Err() == nil {
				last = err.Error()
			}
			return false, nil
		}
		seen := map[string]metricsItem{}
		for _, it := range list.Items {
			seen[it.Metadata.Name] = it
		}
		fill, ok1 := seen["fill"]
		burner, ok2 := seen["burner"]
		if !ok1 || !ok2 {
			last = fmt.Sprintf("the list holds %d item(s) and not both pods", len(list.Items))
			return false, nil
		}
		_, fillMem, err := podSum(fill)
		if err != nil {
			return false, err
		}
		burnCPU, _, err := podSum(burner)
		if err != nil {
			return false, err
		}
		last = fmt.Sprintf("fill memory %d bytes, burner cpu %dm", fillMem, burnCPU)
		return fillMem >= 40*mi && burnCPU >= 100 && burnCPU <= 300, nil
	})
	if err != nil {
		return fmt.Errorf("%w\nlast answer: %s\n(fill holds 40Mi; burner spins under a 200m limit)", err, last)
	}
	if list.Kind != "PodMetricsList" {
		return fmt.Errorf("the list answer has kind %q, want PodMetricsList", list.Kind)
	}
	for _, it := range list.Items {
		if it.Metadata.Namespace != env.Namespace || (it.Metadata.Name != "fill" && it.Metadata.Name != "burner") {
			return fmt.Errorf("listing namespace %s, which holds only fill and burner, returned pod %s/%s", env.Namespace, it.Metadata.Namespace, it.Metadata.Name)
		}
		if it.Timestamp.IsZero() || it.Window.Duration <= 0 {
			return fmt.Errorf("pod %s has no timestamp or window — a client cannot tell how old a sample is without them", it.Metadata.Name)
		}
		if len(it.Containers) == 0 {
			return fmt.Errorf("pod %s has no containers in its metrics", it.Metadata.Name)
		}
	}

	var one metricsItem
	if err := getMetrics(ctx, env, "/namespaces/"+env.Namespace+"/pods/fill", "", &one); err != nil {
		return fmt.Errorf("GET pods/fill: %w", err)
	}
	if one.Kind != "PodMetrics" || one.Metadata.Name != "fill" || len(one.Containers) != 1 || one.Containers[0].Name != "app" {
		return fmt.Errorf("GET pods/fill answered kind %q name %q with %d container(s); want PodMetrics fill with container app", one.Kind, one.Metadata.Name, len(one.Containers))
	}
	err = getMetrics(ctx, env, "/namespaces/"+env.Namespace+"/pods/no-such-pod", "", &one)
	if !notFound(err) {
		return fmt.Errorf("GET pods/no-such-pod should be a 404 NotFound Status, got: %v", err)
	}
	return nil
}

// stageNodeMetrics — the program serves NodeMetrics for every node.
func stageNodeMetrics(ctx context.Context, env *kube.Env, bin string) error {
	nodes, err := kindNodes(ctx, env, "")
	if err != nil {
		return err
	}
	_, stop, err := serve(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()

	var list metricsList
	var last string
	waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	err = kube.WaitFor(waitCtx, "NodeMetrics for every node", func(ctx context.Context) (bool, error) {
		list = metricsList{}
		if err := getMetrics(ctx, env, "/nodes", "", &list); err != nil {
			if ctx.Err() == nil {
				last = err.Error()
			}
			return false, nil
		}
		found := 0
		for _, n := range nodes {
			if slices.ContainsFunc(list.Items, func(it metricsItem) bool { return it.Metadata.Name == n }) {
				found++
			}
		}
		last = fmt.Sprintf("%d of %d nodes listed", found, len(nodes))
		return found == len(nodes), nil
	})
	if err != nil {
		return fmt.Errorf("%w\nlast answer: %s", err, last)
	}
	if list.Kind != "NodeMetricsList" {
		return fmt.Errorf("the list answer has kind %q, want NodeMetricsList", list.Kind)
	}
	for _, n := range nodes {
		i := slices.IndexFunc(list.Items, func(it metricsItem) bool { return it.Metadata.Name == n })
		if i < 0 {
			return fmt.Errorf("node %s is missing from the list", n)
		}
		it := list.Items[i]
		cq, err1 := resource.ParseQuantity(it.Usage["cpu"])
		mq, err2 := resource.ParseQuantity(it.Usage["memory"])
		if err1 != nil || err2 != nil || cq.IsZero() || mq.IsZero() {
			return fmt.Errorf("node %s has usage %v — both cpu and memory are quantities, and a running node uses some of each", n, it.Usage)
		}
		if it.Timestamp.IsZero() || it.Window.Duration <= 0 {
			return fmt.Errorf("node %s has no timestamp or window", n)
		}
	}

	name := nodes[0]
	var one metricsItem
	if err := getMetrics(ctx, env, "/nodes/"+name, "", &one); err != nil {
		return fmt.Errorf("GET nodes/%s: %w", name, err)
	}
	if one.Kind != "NodeMetrics" || one.Metadata.Name != name {
		return fmt.Errorf("GET nodes/%s answered kind %q name %q", name, one.Kind, one.Metadata.Name)
	}
	s, err := readSummary(ctx, env, name)
	if err != nil {
		return err
	}
	mq, _ := resource.ParseQuantity(one.Usage["memory"])
	ws := float64(s.Node.Memory.WorkingSetBytes)
	if m := float64(mq.Value()); m < ws/2 || m > ws*2 {
		return fmt.Errorf("node %s: the program says memory %d bytes and the kubelet reports a working set of %.0f — a node's usage is the node's own figure, not its pods added up", name, mq.Value(), ws)
	}
	if err := getMetrics(ctx, env, "/nodes/no-such-node", "", &one); !notFound(err) {
		return fmt.Errorf("GET nodes/no-such-node should be a 404 NotFound Status, got: %v", err)
	}
	return nil
}

// stageKubectlTop — the real kubectl top reads the program's API: a label
// selector, every namespace at once, and nodes.
func stageKubectlTop(ctx context.Context, env *kube.Env, bin string) error {
	nodes, err := workers(ctx, env)
	if err != nil {
		return err
	}
	if err := seedRunning(ctx, env,
		fixturePod(env, "hot", nodes[0], map[string]string{"tier": "hot"}, ctr{name: "app", fillMiB: 40}),
		fixturePod(env, "cold", nodes[1], map[string]string{"tier": "cold"}, ctr{name: "app"})); err != nil {
		return err
	}
	_, stop, err := serve(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()
	kc, cleanup, err := scoped(env)
	if err != nil {
		return err
	}
	defer cleanup()

	var out string
	var last error
	waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if err := kube.WaitFor(waitCtx, "kubectl top pod to show hot and cold", func(ctx context.Context) (bool, error) {
		out, last = kubectlTop(ctx, kc, "pod", "--no-headers")
		return last == nil && hasRow(out, "hot") && hasRow(out, "cold"), nil
	}); err != nil {
		if last != nil {
			return fmt.Errorf("%w\nlast attempt: %v", err, last)
		}
		return fmt.Errorf("%w\nlast output:\n%s", err, tail(out))
	}

	out, err = kubectlTop(ctx, kc, "pod", "-l", "tier=hot", "--no-headers")
	if err != nil {
		return err
	}
	if !hasRow(out, "hot") || hasRow(out, "cold") {
		return fmt.Errorf("kubectl top pod -l tier=hot printed:\n%s\nwant hot and not cold — kubectl passes the selector as labelSelector, and the server applies it", tail(out))
	}

	out, err = kubectlTop(ctx, kc, "pod", "-A", "--no-headers")
	if err != nil {
		return err
	}
	if !hasRow(out, env.Namespace, "hot") || !strings.Contains(out, "kube-system") {
		return fmt.Errorf("kubectl top pod -A printed:\n%s\nwant this namespace's pods and kube-system's — every namespace is GET /apis/metrics.k8s.io/v1beta1/pods", tail(out))
	}

	out, err = kubectlTop(ctx, kc, "node", "--no-headers")
	if err != nil {
		return err
	}
	all, err := kindNodes(ctx, env, "")
	if err != nil {
		return err
	}
	for _, n := range all {
		if !hasRow(out, n) {
			return fmt.Errorf("kubectl top node printed:\n%s\nand node %s is not in it", tail(out), n)
		}
	}
	return nil
}

// hasRow reports whether some line of kubectl's output starts with these
// columns.
func hasRow(out string, cols ...string) bool {
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) >= len(cols) && slices.Equal(f[:len(cols)], cols) {
			return true
		}
	}
	return false
}
