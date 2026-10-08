// Your diff and safe apply.
//
// Stage 1: `objects -f FILE` reads a multi-document manifest and prints one
// Kind/name line per object.
// Stage 2: `get -f FILE` says which of those objects the cluster already has.
// Stage 3: `patch -f FILE` computes the three-way merge patch client-side
// apply would send.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/jsonmergepatch"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: app <objects> -f FILE")
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	file := fs.String("f", "", "manifest file")
	fs.Parse(os.Args[2:])

	code, err := run(cmd, *file)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		if code == 0 {
			code = 2
		}
	}
	os.Exit(code)
}

func run(cmd, file string) (int, error) {
	switch cmd {
	case "objects":
		objs, err := load(file)
		if err != nil {
			return 2, err
		}
		for _, o := range objs {
			fmt.Println(ref(o))
		}
		return 0, nil
	case "get":
		c, objs, err := setup(file)
		if err != nil {
			return 2, err
		}
		for _, o := range objs {
			live, err := c.live(o)
			if err != nil {
				return 2, err
			}
			state := "absent"
			if live != nil {
				state = "present"
			}
			fmt.Println(ref(o), state)
		}
		return 0, nil
	case "patch":
		c, objs, err := setup(file)
		if err != nil {
			return 2, err
		}
		for _, o := range objs {
			live, err := c.live(o)
			if err != nil {
				return 2, err
			}
			if live == nil {
				fmt.Println(ref(o), "create")
				continue
			}
			p, err := threeWay(o, live)
			if err != nil {
				return 2, fmt.Errorf("%s: %w", ref(o), err)
			}
			fmt.Println(ref(o), string(p))
		}
		return 0, nil
	}
	return 2, fmt.Errorf("unknown command %q", cmd)
}

// client is everything a command needs to address an arbitrary object: a
// dynamic client, a mapper from kinds to resources, and the namespace the
// kubeconfig's context selects.
type client struct {
	ctx    context.Context
	dyn    dynamic.Interface
	mapper meta.RESTMapper
	ns     string
}

// setup loads the manifest and connects, the way every cluster command starts.
func setup(file string) (*client, []*unstructured.Unstructured, error) {
	objs, err := load(file)
	if err != nil {
		return nil, nil, err
	}
	kc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{})
	cfg, err := kc.ClientConfig()
	if err != nil {
		return nil, nil, err
	}
	ns, _, err := kc.Namespace()
	if err != nil {
		return nil, nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))
	c := &client{ctx: context.Background(), dyn: dyn, mapper: mapper, ns: ns}
	for _, o := range objs {
		if _, err := c.resource(o); err != nil {
			return nil, nil, err
		}
	}
	return c, objs, nil
}

// resource resolves an object to the endpoint that serves it, and fills in the
// namespace: the manifest's if it names one, the context's otherwise. A
// cluster-scoped kind never gets one.
func (c *client) resource(o *unstructured.Unstructured) (dynamic.ResourceInterface, error) {
	gvk := o.GroupVersionKind()
	m, err := c.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, fmt.Errorf("%s: the server has no %s: %w", ref(o), gvk, err)
	}
	if m.Scope.Name() != meta.RESTScopeNameNamespace {
		o.SetNamespace("")
		return c.dyn.Resource(m.Resource), nil
	}
	if o.GetNamespace() == "" {
		o.SetNamespace(c.ns)
	}
	return c.dyn.Resource(m.Resource).Namespace(o.GetNamespace()), nil
}

// live fetches the object as the cluster has it, or nil if it does not exist.
func (c *client) live(o *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	r, err := c.resource(o)
	if err != nil {
		return nil, err
	}
	l, err := r.Get(c.ctx, o.GetName(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ref(o), err)
	}
	return l, nil
}

// lastApplied is where client-side apply keeps what it sent last time.
const lastApplied = "kubectl.kubernetes.io/last-applied-configuration"

// threeWay is the JSON merge patch that takes live to desired. Changes come from
// comparing live with desired; deletions only from comparing last-applied with
// desired, so a field someone else added is left alone while a field this
// manifest used to set and no longer does is removed.
func threeWay(desired, live *unstructured.Unstructured) ([]byte, error) {
	original := []byte("{}")
	if a := live.GetAnnotations()[lastApplied]; a != "" {
		original = []byte(a)
	}
	modified, err := json.Marshal(withoutLastApplied(desired).Object)
	if err != nil {
		return nil, err
	}
	current, err := json.Marshal(withoutLastApplied(live).Object)
	if err != nil {
		return nil, err
	}
	var orig map[string]any
	if err := json.Unmarshal(original, &orig); err != nil {
		return nil, fmt.Errorf("unreadable %s annotation: %w", lastApplied, err)
	}
	original, err = json.Marshal(withoutLastApplied(&unstructured.Unstructured{Object: orig}).Object)
	if err != nil {
		return nil, err
	}
	return jsonmergepatch.CreateThreeWayJSONMergePatch(original, modified, current)
}

// withoutLastApplied is a copy with the annotation removed, so the bookkeeping
// never shows up as a change of its own.
func withoutLastApplied(o *unstructured.Unstructured) *unstructured.Unstructured {
	c := o.DeepCopy()
	unstructured.RemoveNestedField(c.Object, "metadata", "annotations", lastApplied)
	if a, ok, _ := unstructured.NestedMap(c.Object, "metadata", "annotations"); ok && len(a) == 0 {
		unstructured.RemoveNestedField(c.Object, "metadata", "annotations")
	}
	return c
}

// load reads every object in a manifest. YAML and JSON documents may be mixed,
// and empty or comment-only documents are skipped, as kubectl skips them.
func load(file string) ([]*unstructured.Unstructured, error) {
	if file == "" {
		return nil, fmt.Errorf("which file? (-f)")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var out []*unstructured.Unstructured
	for i := 1; ; i++ {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, fmt.Errorf("%s document %d: %w", file, i, err)
		}
		if len(m) == 0 {
			continue
		}
		o := &unstructured.Unstructured{Object: m}
		if o.GetAPIVersion() == "" || o.GetKind() == "" || o.GetName() == "" {
			return nil, fmt.Errorf("%s document %d: apiVersion, kind and metadata.name are all required", file, i)
		}
		out = append(out, o)
	}
}

func ref(o *unstructured.Unstructured) string { return o.GetKind() + "/" + o.GetName() }
