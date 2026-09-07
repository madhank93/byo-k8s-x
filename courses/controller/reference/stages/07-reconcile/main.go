// Your controller.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

// The resource this controller owns. A CustomResourceDefinition is itself an
// object, so the same dynamic client that will read Websites can create the
// definition that makes Websites exist.
const (
	group   = "byok8s.dev"
	version = "v1alpha1"
	kind    = "Website"
	plural  = "websites"
	crdName = plural + "." + group
)

var (
	crdGVR = schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}
	websiteGVR = schema.GroupVersionResource{Group: group, Version: version, Resource: plural}
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	// A controller runs until something asks it to stop. SIGTERM is what
	// Kubernetes sends a pod it is deleting, so that is what ends this loop.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	cfg, ns, err := clientConfig()
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}

	if err := installCRD(ctx, dyn); err != nil {
		return err
	}
	fmt.Printf("crd %s established\n", crdName)

	return observe(ctx, dyn, ns)
}

// observe reports every Website in the namespace and every change to one.
//
// The informer does the list-then-watch by itself, and keeps what it saw in a
// local store — so the events below are not the source of truth, they are a
// notification that the store changed. That distinction is what the rest of
// this course is built on.
func observe(ctx context.Context, dyn dynamic.Interface, ns string) error {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, 0, ns, nil)
	informer := factory.ForResource(websiteGVR).Informer()
	lister := factory.ForResource(websiteGVR).Lister()

	// The event says what changed; the lister says what there is. Answering
	// "how many Websites are there" from the cache costs nothing and cannot
	// rate-limit you, which is why controllers read this way and not with a
	// fresh List on every event.
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	defer queue.ShutDown()

	// Events carry objects; the queue carries keys. A key is the smallest
	// thing that says "look at this again", and two events about the same
	// object collapse into one entry — so a burst of updates costs one pass,
	// and the pass always reads the latest state rather than a stale event.
	enqueue := func(obj any) {
		key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
		if err != nil {
			fmt.Fprintf(os.Stderr, "key for object: %v\n", err)
			return
		}
		queue.Add(key)
	}

	report := func(verb string, obj any) {
		fmt.Printf("%s %s\n", verb, objName(obj))
		known, err := lister.List(labels.Everything())
		if err != nil {
			fmt.Fprintf(os.Stderr, "list from cache: %v\n", err)
			return
		}
		fmt.Printf("cache %d\n", len(known))
	}

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { report("add", obj); enqueue(obj) },
		UpdateFunc: func(_, obj any) { report("update", obj); enqueue(obj) },
		DeleteFunc: func(obj any) { report("delete", obj); enqueue(obj) },
	}); err != nil {
		return fmt.Errorf("add event handler: %w", err)
	}

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return fmt.Errorf("the cache never synced")
	}
	// Nothing should act on the cluster before this point: until the cache has
	// synced, "no Website exists" and "I have not been told yet" look the same.
	fmt.Printf("synced %d\n", len(informer.GetStore().List()))

	go func() {
		for processNext(ctx, queue, lister) {
		}
	}()

	<-ctx.Done()
	return nil
}

// processNext takes one key off the queue and works on it. It returns false
// only when the queue is shutting down, which is what ends the worker.
//
// Done must be called for every key taken, or the queue will refuse to hand
// out that key again — it holds it as "in progress" forever.
func processNext(ctx context.Context, queue workqueue.TypedRateLimitingInterface[string], lister cache.GenericLister) bool {
	key, shutdown := queue.Get()
	if shutdown {
		return false
	}
	defer queue.Done(key)

	reconcile(ctx, lister, key)
	queue.Forget(key)
	return true
}

// reconcile reads the world as it is now and decides what to do about it.
//
// It is handed a key, not an event, and it looks the object up itself: by the
// time a key comes off the queue the object may have changed again, or be
// gone. Deciding from the event that queued the key is the single most common
// controller bug — it makes the loop edge-triggered, and edges get missed.
func reconcile(_ context.Context, lister cache.GenericLister, key string) {
	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad key %q: %v\n", key, err)
		return
	}
	obj, err := lister.ByNamespace(ns).Get(name)
	if apierrors.IsNotFound(err) {
		// Not an error: the object is gone, and reconciling to "nothing"
		// is a legitimate outcome.
		fmt.Printf("reconcile %s gone\n", key)
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "get %s from cache: %v\n", key, err)
		return
	}
	site, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	image, _, _ := unstructured.NestedString(site.Object, "spec", "image")
	replicas, _, _ := unstructured.NestedInt64(site.Object, "spec", "replicas")
	fmt.Printf("reconcile %s image=%s replicas=%d\n", key, image, replicas)
}

// objName is the object's name, even when the object is a tombstone — a
// delete the informer noticed only by resyncing, where all it kept is the last
// state it saw.
func objName(obj any) string {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u.GetName()
	}
	return "<unknown>"
}

// clientConfig loads the kubeconfig the way every Kubernetes tool does, and
// returns the namespace its context selects — which is the namespace this
// controller works in.
func clientConfig() (*rest.Config, string, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	cfg, err := cc.ClientConfig()
	if err != nil {
		return nil, "", fmt.Errorf("load kubeconfig: %w", err)
	}
	ns, _, err := cc.Namespace()
	if err != nil {
		return nil, "", fmt.Errorf("read namespace: %w", err)
	}
	return cfg, ns, nil
}

// installCRD creates the definition if it is missing and updates it if it is
// not, then waits for the API server to serve it. Creating it at startup is
// what makes the controller self-contained: nothing has to be applied by hand
// before it can run.
func installCRD(ctx context.Context, dyn dynamic.Interface) error {
	want := crdObject()
	client := dyn.Resource(crdGVR)

	existing, err := client.Get(ctx, crdName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if _, err := client.Create(ctx, want, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create crd: %w", err)
		}
	case err != nil:
		return fmt.Errorf("get crd: %w", err)
	default:
		// An update must carry the resourceVersion it is based on, or the API
		// server has no way to tell a change from a stale overwrite.
		want.SetResourceVersion(existing.GetResourceVersion())
		if _, err := client.Update(ctx, want, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update crd: %w", err)
		}
	}
	return waitEstablished(ctx, client)
}

// waitEstablished blocks until the CRD reports Established. Until then the
// API server may still answer 404 for the new kind, so a controller that
// starts watching immediately races its own definition.
func waitEstablished(ctx context.Context, client dynamic.ResourceInterface) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	return wait.PollUntilContextCancel(ctx, 200*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		got, err := client.Get(ctx, crdName, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		conds, _, _ := unstructured.NestedSlice(got.Object, "status", "conditions")
		for _, c := range conds {
			cond, ok := c.(map[string]any)
			if !ok {
				continue
			}
			if cond["type"] == "Established" && cond["status"] == "True" {
				return true, nil
			}
		}
		return false, nil
	})
}

// crdObject is the definition itself. The schema is the resource's contract:
// the API server validates every write against it and fills in defaults, so
// every client — including this controller — can read a Website knowing the
// fields are the shape it expects.
func crdObject() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": crdName},
		"spec": map[string]any{
			"group": group,
			"names": map[string]any{
				"kind":     kind,
				"listKind": kind + "List",
				"plural":   plural,
				"singular": "website",
			},
			"scope": "Namespaced",
			"versions": []any{map[string]any{
				"name":    version,
				"served":  true,
				"storage": true,
				"schema":  map[string]any{"openAPIV3Schema": websiteSchema()},
				"additionalPrinterColumns": []any{
					map[string]any{"name": "Image", "type": "string", "jsonPath": ".spec.image"},
					map[string]any{"name": "Replicas", "type": "integer", "jsonPath": ".spec.replicas"},
					map[string]any{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"},
				},
			}},
		},
	}}
}

// websiteSchema types the fields of a Website. Numbers have to be int64 and
// float64 here: an unstructured object is JSON, and the conversion rejects
// anything a JSON document could not hold.
func websiteSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"spec": map[string]any{
				"type":     "object",
				"required": []any{"image"},
				"properties": map[string]any{
					"image": map[string]any{
						"type":        "string",
						"description": "The container image to serve.",
					},
					"replicas": map[string]any{
						"type":        "integer",
						"default":     int64(1),
						"minimum":     float64(0),
						"description": "How many of them to run.",
					},
					"host": map[string]any{
						"type":        "string",
						"description": "The hostname this site answers on.",
					},
				},
			},
		},
	}
}
