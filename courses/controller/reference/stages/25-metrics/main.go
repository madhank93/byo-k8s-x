// Your controller.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync/atomic"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	appsv1apply "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1apply "k8s.io/client-go/applyconfigurations/core/v1"
	metav1apply "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
)

// The resource this controller owns. A CustomResourceDefinition is itself an
// object, so the same dynamic client that will read Websites can create the
// definition that makes Websites exist.
// resyncEvery is how long a Website can stay wrong before the controller
// notices on its own. Short enough to be useful, long enough that a thousand
// Websites do not flood the API server.
const resyncEvery = 10 * time.Second

// pausedAnnotation stops this controller acting on one Website without
// stopping it acting on any other.
const pausedAnnotation = "byok8s.dev/paused"

// finalizerName is this controller's claim on a Website: while it is present,
// the API server will not delete the object.
const finalizerName = "byok8s.dev/cleanup"

// fieldManager is this controller's name in every object's managedFields. It
// is part of the contract: change it and the controller no longer owns the
// fields it set under the old name.
const fieldManager = "byok8s-controller"

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

// reconcileTotal counts reconciles rather than successes: the useful question
// to ask a controller is how hard it is working, and a loop that keeps failing
// is working hardest of all.
var reconcileTotal atomic.Int64

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

	metricsAddr := flag.String("metrics-addr", "", "address to serve /metrics on; empty serves nothing")
	flag.Parse()

	// Started before anything else can fail, so a controller stuck on a
	// broken kubeconfig still answers a liveness probe.
	serveMetrics(ctx, *metricsAddr)

	cfg, ns, err := clientConfig()
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("clientset: %w", err)
	}

	if err := installCRD(ctx, dyn); err != nil {
		return err
	}
	fmt.Printf("crd %s established\n", crdName)

	return observe(ctx, dyn, clientset, ns)
}

// observe reports every Website in the namespace and every change to one.
//
// The informer does the list-then-watch by itself, and keeps what it saw in a
// local store — so the events below are not the source of truth, they are a
// notification that the store changed. That distinction is what the rest of
// this course is built on.
func observe(ctx context.Context, dyn dynamic.Interface, clientset kubernetes.Interface, ns string) error {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, 0, ns, nil)
	informer := factory.ForResource(websiteGVR).Informer()
	// The event says what changed; the lister says what there is. Answering
	// "how many Websites are there" from the cache costs nothing and cannot
	// rate-limit you, which is why controllers read this way and not with a
	// fresh List on every event.
	lister := factory.ForResource(websiteGVR).Lister()

	// Events are a log for people, attached to the object they are about —
	// the bottom half of `kubectl describe`. The broadcaster batches and
	// aggregates them; without the shutdown, the last ones are never sent.
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: clientset.CoreV1().Events(ns)})
	defer broadcaster.Shutdown()
	recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: fieldManager})

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

	// Watch the children too. An event about a Deployment is turned into its
	// owner's key, and the ordinary pass follows: no "the child was deleted"
	// branch exists, because the pass already rebuilds whatever is missing.
	children := informers.NewSharedInformerFactoryWithOptions(clientset, 0, informers.WithNamespace(ns))
	deployments := children.Apps().V1().Deployments().Informer()
	if _, err := deployments.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { enqueueOwner(queue, obj) },
		UpdateFunc: func(_, obj any) { enqueueOwner(queue, obj) },
		DeleteFunc: func(obj any) { enqueueOwner(queue, obj) },
	}); err != nil {
		return fmt.Errorf("add child event handler: %w", err)
	}

	factory.Start(ctx.Done())
	children.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced, deployments.HasSynced) {
		return fmt.Errorf("the cache never synced")
	}
	// Nothing should act on the cluster before this point: until the cache has
	// synced, "no Website exists" and "I have not been told yet" look the same.
	fmt.Printf("synced %d\n", len(informer.GetStore().List()))

	go func() {
		for processNext(ctx, queue, lister, clientset, dyn, recorder) {
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
func processNext(ctx context.Context, queue workqueue.TypedRateLimitingInterface[string], lister cache.GenericLister, clientset kubernetes.Interface, dyn dynamic.Interface, recorder record.EventRecorder) bool {
	key, shutdown := queue.Get()
	if shutdown {
		return false
	}
	defer queue.Done(key)

	if err := reconcile(ctx, lister, clientset, dyn, recorder, key); err != nil {
		// Rate-limited, not immediate: a failure that repeats — a webhook
		// that is down, a quota that is full — must not become a hot loop
		// against the API server. The delay grows with each attempt.
		fmt.Printf("retry %s attempt %d: %v\n", key, queue.NumRequeues(key)+1, err)
		queue.AddRateLimited(key)
		return true
	}
	// Forget resets the backoff. Without it the next failure of this key
	// starts where the last one left off, however long ago that was.
	queue.Forget(key)

	// And come back on a timer. Nothing has to happen for a Deployment to
	// stop matching its Website — a hand-run kubectl scale changes the child,
	// not the parent, so no event about the Website will ever arrive. A slow
	// heartbeat catches that; it is the floor under correctness, not the way
	// changes are normally noticed.
	queue.AddAfter(key, resyncEvery)
	return true
}

// reconcile reads the world as it is now and decides what to do about it.
//
// It is handed a key, not an event, and it looks the object up itself: by the
// time a key comes off the queue the object may have changed again, or be
// gone. Deciding from the event that queued the key is the single most common
// controller bug — it makes the loop edge-triggered, and edges get missed.
func reconcile(ctx context.Context, lister cache.GenericLister, clientset kubernetes.Interface, dyn dynamic.Interface, recorder record.EventRecorder, key string) error {
	reconcileTotal.Add(1)

	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		// A key this program cannot parse will never parse: retrying is
		// pointless, so it is dropped rather than requeued.
		fmt.Fprintf(os.Stderr, "bad key %q: %v\n", key, err)
		return nil
	}
	obj, err := lister.ByNamespace(ns).Get(name)
	if apierrors.IsNotFound(err) {
		// Not an error: the object is gone, and reconciling to "nothing"
		// is a legitimate outcome.
		fmt.Printf("reconcile %s gone\n", key)
		return nil
	}
	if err != nil {
		return fmt.Errorf("get %s from cache: %w", key, err)
	}
	site, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil
	}
	// Deletion is a reconcile like any other: the object is still there, it
	// just has a deletionTimestamp. Nothing will remove it until every
	// finalizer on it is gone, which is the window this controller uses.
	if site.GetDeletionTimestamp() != nil {
		return finalize(ctx, clientset, dyn, site)
	}
	if !slices.Contains(site.GetFinalizers(), finalizerName) {
		return addFinalizer(ctx, dyn, site)
	}

	// An escape hatch for the humans. When something is wrong — a bad spec
	// mid-incident, a migration in progress — the way out has to be to stop
	// the controller touching one object, not to delete the controller.
	if site.GetAnnotations()[pausedAnnotation] == "true" {
		fmt.Printf("paused %s\n", key)
		return nil
	}

	image, _, _ := unstructured.NestedString(site.Object, "spec", "image")
	replicas, _, _ := unstructured.NestedInt64(site.Object, "spec", "replicas")
	fmt.Printf("reconcile %s image=%s replicas=%d\n", key, image, replicas)

	if err := ensureDeployment(ctx, clientset, site, image, int32(replicas)); err != nil {
		return err
	}
	if err := ensureService(ctx, clientset, site); err != nil {
		return err
	}
	return writeStatus(ctx, clientset, dyn, recorder, site)
}

// writeStatus reports what the controller actually achieved, which is not the
// same as what was asked for: spec is the wish, status is the world.
//
// observedGeneration is the honest part. It says which version of the spec
// this status describes, so a client can tell "everything is fine" from "I
// have not caught up with your change yet".
func writeStatus(ctx context.Context, clientset kubernetes.Interface, dyn dynamic.Interface, recorder record.EventRecorder, site *unstructured.Unstructured) error {
	ns, name := site.GetNamespace(), site.GetName()

	var ready, wanted int64
	dep, err := clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	switch {
	case err == nil:
		ready = int64(dep.Status.ReadyReplicas)
		if dep.Spec.Replicas != nil {
			wanted = int64(*dep.Spec.Replicas)
		}
	case !apierrors.IsNotFound(err):
		return fmt.Errorf("read deployment status: %w", err)
	}

	// Read, change, write — and if someone else wrote in between, read again.
	// The whole sequence is inside the retry: resubmitting the same stale
	// object would fail identically forever.
	client := dyn.Resource(websiteGVR).Namespace(ns)
	var transitioned bool
	var reason, message string
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		updated, err := client.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		before := condition(updated, "Ready")
		if err := unstructured.SetNestedField(updated.Object, ready, "status", "replicas"); err != nil {
			return err
		}
		if err := unstructured.SetNestedField(updated.Object, updated.GetGeneration(), "status", "observedGeneration"); err != nil {
			return err
		}
		if err := setReadyCondition(updated, ready, wanted); err != nil {
			return err
		}
		after := condition(updated, "Ready")
		transitioned = before == nil || before["status"] != after["status"]
		reason, _ = after["reason"].(string)
		message, _ = after["message"].(string)

		_, err = client.UpdateStatus(ctx, updated, metav1.UpdateOptions{})
		return err
	})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("write status: %w", err)
	}

	// Record transitions, not passes. An event every ten seconds saying
	// "reconciled" buries the one line that mattered, and does it while the
	// cluster is busiest.
	if transitioned {
		recorder.Eventf(site, corev1.EventTypeNormal, reason, "%s", message)
	}
	return nil
}

// condition digs one condition out of an object's status.
func condition(obj *unstructured.Unstructured, name string) map[string]any {
	conds, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, raw := range conds {
		cond, ok := raw.(map[string]any)
		if ok && cond["type"] == name {
			return cond
		}
	}
	return nil
}

// ensureService gives the Website an address inside the cluster.
//
// Each child is stated the same way, so adding one is adding a call rather
// than a mechanism.
func ensureService(ctx context.Context, clientset kubernetes.Interface, site *unstructured.Unstructured) error {
	ns, name := site.GetNamespace(), site.GetName()
	ac := corev1apply.Service(name, ns).
		WithLabels(childLabels(name)).
		WithOwnerReferences(ownerRef(site)).
		WithSpec(corev1apply.ServiceSpec().
			WithSelector(selectorLabels(name)).
			WithPorts(corev1apply.ServicePort().
				WithName("http").
				WithPort(80).
				WithTargetPort(intstr.FromInt32(80))))

	if _, err := clientset.CoreV1().Services(ns).Apply(ctx, ac,
		metav1.ApplyOptions{FieldManager: fieldManager, Force: true}); err != nil {
		return fmt.Errorf("apply service: %w", err)
	}
	return nil
}

// ensureDeployment states the Deployment this Website means and lets the API
// server work out what that changes.
//
// Server-side apply replaces the whole check-then-create-then-patch dance:
// send the object as it should be, and the API server records which fields
// this manager owns and merges them with everyone else's. Creating, adopting
// and repairing collapse into this one call.
func ensureDeployment(ctx context.Context, clientset kubernetes.Interface, site *unstructured.Unstructured, image string, replicas int32) error {
	if err := ownedByUs(ctx, clientset, site); err != nil {
		return err
	}

	// Force takes back the fields this manager owns when someone else has
	// written them. For a controller that is the right answer: the Website's
	// spec is the intent, and a hand-edited child is drift.
	if _, err := clientset.AppsV1().Deployments(site.GetNamespace()).Apply(ctx,
		deploymentFor(site, image, replicas),
		metav1.ApplyOptions{FieldManager: fieldManager, Force: true}); err != nil {
		return fmt.Errorf("apply deployment: %w", err)
	}
	return nil
}

// ownedByUs refuses to touch a child that belongs to something else.
//
// Names collide. A Website called grafana in a namespace that already has a
// Deployment called grafana, owned by a Helm release, is not hypothetical —
// and with force-apply behind it, writing to that object would silently take
// it over. An object with no controller is unclaimed and may be adopted; an
// object with a different controller is somebody else's, and the only correct
// move is to stop and say so.
func ownedByUs(ctx context.Context, clientset kubernetes.Interface, site *unstructured.Unstructured) error {
	ns, name := site.GetNamespace(), site.GetName()
	existing, err := clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get deployment: %w", err)
	}
	owner := metav1.GetControllerOf(existing)
	if owner == nil || owner.UID == site.GetUID() {
		return nil
	}
	return fmt.Errorf("deployment %s/%s is controlled by %s %q — leaving it alone", ns, name, owner.Kind, owner.Name)
}

// deploymentFor is the Deployment a Website means, as an apply configuration:
// a partial object stating only what this manager claims. Anything left out is
// left to whoever else owns it.
func deploymentFor(site *unstructured.Unstructured, image string, replicas int32) *appsv1apply.DeploymentApplyConfiguration {
	ns, name := site.GetNamespace(), site.GetName()
	return appsv1apply.Deployment(name, ns).
		WithLabels(siteLabels(site)).
		WithOwnerReferences(ownerRef(site)).
		WithSpec(appsv1apply.DeploymentSpec().
			WithReplicas(replicas).
			// A selector cannot be changed after creation, so it holds only
			// the label that identifies the app — never one that varies with
			// the spec.
			WithSelector(metav1apply.LabelSelector().WithMatchLabels(selectorLabels(name))).
			WithTemplate(corev1apply.PodTemplateSpec().
				WithLabels(childLabels(name)).
				WithSpec(corev1apply.PodSpec().
					WithContainers(corev1apply.Container().
						WithName("web").
						WithImage(image).
						WithPorts(corev1apply.ContainerPort().WithContainerPort(80))))))
}

// enqueueOwner turns an event about a child into work on its owner.
//
// This is what owner references buy beyond garbage collection: any object can
// point back at the one thing responsible for it, so a child that is deleted
// or edited reaches the loop in the time it takes to hear about it — rather
// than waiting for the resync timer to come round.
func enqueueOwner(queue workqueue.TypedRateLimitingInterface[string], obj any) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	child, err := meta.Accessor(obj)
	if err != nil {
		return
	}
	owner := metav1.GetControllerOf(child)
	if owner == nil || owner.Kind != kind {
		return
	}
	key := child.GetNamespace() + "/" + owner.Name
	fmt.Printf("child %s/%s owned by %s\n", child.GetNamespace(), child.GetName(), key)
	queue.Add(key)
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
				// A status subresource splits the object in two: clients write
				// spec, the controller writes status, and neither can clobber
				// the other by sending back a whole object it had read.
				"subresources": map[string]any{"status": map[string]any{}},
				"additionalPrinterColumns": []any{
					map[string]any{"name": "Image", "type": "string", "jsonPath": ".spec.image"},
					map[string]any{"name": "Replicas", "type": "integer", "jsonPath": ".spec.replicas"},
					map[string]any{"name": "Ready", "type": "string", "jsonPath": `.status.conditions[?(@.type=="Ready")].status`},
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
			"status": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"replicas":           map[string]any{"type": "integer"},
					"observedGeneration": map[string]any{"type": "integer"},
					"conditions": map[string]any{
						"type": "array",
						// A map list keyed by type: the API server then treats
						// two writers setting different condition types as
						// changing different fields, not fighting over one list.
						"x-kubernetes-list-type":     "map",
						"x-kubernetes-list-map-keys": []any{"type"},
						"items": map[string]any{
							"type":     "object",
							"required": []any{"type", "status", "lastTransitionTime", "reason"},
							"properties": map[string]any{
								"type":               map[string]any{"type": "string"},
								"status":             map[string]any{"type": "string"},
								"reason":             map[string]any{"type": "string"},
								"message":            map[string]any{"type": "string"},
								"lastTransitionTime": map[string]any{"type": "string", "format": "date-time"},
								"observedGeneration": map[string]any{"type": "integer"},
							},
						},
					},
				},
			},
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

// ownerRef is what makes a child belong to the Website.
//
// Garbage collection is the API server's job, not the controller's: an owned
// object whose owner is gone is deleted for you. Controller: true also marks
// which owner is in charge, so two controllers cannot both claim the same
// child and fight over it.
func ownerRef(site *unstructured.Unstructured) *metav1apply.OwnerReferenceApplyConfiguration {
	return metav1apply.OwnerReference().
		WithAPIVersion(group + "/" + version).
		WithKind(kind).
		WithName(site.GetName()).
		WithUID(site.GetUID()).
		WithController(true).
		WithBlockOwnerDeletion(true)
}

// selectorLabels are what a Service selects and a Deployment matches: a subset
// of childLabels, because a selector is immutable once the object exists.
func selectorLabels(name string) map[string]string {
	return map[string]string{"app": name}
}

// childLabels are stamped on everything a Website owns, and are what the
// Service selects on. One definition, so the two can never disagree.
func childLabels(name string) map[string]string {
	return map[string]string{"app": name, "byok8s.dev/website": name}
}

// siteLabels are the child's own labels: the join labels plus the host, so a
// Website's objects can be found by the site they serve.
//
// A label value the API server will not accept — a host longer than 63
// characters, say — makes every write of this child fail until the Website is
// corrected. That is the ordinary shape of a controller error: not a bug in
// the loop, a spec the cluster refuses.
func siteLabels(site *unstructured.Unstructured) map[string]string {
	labels := childLabels(site.GetName())
	if host, ok, _ := unstructured.NestedString(site.Object, "spec", "host"); ok && host != "" {
		labels["byok8s.dev/host"] = host
	}
	return labels
}

// setReadyCondition records whether the Website is actually serving.
//
// A condition is the answer to a yes/no question with a reason attached, and
// the reason is the useful half: "not ready" is a status page, "not ready
// because 0 of 3 replicas are available" is a diagnosis. lastTransitionTime is
// only touched when the answer changes, so it means "since when", which is
// what anyone reading it wants to know.
func setReadyCondition(site *unstructured.Unstructured, ready, wanted int64) error {
	status, reason, message := "False", "Deploying", fmt.Sprintf("%d of %d replicas are ready", ready, wanted)
	if wanted > 0 && ready >= wanted {
		status, reason = "True", "MinimumReplicasAvailable"
	}

	conditions, _, err := unstructured.NestedSlice(site.Object, "status", "conditions")
	if err != nil {
		return err
	}
	changed := time.Now().UTC().Format(time.RFC3339)
	next := make([]any, 0, len(conditions)+1)
	for _, raw := range conditions {
		cond, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if cond["type"] != "Ready" {
			next = append(next, cond)
			continue
		}
		if cond["status"] == status {
			changed, _ = cond["lastTransitionTime"].(string)
		}
	}
	next = append(next, map[string]any{
		"type":               "Ready",
		"status":             status,
		"reason":             reason,
		"message":            message,
		"lastTransitionTime": changed,
		"observedGeneration": site.GetGeneration(),
	})
	return unstructured.SetNestedSlice(site.Object, next, "status", "conditions")
}

// addFinalizer asks the API server to hold the object open for us when
// someone deletes it. It has to be added before the children exist, not
// after: a delete that arrives in between would leave nothing to clean up
// with.
func addFinalizer(ctx context.Context, dyn dynamic.Interface, site *unstructured.Unstructured) error {
	client := dyn.Resource(websiteGVR).Namespace(site.GetNamespace())
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest, err := client.Get(ctx, site.GetName(), metav1.GetOptions{})
		if err != nil {
			return err
		}
		if slices.Contains(latest.GetFinalizers(), finalizerName) {
			return nil
		}
		latest.SetFinalizers(append(latest.GetFinalizers(), finalizerName))
		_, err = client.Update(ctx, latest, metav1.UpdateOptions{})
		return err
	})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("add finalizer: %w", err)
	}
	fmt.Printf("finalizer added %s/%s\n", site.GetNamespace(), site.GetName())
	return nil
}

// finalize does the work that garbage collection cannot, then gets out of the
// way.
//
// Owner references already delete the children, so this deliberately does
// something they could not: it removes the Service first and says so. The
// shape is what matters — do the external work, then drop the finalizer, and
// never the other way round, because after the finalizer is gone the object
// is unreachable and the work would never happen.
func finalize(ctx context.Context, clientset kubernetes.Interface, dyn dynamic.Interface, site *unstructured.Unstructured) error {
	ns, name := site.GetNamespace(), site.GetName()
	if !slices.Contains(site.GetFinalizers(), finalizerName) {
		return nil
	}

	if err := clientset.CoreV1().Services(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete service: %w", err)
	}
	fmt.Printf("cleaned up %s/%s\n", ns, name)

	client := dyn.Resource(websiteGVR).Namespace(ns)
	latest, err := client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("read website: %w", err)
	}
	remaining := slices.DeleteFunc(latest.GetFinalizers(), func(f string) bool { return f == finalizerName })
	latest.SetFinalizers(remaining)
	if _, err := client.Update(ctx, latest, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("remove finalizer: %w", err)
	}
	return nil
}

// serveMetrics publishes what the controller has done, in the text format a
// scrape job expects.
//
// The TYPE line is not decoration: it is how a scraper tells a counter, which
// only ever goes up and is read as a rate, from a gauge, which is read as it
// stands.
func serveMetrics(ctx context.Context, addr string) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "# HELP byok8s_reconcile_total Reconciles started since this process began.\n")
		fmt.Fprint(w, "# TYPE byok8s_reconcile_total counter\n")
		fmt.Fprintf(w, "byok8s_reconcile_total %d\n", reconcileTotal.Load())
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		// The endpoint belongs to the process: when it goes, it goes.
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "metrics endpoint: %v\n", err)
		}
	}()
}
