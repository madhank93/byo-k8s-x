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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	appsv1client "k8s.io/client-go/kubernetes/typed/apps/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
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
		for processNext(ctx, queue, lister, clientset) {
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
func processNext(ctx context.Context, queue workqueue.TypedRateLimitingInterface[string], lister cache.GenericLister, clientset kubernetes.Interface) bool {
	key, shutdown := queue.Get()
	if shutdown {
		return false
	}
	defer queue.Done(key)

	if err := reconcile(ctx, lister, clientset, key); err != nil {
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
func reconcile(ctx context.Context, lister cache.GenericLister, clientset kubernetes.Interface, key string) error {
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
	return ensureService(ctx, clientset, site)
}

// ensureService gives the Website an address inside the cluster.
//
// A Website is one wish that means several objects, and each one is reconciled
// the same way: what should exist, what does exist, close the gap. The Service
// selects the same labels the Deployment stamps on its pods — that shared
// label set is the only thing connecting them.
func ensureService(ctx context.Context, clientset kubernetes.Interface, site *unstructured.Unstructured) error {
	ns, name := site.GetNamespace(), site.GetName()
	services := clientset.CoreV1().Services(ns)
	if _, err := services.Get(ctx, name, metav1.GetOptions{}); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get service: %w", err)
	}

	labels := childLabels(name)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ns,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(site)},
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       80,
				TargetPort: intstr.FromInt32(80),
			}},
		},
	}
	if _, err := services.Create(ctx, svc, metav1.CreateOptions{}); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("create service: %w", err)
	}
	fmt.Printf("created service %s/%s\n", ns, name)
	return nil
}

// ensureDeployment makes the Deployment the Website asks for exist.
//
// The Website is the wish; the Deployment is the thing that grants it. Every
// controller past this point is a variation on the same sentence: read the
// wish, look at what is there, make the difference go away.
func ensureDeployment(ctx context.Context, clientset kubernetes.Interface, site *unstructured.Unstructured, image string, replicas int32) error {
	ns, name := site.GetNamespace(), site.GetName()
	deployments := clientset.AppsV1().Deployments(ns)
	existing, err := deployments.Get(ctx, name, metav1.GetOptions{})
	switch {
	case err == nil:
		if err := adopt(ctx, deployments, existing, site); err != nil {
			return err
		}
		return repair(ctx, deployments, existing, image, replicas)
	case !apierrors.IsNotFound(err):
		return fmt.Errorf("get deployment: %w", err)
	}
	if _, err := deployments.Create(ctx, deploymentFor(site, image, replicas), metav1.CreateOptions{}); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("create deployment: %w", err)
	}
	fmt.Printf("created deployment %s/%s\n", ns, name)
	return nil
}

// repair puts back what drifted.
//
// The Website's spec is the intent; anything else in the Deployment is an
// accident — a hand-run kubectl scale, a half-finished migration, another
// tool. A controller does not ask how the difference appeared. It reads what
// should be true, sees what is true, and closes the gap.
func repair(ctx context.Context, deployments appsv1client.DeploymentInterface, existing *appsv1.Deployment, image string, replicas int32) error {
	sameReplicas := existing.Spec.Replicas != nil && *existing.Spec.Replicas == replicas
	sameImage := len(existing.Spec.Template.Spec.Containers) > 0 &&
		existing.Spec.Template.Spec.Containers[0].Image == image
	if sameReplicas && sameImage {
		return nil
	}
	// A patch, not a read-modify-write update: the Deployment's status is
	// rewritten constantly by the controllers behind it, so an update built on
	// the copy just read loses the race often enough to matter. A patch says
	// what to change and leaves everything else alone.
	// The container is matched by name, because a strategic merge patch merges
	// list entries by their merge key rather than replacing the whole list.
	patch := fmt.Sprintf(
		`{"spec":{"replicas":%d,"template":{"spec":{"containers":[{"name":"web","image":%q}]}}}}`,
		replicas, image)
	if _, err := deployments.Patch(ctx, existing.Name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("repair deployment: %w", err)
	}
	fmt.Printf("repaired deployment %s/%s image=%s replicas=%d\n", existing.Namespace, existing.Name, image, replicas)
	return nil
}

// adopt claims a Deployment that already carries the right name but nobody's
// ownership — the state you get when someone created it by hand, or when an
// earlier version of this controller did not set owner references.
//
// Creating a second one is not an option: the name is taken. Failing is not
// either, because the cluster would then need a human before it could
// converge. Adopting is the only outcome that leaves the cluster correct.
func adopt(ctx context.Context, deployments appsv1client.DeploymentInterface, existing *appsv1.Deployment, site *unstructured.Unstructured) error {
	if metav1.GetControllerOf(existing) != nil {
		return nil
	}
	updated := existing.DeepCopy()
	updated.OwnerReferences = append(updated.OwnerReferences, ownerRef(site))
	if _, err := deployments.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("adopt deployment: %w", err)
	}
	fmt.Printf("adopted deployment %s/%s\n", existing.Namespace, existing.Name)
	return nil
}

// deploymentFor is the Deployment a Website means. The labels are the join
// between the two, and the Service added later selects on the same ones.
func deploymentFor(site *unstructured.Unstructured, image string, replicas int32) *appsv1.Deployment {
	ns, name := site.GetNamespace(), site.GetName()
	labels := childLabels(name)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ns,
			Labels:          siteLabels(site),
			OwnerReferences: []metav1.OwnerReference{ownerRef(site)},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "web",
						Image: image,
						Ports: []corev1.ContainerPort{{ContainerPort: 80}},
					}},
				},
			},
		},
	}
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

// ownerRef is what makes the Deployment belong to the Website.
//
// Garbage collection is the API server's job, not the controller's: an owned
// object whose owner is gone is deleted for you. Controller: true also marks
// which owner is in charge, so two controllers cannot both claim the same
// child and fight over it.
func ownerRef(site *unstructured.Unstructured) metav1.OwnerReference {
	yes := true
	return metav1.OwnerReference{
		APIVersion:         group + "/" + version,
		Kind:               kind,
		Name:               site.GetName(),
		UID:                site.GetUID(),
		Controller:         &yes,
		BlockOwnerDeletion: &yes,
	}
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
