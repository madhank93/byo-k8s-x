// Package controller holds the assertions for the "Build your own controller"
// course — one function per stage, registered by slug.
//
// A controller is a long-running process, so most stages here do not simply
// run the binary and read its output: they start the program, change the
// cluster underneath it, wait for the cluster to converge, and only then
// judge. Nothing inspects the learner's source — any implementation that
// converges is a correct one.
package controller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	typedappsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	"k8s.io/client-go/util/retry"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// StageTimeout bounds one stage.
const StageTimeout = stages.Timeout

// Stage is one gradable step, in the shape cmd/tester dispatches on.
type Stage = stages.Stage

// The resource the course builds a controller for. The assertions know these
// names because the course specifies them; the learner's program is free to
// reach them any way it likes.
const (
	group   = "byok8s.dev"
	version = "v1alpha1"
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

var registry = map[string]Stage{}

func register(s Stage) { registry[s.Slug] = s }

// Lookup returns the stage with this slug.
func Lookup(slug string) (Stage, bool) {
	s, ok := registry[slug]
	return s, ok
}

func init() {
	register(Stage{Slug: "install-crd", Run: stageInstallCRD})
	register(Stage{Slug: "crd-schema", Run: stageCRDSchema})
	register(Stage{Slug: "list-then-watch", Run: stageListThenWatch})
	register(Stage{Slug: "shared-informer", Run: stageSharedInformer})
	register(Stage{Slug: "lister", Run: stageLister})
	register(Stage{Slug: "workqueue", Run: stageWorkqueue})
	register(Stage{Slug: "reconcile", Run: stageReconcile})
	register(Stage{Slug: "create-child", Run: stageCreateChild})
	register(Stage{Slug: "owner-references", Run: stageOwnerReferences})
	register(Stage{Slug: "adopt-existing", Run: stageAdoptExisting})
	register(Stage{Slug: "repair-drift", Run: stageRepairDrift})
	register(Stage{Slug: "update-spec", Run: stageUpdateSpec})
	register(Stage{Slug: "second-child", Run: stageSecondChild})
	register(Stage{Slug: "requeue-on-error", Run: stageRequeueOnError})
	register(Stage{Slug: "requeue-after", Run: stageRequeueAfter})
	register(Stage{Slug: "paused-annotation", Run: stagePausedAnnotation})
	register(Stage{Slug: "status-subresource", Run: stageStatusSubresource})
	register(Stage{Slug: "conditions", Run: stageConditions})
	register(Stage{Slug: "finalizer", Run: stageFinalizer})
	register(Stage{Slug: "conflict-retry", Run: stageConflictRetry})
	register(Stage{Slug: "server-side-apply", Run: stageServerSideApply})
	register(Stage{Slug: "secondary-watch", Run: stageSecondaryWatch})
	register(Stage{Slug: "foreign-children", Run: stageForeignChildren})
	register(Stage{Slug: "events", Run: stageEvents})
	register(Stage{Slug: "metrics", Run: stageMetrics})
	register(Stage{Slug: "health-probes", Run: stageHealthProbes})
	register(Stage{Slug: "leader-election", Run: stageLeaderElection})
	register(Stage{Slug: "graceful-shutdown", Run: stageGracefulShutdown})
}

// stageInstallCRD — the program defines websites.byok8s.dev and does not
// return until the API server serves it.
func stageInstallCRD(ctx context.Context, env *kube.Env, bin string) error {
	if err := removeCRD(ctx, env); err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, crdName, 60*time.Second); err != nil {
		return fmt.Errorf("waiting for the program to report %s: %w", crdName, err)
	}

	got, err := getCRD(ctx, env)
	if err != nil {
		return fmt.Errorf("the program says %s is ready but it does not exist: %w", crdName, err)
	}
	if !established(got) {
		return fmt.Errorf("%s exists but is not Established — the program announced it before the API server was serving it", crdName)
	}
	scope, _, _ := unstructured.NestedString(got.Object, "spec", "scope")
	if scope != "Namespaced" {
		return fmt.Errorf("expected a Namespaced resource, got scope %q", scope)
	}
	if g, _, _ := unstructured.NestedString(got.Object, "spec", "group"); g != group {
		return fmt.Errorf("expected group %q, got %q", group, g)
	}
	return nil
}

// stageCRDSchema — the definition types its fields, so the API server rejects
// a Website that does not fit and fills in what was left out.
func stageCRDSchema(ctx context.Context, env *kube.Env, bin string) error {
	if err := removeCRD(ctx, env); err != nil {
		return err
	}
	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, crdName, 60*time.Second); err != nil {
		return fmt.Errorf("waiting for the program to report %s: %w", crdName, err)
	}

	crd, err := getCRD(ctx, env)
	if err != nil {
		return err
	}
	if cols, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions"); len(cols) > 0 {
		v, _ := cols[0].(map[string]any)
		if printer, ok, _ := unstructured.NestedSlice(v, "additionalPrinterColumns"); !ok || len(printer) == 0 {
			return fmt.Errorf("the definition has no additionalPrinterColumns, so `kubectl get websites` shows nothing but names and ages")
		}
	}

	c, err := dyn(env)
	if err != nil {
		return err
	}
	websites := c.Resource(websiteGVR).Namespace(env.Namespace)

	// A schema earns its keep by refusing what does not fit.
	bad := website("bad", map[string]any{"image": "nginx", "replicas": "two"})
	if _, err := websites.Create(ctx, bad, metav1.CreateOptions{}); err == nil {
		return fmt.Errorf("the API server accepted replicas: \"two\" — the schema does not type the field")
	} else if !apierrors.IsInvalid(err) && !apierrors.IsBadRequest(err) {
		return fmt.Errorf("expected the API server to reject replicas: \"two\" as invalid, got: %w", err)
	}
	missing := website("no-image", map[string]any{"replicas": int64(1)})
	if _, err := websites.Create(ctx, missing, metav1.CreateOptions{}); err == nil {
		return fmt.Errorf("the API server accepted a Website with no image — the schema does not require it")
	}

	// And by filling in what was left out.
	created, err := websites.Create(ctx, website("defaulted", map[string]any{"image": "nginx"}), metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("creating a valid Website: %w", err)
	}
	replicas, found, err := unstructured.NestedInt64(created.Object, "spec", "replicas")
	if err != nil || !found {
		return fmt.Errorf("spec.replicas was not defaulted — a Website with no replicas came back without the field")
	}
	if replicas != 1 {
		return fmt.Errorf("expected spec.replicas to default to 1, got %d", replicas)
	}
	return nil
}

// stageListThenWatch — the program reports what already exists and then every
// change, with no object reported twice at the seam between the two.
func stageListThenWatch(ctx context.Context, env *kube.Env, bin string) error {
	// One Website exists before the program starts: it must be reported from
	// the list, not missed because nothing changed after startup.
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}
	if _, err := websites.Create(ctx, website("seeded", map[string]any{"image": "nginx"}), metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seeding a Website: %w", err)
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "add seeded", 60*time.Second); err != nil {
		return fmt.Errorf("the Website that existed before startup was never reported: %w", err)
	}

	if _, err := websites.Create(ctx, website("later", map[string]any{"image": "nginx"}), metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating a Website: %w", err)
	}
	if err := awaitLine(ctx, p, "add later", 30*time.Second); err != nil {
		return fmt.Errorf("a Website created while watching was never reported: %w", err)
	}

	if err := updateWebsite(ctx, websites, "later", func(site *unstructured.Unstructured) error {
		return unstructured.SetNestedField(site.Object, "nginx:1.27", "spec", "image")
	}); err != nil {
		return fmt.Errorf("updating a Website: %w", err)
	}
	if err := awaitLine(ctx, p, "update later", 30*time.Second); err != nil {
		return fmt.Errorf("a change to a Website was never reported: %w", err)
	}

	if err := websites.Delete(ctx, "later", metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("deleting a Website: %w", err)
	}
	if err := awaitLine(ctx, p, "delete later", 30*time.Second); err != nil {
		return fmt.Errorf("a deleted Website was never reported: %w", err)
	}

	// The list and the watch are one operation. Reporting the seeded Website
	// twice means the watch started from scratch instead of from the list.
	if n := countLines(p.Stdout(), "add seeded"); n != 1 {
		return fmt.Errorf("expected the pre-existing Website to be reported once, got %d times:\n%s", n, tail(p.Stdout()))
	}
	return nil
}

// stageSharedInformer — the program keeps a cache and says when it is warm,
// which is the moment it becomes safe to act on what it holds.
func stageSharedInformer(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}
	for _, name := range []string{"one", "two"} {
		if _, err := websites.Create(ctx, website(name, map[string]any{"image": "nginx"}), metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("seeding a Website: %w", err)
		}
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return fmt.Errorf("the program never reported its cache as synced: %w", err)
	}
	if want := "synced 2"; !strings.Contains(p.Stdout(), want) {
		return fmt.Errorf("expected %q — two Websites existed when the cache warmed — got:\n%s", want, tail(p.Stdout()))
	}
	for _, name := range []string{"add one", "add two"} {
		if !strings.Contains(p.Stdout(), name) {
			return fmt.Errorf("expected %q in the output, got:\n%s", name, tail(p.Stdout()))
		}
	}

	// The handlers must keep firing after the sync, not only during it.
	if _, err := websites.Create(ctx, website("three", map[string]any{"image": "nginx"}), metav1.CreateOptions{}); err != nil {
		return err
	}
	if err := awaitLine(ctx, p, "add three", 30*time.Second); err != nil {
		return fmt.Errorf("a Website created after the sync was never reported: %w", err)
	}
	return nil
}

// stageLister — the program answers "what is there" from its own cache, and
// the answer tracks every change.
func stageLister(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}
	if _, err := websites.Create(ctx, website("first", map[string]any{"image": "nginx"}), metav1.CreateOptions{}); err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	for _, name := range []string{"second", "third"} {
		if _, err := websites.Create(ctx, website(name, map[string]any{"image": "nginx"}), metav1.CreateOptions{}); err != nil {
			return err
		}
	}
	if err := awaitLine(ctx, p, "cache 3", 30*time.Second); err != nil {
		return fmt.Errorf("after three Websites the cache should hold three: %w", err)
	}
	if err := websites.Delete(ctx, "third", metav1.DeleteOptions{}); err != nil {
		return err
	}
	if err := awaitLine(ctx, p, "cache 2", 30*time.Second); err != nil {
		return fmt.Errorf("after a delete the cache should hold two: %w", err)
	}
	return nil
}

// stageWorkqueue — events become keys on a queue, and a burst of changes to
// one object costs fewer passes than it produced events.
func stageWorkqueue(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	if _, err := websites.Create(ctx, website("burst", map[string]any{"image": "nginx"}), metav1.CreateOptions{}); err != nil {
		return err
	}
	key := env.Namespace + "/burst"
	if err := awaitLine(ctx, p, "reconcile "+key, 30*time.Second); err != nil {
		return fmt.Errorf("expected a pass over %q — the queue carries namespace/name keys, not objects: %w", key, err)
	}

	// Five changes in a row. The queue may collapse them; it must never
	// invent work that was not queued.
	updates := 5
	for i := 0; i < updates; i++ {
		image := fmt.Sprintf("nginx:1.%d", i)
		if err := updateWebsite(ctx, websites, "burst", func(site *unstructured.Unstructured) error {
			return unstructured.SetNestedField(site.Object, image, "spec", "image")
		}); err != nil {
			return fmt.Errorf("update %d: %w", i, err)
		}
	}
	if err := awaitLine(ctx, p, "update burst", 30*time.Second); err != nil {
		return err
	}
	// Let the queue settle before counting.
	time.Sleep(3 * time.Second)

	out := p.Stdout()
	events := countLines(out, "add burst") + countLines(out, "update burst")
	retries := countPrefixed(out, "retry "+key)
	passes := countPrefixed(out, "reconcile "+key)
	switch {
	case passes == 0:
		return fmt.Errorf("no pass over %q at all:\n%s", key, tail(out))
	case passes > events+retries:
		return fmt.Errorf("%d passes for %d events over %q — a queue collapses work, it does not invent it:\n%s",
			passes, events, key, tail(out))
	}
	return nil
}

// stageReconcile — a pass reads the object as it is now, and handles the case
// where "as it is now" means gone.
func stageReconcile(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	if _, err := websites.Create(ctx, website("shop", map[string]any{"image": "nginx", "replicas": int64(3)}), metav1.CreateOptions{}); err != nil {
		return err
	}
	key := env.Namespace + "/shop"
	if err := awaitLine(ctx, p, "reconcile "+key+" image=nginx replicas=3", 30*time.Second); err != nil {
		return fmt.Errorf("a pass should report the spec it read for %s: %w", key, err)
	}

	// Changing the spec and reconciling again must report the new value: the
	// pass reads the object, it does not remember the event.
	if err := updateWebsite(ctx, websites, "shop", func(site *unstructured.Unstructured) error {
		return unstructured.SetNestedField(site.Object, int64(5), "spec", "replicas")
	}); err != nil {
		return err
	}
	if err := awaitLine(ctx, p, "reconcile "+key+" image=nginx replicas=5", 30*time.Second); err != nil {
		return fmt.Errorf("after a spec change the pass should read the new spec: %w", err)
	}

	if err := websites.Delete(ctx, "shop", metav1.DeleteOptions{}); err != nil {
		return err
	}
	if err := awaitLine(ctx, p, "reconcile "+key+" gone", 30*time.Second); err != nil {
		return fmt.Errorf("a deleted object still gets a pass, and the pass must survive it: %w", err)
	}
	if done, res := p.Exited(); done {
		return fmt.Errorf("the program exited (%d) when its object was deleted:\n%s", res.ExitCode, tail(res.Stderr))
	}
	return nil
}

// stageCreateChild — a Website becomes a Deployment that matches its spec.
func stageCreateChild(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	spec := map[string]any{"image": "registry.k8s.io/pause:3.9", "replicas": int64(2)}
	if _, err := websites.Create(ctx, website("blog", spec), metav1.CreateOptions{}); err != nil {
		return err
	}

	dep, err := awaitDeployment(ctx, env, "blog", 60*time.Second)
	if err != nil {
		return fmt.Errorf("a Website with no Deployment is a wish nobody granted: %w", err)
	}
	if got := *dep.Spec.Replicas; got != 2 {
		return fmt.Errorf("expected the Deployment to ask for 2 replicas, got %d", got)
	}
	if got := dep.Spec.Template.Spec.Containers[0].Image; got != spec["image"] {
		return fmt.Errorf("expected image %q, got %q", spec["image"], got)
	}
	if len(dep.Spec.Selector.MatchLabels) == 0 {
		return fmt.Errorf("the Deployment has no selector labels — the Service added later has nothing to select on")
	}
	return nil
}

// stageOwnerReferences — the Deployment belongs to the Website, so deleting
// the Website takes the Deployment with it and the controller writes no
// deletion code at all.
func stageOwnerReferences(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	site, err := websites.Create(ctx, website("owned", map[string]any{"image": "registry.k8s.io/pause:3.9"}), metav1.CreateOptions{})
	if err != nil {
		return err
	}
	dep, err := awaitDeployment(ctx, env, "owned", 60*time.Second)
	if err != nil {
		return err
	}

	refs := dep.GetOwnerReferences()
	if len(refs) == 0 {
		return fmt.Errorf("the Deployment has no ownerReferences, so nothing ties it to the Website that asked for it")
	}
	ref := refs[0]
	switch {
	case ref.UID != site.GetUID():
		return fmt.Errorf("the ownerReference points at UID %q, but the Website is %q — a name is not enough, a recreated object is a different owner", ref.UID, site.GetUID())
	case ref.Kind != "Website":
		return fmt.Errorf("expected the owner to be a Website, got %q", ref.Kind)
	case ref.Controller == nil || !*ref.Controller:
		return fmt.Errorf("the ownerReference is not marked controller: true, so nothing says which owner is in charge of this child")
	}

	if err := websites.Delete(ctx, "owned", metav1.DeleteOptions{}); err != nil {
		return err
	}
	return waitFor(ctx, "the Deployment to be collected with its Website", 90*time.Second, func(ctx context.Context) (bool, error) {
		_, err := env.Client.AppsV1().Deployments(env.Namespace).Get(ctx, "owned", metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	})
}

// stageAdoptExisting — a Deployment that is already there under the right name
// is claimed, not duplicated and not treated as an error.
func stageAdoptExisting(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	// Someone got here first: the Deployment exists and belongs to nobody.
	if err := seedUnowned(ctx, env, "legacy"); err != nil {
		return fmt.Errorf("seeding an unowned Deployment: %w", err)
	}
	before, err := env.Client.AppsV1().Deployments(env.Namespace).Get(ctx, "legacy", metav1.GetOptions{})
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	site, err := websites.Create(ctx, website("legacy", map[string]any{"image": "registry.k8s.io/pause:3.9"}), metav1.CreateOptions{})
	if err != nil {
		return err
	}

	var after *appsv1.Deployment
	if err := waitFor(ctx, "the existing Deployment to be adopted", 60*time.Second, func(ctx context.Context) (bool, error) {
		got, err := env.Client.AppsV1().Deployments(env.Namespace).Get(ctx, "legacy", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		after = got
		return len(got.GetOwnerReferences()) > 0, nil
	}); err != nil {
		if done, res := p.Exited(); done {
			return fmt.Errorf("the program exited (%d) rather than adopting what was already there:\n%s", res.ExitCode, tail(res.Stderr))
		}
		return err
	}
	if after.UID != before.UID {
		return fmt.Errorf("the Deployment was replaced (UID %q became %q) — adopting means keeping the object, not recreating it", before.UID, after.UID)
	}
	if ref := after.GetOwnerReferences()[0]; ref.UID != site.GetUID() {
		return fmt.Errorf("the adopted Deployment points at UID %q, not the Website's %q", ref.UID, site.GetUID())
	}
	return nil
}

// stageRepairDrift — someone scales the Deployment by hand; the next pass puts
// it back, because the Website's spec is the only intent that counts.
func stageRepairDrift(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	if _, err := websites.Create(ctx, website("drift", map[string]any{"image": "registry.k8s.io/pause:3.9", "replicas": int64(2)}), metav1.CreateOptions{}); err != nil {
		return err
	}
	if _, err := awaitDeployment(ctx, env, "drift", 60*time.Second); err != nil {
		return err
	}

	// Scale it away behind the controller's back.
	deployments := env.Client.AppsV1().Deployments(env.Namespace)
	if err := scaleTo(ctx, deployments, "drift", 0); err != nil {
		return fmt.Errorf("scaling the Deployment by hand: %w", err)
	}

	// Nudge the Website so it is reconciled again. Noticing drift without a
	// nudge needs a timer or a watch on the child, which are later stages.
	if err := touchWebsite(ctx, websites, "drift"); err != nil {
		return err
	}

	if err := waitFor(ctx, "the Deployment to be scaled back to 2", 60*time.Second, func(ctx context.Context) (bool, error) {
		got, err := deployments.Get(ctx, "drift", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return got.Spec.Replicas != nil && *got.Spec.Replicas == 2, nil
	}); err != nil {
		return fmt.Errorf("%w\nthe program said:\n%s\n%s", err, tail(p.Stdout()), tail(p.Stderr()))
	}
	return nil
}

// stageUpdateSpec — editing the Website rolls the change out to the
// Deployment, which is the same repair loop pointed at a different difference.
func stageUpdateSpec(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	const (
		before = "registry.k8s.io/pause:3.9"
		after  = "registry.k8s.io/pause:3.10"
	)
	if _, err := websites.Create(ctx, website("rollout", map[string]any{"image": before, "replicas": int64(1)}), metav1.CreateOptions{}); err != nil {
		return err
	}
	if _, err := awaitDeployment(ctx, env, "rollout", 60*time.Second); err != nil {
		return err
	}

	if err := updateWebsite(ctx, websites, "rollout", func(site *unstructured.Unstructured) error {
		if err := unstructured.SetNestedField(site.Object, after, "spec", "image"); err != nil {
			return err
		}
		return unstructured.SetNestedField(site.Object, int64(3), "spec", "replicas")
	}); err != nil {
		return err
	}

	deployments := env.Client.AppsV1().Deployments(env.Namespace)
	if err := waitFor(ctx, "the Deployment to follow the Website's new spec", 60*time.Second, func(ctx context.Context) (bool, error) {
		got, err := deployments.Get(ctx, "rollout", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		if len(got.Spec.Template.Spec.Containers) == 0 {
			return false, nil
		}
		return got.Spec.Template.Spec.Containers[0].Image == after &&
			got.Spec.Replicas != nil && *got.Spec.Replicas == 3, nil
	}); err != nil {
		return fmt.Errorf("%w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	return nil
}

// stageSecondChild — one Website means two objects, and they agree with each
// other: the Service selects exactly the pods the Deployment makes.
func stageSecondChild(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	if _, err := websites.Create(ctx, website("served", map[string]any{"image": "registry.k8s.io/pause:3.9"}), metav1.CreateOptions{}); err != nil {
		return err
	}
	dep, err := awaitDeployment(ctx, env, "served", 60*time.Second)
	if err != nil {
		return err
	}

	var svc *corev1.Service
	if err := waitFor(ctx, "the Service", 60*time.Second, func(ctx context.Context) (bool, error) {
		got, err := env.Client.CoreV1().Services(env.Namespace).Get(ctx, "served", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		svc = got
		return true, nil
	}); err != nil {
		return fmt.Errorf("%w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	if len(svc.Spec.Ports) == 0 {
		return fmt.Errorf("the Service exposes no ports")
	}
	if len(svc.Spec.Selector) == 0 {
		return fmt.Errorf("the Service has no selector, so it will never have an endpoint")
	}
	pods := dep.Spec.Template.Labels
	for k, v := range svc.Spec.Selector {
		if pods[k] != v {
			return fmt.Errorf("the Service selects %s=%s but the Deployment's pods are labelled %v — the Service will never find them", k, v, pods)
		}
	}
	if len(svc.GetOwnerReferences()) == 0 {
		return fmt.Errorf("the Service has no ownerReference, so deleting the Website would leave it behind")
	}
	return nil
}

// stageRequeueOnError — a spec the cluster refuses is not fatal: the pass
// fails, the key goes back on the queue, and the controller converges as soon
// as the spec is fixed.
func stageRequeueOnError(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	// A host too long to be a label value: the API server will reject every
	// child built from it.
	tooLong := strings.Repeat("a", 70)
	if _, err := websites.Create(ctx, website("flaky", map[string]any{
		"image": "registry.k8s.io/pause:3.9",
		"host":  tooLong,
	}), metav1.CreateOptions{}); err != nil {
		return err
	}

	key := env.Namespace + "/flaky"
	if err := awaitLine(ctx, p, "retry "+key, 60*time.Second); err != nil {
		return fmt.Errorf("a failed pass must be retried, not dropped: %w", err)
	}
	if done, res := p.Exited(); done {
		return fmt.Errorf("the program exited (%d) because one Website could not be reconciled — one bad object must not stop the loop:\n%s",
			res.ExitCode, tail(res.Stderr))
	}

	// Retries must back off rather than spin: a hot loop would produce
	// hundreds of attempts in the time this takes.
	time.Sleep(5 * time.Second)
	if attempts := countPrefixed(p.Stdout(), "retry "+key); attempts > 40 {
		return fmt.Errorf("%d retries in a few seconds — failures must back off, not spin", attempts)
	}

	// Fix the Website: the same key must now succeed.
	if err := updateWebsite(ctx, websites, "flaky", func(site *unstructured.Unstructured) error {
		return unstructured.SetNestedField(site.Object, "example.com", "spec", "host")
	}); err != nil {
		return err
	}
	if _, err := awaitDeployment(ctx, env, "flaky", 60*time.Second); err != nil {
		return fmt.Errorf("once the spec was fixed the controller should converge: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	return nil
}

// stageRequeueAfter — drift in a child produces no event about the parent, so
// the controller comes back on its own and finds it.
func stageRequeueAfter(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	if _, err := websites.Create(ctx, website("timer", map[string]any{"image": "registry.k8s.io/pause:3.9", "replicas": int64(2)}), metav1.CreateOptions{}); err != nil {
		return err
	}
	if _, err := awaitDeployment(ctx, env, "timer", 60*time.Second); err != nil {
		return err
	}

	// Change the child and nothing else. No Website event will follow.
	deployments := env.Client.AppsV1().Deployments(env.Namespace)
	if err := scaleTo(ctx, deployments, "timer", 0); err != nil {
		return err
	}

	if err := waitFor(ctx, "the controller to notice the drift by itself", 75*time.Second, func(ctx context.Context) (bool, error) {
		got, err := deployments.Get(ctx, "timer", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return got.Spec.Replicas != nil && *got.Spec.Replicas == 2, nil
	}); err != nil {
		return fmt.Errorf("%w\nnothing touched the Website, so only a timer could have caught this\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	return nil
}

// stagePausedAnnotation — an annotation takes one Website out of the
// controller's hands, and giving it back resumes exactly where it left off.
func stagePausedAnnotation(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	if _, err := websites.Create(ctx, website("held", map[string]any{"image": "registry.k8s.io/pause:3.9", "replicas": int64(2)}), metav1.CreateOptions{}); err != nil {
		return err
	}
	if _, err := awaitDeployment(ctx, env, "held", 60*time.Second); err != nil {
		return err
	}

	if err := updateWebsite(ctx, websites, "held", func(site *unstructured.Unstructured) error {
		site.SetAnnotations(map[string]string{"byok8s.dev/paused": "true"})
		return nil
	}); err != nil {
		return err
	}
	key := env.Namespace + "/held"
	if err := awaitLine(ctx, p, "paused "+key, 30*time.Second); err != nil {
		return fmt.Errorf("a paused Website should say so rather than go quiet: %w", err)
	}

	// Break the child. A paused Website must be left broken.
	deployments := env.Client.AppsV1().Deployments(env.Namespace)
	if err := scaleTo(ctx, deployments, "held", 0); err != nil {
		return err
	}
	time.Sleep(20 * time.Second) // two turns of the resync timer
	held, err := deployments.Get(ctx, "held", metav1.GetOptions{})
	if err != nil {
		return err
	}
	if held.Spec.Replicas == nil || *held.Spec.Replicas != 0 {
		return fmt.Errorf("the Deployment was repaired while its Website was paused — pausing has to mean hands off")
	}

	// Hand it back.
	if err := updateWebsite(ctx, websites, "held", func(site *unstructured.Unstructured) error {
		site.SetAnnotations(map[string]string{})
		return nil
	}); err != nil {
		return err
	}
	if err := waitFor(ctx, "the controller to resume", 60*time.Second, func(ctx context.Context) (bool, error) {
		got, err := deployments.Get(ctx, "held", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return got.Spec.Replicas != nil && *got.Spec.Replicas == 2, nil
	}); err != nil {
		return fmt.Errorf("%w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	return nil
}

// stageStatusSubresource — the controller reports what it achieved, on the
// half of the object that belongs to it.
func stageStatusSubresource(ctx context.Context, env *kube.Env, bin string) error {
	if err := removeCRD(ctx, env); err != nil {
		return err
	}
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}

	crd, err := getCRD(ctx, env)
	if err != nil {
		return err
	}
	versions, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions")
	if len(versions) == 0 {
		return fmt.Errorf("the definition has no versions")
	}
	v, _ := versions[0].(map[string]any)
	if _, ok, _ := unstructured.NestedMap(v, "subresources", "status"); !ok {
		return fmt.Errorf("the definition has no status subresource, so a client that writes spec can still overwrite status")
	}

	websites, err := websiteClient(env)
	if err != nil {
		return err
	}
	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	created, err := websites.Create(ctx, website("reported", map[string]any{"image": "registry.k8s.io/pause:3.9"}), metav1.CreateOptions{})
	if err != nil {
		return err
	}
	generation := created.GetGeneration()

	if err := waitFor(ctx, "status.observedGeneration to catch up", 60*time.Second, func(ctx context.Context) (bool, error) {
		got, err := websites.Get(ctx, "reported", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		seen, found, _ := unstructured.NestedInt64(got.Object, "status", "observedGeneration")
		return found && seen == generation, nil
	}); err != nil {
		return fmt.Errorf("%w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	got, err := websites.Get(ctx, "reported", metav1.GetOptions{})
	if err != nil {
		return err
	}
	if _, found, _ := unstructured.NestedInt64(got.Object, "status", "replicas"); !found {
		return fmt.Errorf("status.replicas was never written, so nothing says how many are actually running")
	}
	// Writing status must not look like a spec change, or every status write
	// would queue another pass and the loop would never settle.
	if got.GetGeneration() != generation {
		return fmt.Errorf("generation went from %d to %d — writing status must not touch spec", generation, got.GetGeneration())
	}
	return nil
}

// stageConditions — status says yes or no, and says why.
func stageConditions(ctx context.Context, env *kube.Env, bin string) error {
	if err := removeCRD(ctx, env); err != nil {
		return err
	}
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	if _, err := websites.Create(ctx, website("ready", map[string]any{"image": "registry.k8s.io/pause:3.9", "replicas": int64(1)}), metav1.CreateOptions{}); err != nil {
		return err
	}

	// The condition must appear long before it is true: "not ready yet, and
	// here is why" is the whole point of reporting one.
	var first map[string]any
	if err := waitFor(ctx, "a Ready condition to appear", 60*time.Second, func(ctx context.Context) (bool, error) {
		got, err := websites.Get(ctx, "ready", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		first = condition(got, "Ready")
		return first != nil, nil
	}); err != nil {
		return fmt.Errorf("%w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	for _, field := range []string{"status", "reason", "lastTransitionTime"} {
		if v, _ := first[field].(string); v == "" {
			return fmt.Errorf("the Ready condition has no %s — a condition without one is not a condition:\n%v", field, first)
		}
	}

	if err := waitFor(ctx, "the Ready condition to become True", 150*time.Second, func(ctx context.Context) (bool, error) {
		got, err := websites.Get(ctx, "ready", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		cond := condition(got, "Ready")
		return cond != nil && cond["status"] == "True", nil
	}); err != nil {
		return fmt.Errorf("the Deployment became available but Ready never followed: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	return nil
}

// stageFinalizer — the controller holds a deleted Website open long enough to
// clean up after it, and then lets go.
func stageFinalizer(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	// The program must be running for the finalizer to be released, so this
	// stage stops it by hand in the middle and restarts it.
	stopped := false
	defer func() {
		if !stopped {
			cleanup()
		}
	}()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	if _, err := websites.Create(ctx, website("held-open", map[string]any{"image": "registry.k8s.io/pause:3.9"}), metav1.CreateOptions{}); err != nil {
		return err
	}
	if err := waitFor(ctx, "the finalizer to be added", 60*time.Second, func(ctx context.Context) (bool, error) {
		got, err := websites.Get(ctx, "held-open", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return len(got.GetFinalizers()) > 0, nil
	}); err != nil {
		return fmt.Errorf("%w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	if _, err := awaitDeployment(ctx, env, "held-open", 60*time.Second); err != nil {
		return err
	}

	// With the controller down, a delete must not complete: that is what a
	// finalizer is for.
	cleanup()
	stopped = true
	if err := websites.Delete(ctx, "held-open", metav1.DeleteOptions{}); err != nil {
		return err
	}
	time.Sleep(10 * time.Second)
	pending, err := websites.Get(ctx, "held-open", metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("the Website was deleted while the controller was down — its finalizer did not hold: %w", err)
	}
	if pending.GetDeletionTimestamp() == nil {
		return fmt.Errorf("the Website has no deletionTimestamp after being deleted")
	}

	// Bring it back: it must finish the cleanup and release.
	p2, cleanup2, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup2()
	if err := awaitLine(ctx, p2, "cleaned up "+env.Namespace+"/held-open", 90*time.Second); err != nil {
		return fmt.Errorf("the controller came back to a deleted Website and did not clean up after it: %w", err)
	}
	if err := waitFor(ctx, "the Website to finish deleting", 60*time.Second, func(ctx context.Context) (bool, error) {
		_, err := websites.Get(ctx, "held-open", metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	}); err != nil {
		return fmt.Errorf("the cleanup ran but the finalizer was never removed, so the object is stuck forever: %w\nthe program said:\n%s", err, tail(p2.Stdout()))
	}
	return nil
}

// stageConflictRetry — another writer edits the Website while the controller is
// writing status, and the controller still converges.
func stageConflictRetry(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	if _, err := websites.Create(ctx, website("contested", map[string]any{"image": "registry.k8s.io/pause:3.9", "replicas": int64(1)}), metav1.CreateOptions{}); err != nil {
		return err
	}
	if _, err := awaitDeployment(ctx, env, "contested", 60*time.Second); err != nil {
		return err
	}

	// Twenty spec edits in a row, each racing the controller's status write.
	for i := 0; i < 20; i++ {
		replicas := int64(1 + i%3)
		if err := updateWebsite(ctx, websites, "contested", func(site *unstructured.Unstructured) error {
			return unstructured.SetNestedField(site.Object, replicas, "spec", "replicas")
		}); err != nil {
			return fmt.Errorf("edit %d: %w", i, err)
		}
	}

	final, err := websites.Get(ctx, "contested", metav1.GetOptions{})
	if err != nil {
		return err
	}
	want := final.GetGeneration()

	if err := waitFor(ctx, "status to catch up with the last edit", 60*time.Second, func(ctx context.Context) (bool, error) {
		got, err := websites.Get(ctx, "contested", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		seen, found, _ := unstructured.NestedInt64(got.Object, "status", "observedGeneration")
		return found && seen >= want, nil
	}); err != nil {
		return fmt.Errorf("status never caught up after concurrent edits — a 409 has to be re-read and retried, not given up on: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	if done, res := p.Exited(); done {
		return fmt.Errorf("the program exited (%d) during concurrent edits:\n%s", res.ExitCode, tail(res.Stderr))
	}
	return nil
}

// stageServerSideApply — the children are applied, and the API server records
// this controller as the manager of the fields it set.
func stageServerSideApply(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	if _, err := websites.Create(ctx, website("applied", map[string]any{"image": "registry.k8s.io/pause:3.9", "replicas": int64(2)}), metav1.CreateOptions{}); err != nil {
		return err
	}
	dep, err := awaitDeployment(ctx, env, "applied", 60*time.Second)
	if err != nil {
		return err
	}

	var manager string
	for _, entry := range dep.GetManagedFields() {
		if entry.Operation == metav1.ManagedFieldsOperationApply {
			manager = entry.Manager
			break
		}
	}
	if manager == "" {
		return fmt.Errorf("no managedFields entry with operation Apply — the Deployment was created or patched, not applied:\n%v", dep.GetManagedFields())
	}

	// An outside manager takes a field; the controller must take it back,
	// because the Website's spec is the intent.
	deployments := env.Client.AppsV1().Deployments(env.Namespace)
	if _, err := deployments.Patch(ctx, "applied", types.MergePatchType,
		[]byte(`{"spec":{"replicas":9}}`), metav1.PatchOptions{FieldManager: "someone-else"}); err != nil {
		return err
	}
	if err := waitFor(ctx, "the controller to reclaim the replicas field", 60*time.Second, func(ctx context.Context) (bool, error) {
		got, err := deployments.Get(ctx, "applied", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return got.Spec.Replicas != nil && *got.Spec.Replicas == 2, nil
	}); err != nil {
		return fmt.Errorf("%w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	return nil
}

// stageSecondaryWatch — deleting a child is noticed through the child, not
// through a timer, and the pass that follows is the ordinary one.
func stageSecondaryWatch(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	if _, err := websites.Create(ctx, website("watched", map[string]any{"image": "registry.k8s.io/pause:3.9"}), metav1.CreateOptions{}); err != nil {
		return err
	}
	before, err := awaitDeployment(ctx, env, "watched", 60*time.Second)
	if err != nil {
		return err
	}

	deployments := env.Client.AppsV1().Deployments(env.Namespace)
	if err := deployments.Delete(ctx, "watched", metav1.DeleteOptions{}); err != nil {
		return err
	}

	// The child event has to reach the loop as work on its owner.
	if err := awaitLine(ctx, p, "child "+env.Namespace+"/watched owned by "+env.Namespace+"/watched", 30*time.Second); err != nil {
		return fmt.Errorf("a change to a child was never mapped back to its Website: %w", err)
	}

	var after *appsv1.Deployment
	if err := waitFor(ctx, "the Deployment to be recreated", 60*time.Second, func(ctx context.Context) (bool, error) {
		got, err := deployments.Get(ctx, "watched", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		after = got
		return got.UID != before.UID, nil
	}); err != nil {
		return fmt.Errorf("%w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	if after.UID == before.UID {
		return fmt.Errorf("the Deployment was never actually replaced")
	}
	return nil
}

// stageForeignChildren — a Deployment that belongs to something else is left
// exactly as it was, and the controller says why.
func stageForeignChildren(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	// A Deployment that already has a controller: someone else's object that
	// happens to share the name.
	if err := env.SeedConfigMap(ctx, "other-owner"); err != nil {
		return err
	}
	cm, err := env.ConfigMap(ctx, "other-owner")
	if err != nil {
		return err
	}
	if err := seedUnowned(ctx, env, "guarded"); err != nil {
		return err
	}
	deployments := env.Client.AppsV1().Deployments(env.Namespace)
	// The Deployment controller writes status to a Deployment as soon as it
	// exists, so handing it a new owner has to be retried on conflict like
	// any other write against an object something else is also updating.
	yes := true
	var before *appsv1.Deployment
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		claimed, err := deployments.Get(ctx, "guarded", metav1.GetOptions{})
		if err != nil {
			return err
		}
		claimed.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "v1", Kind: "ConfigMap", Name: cm.Name, UID: cm.UID,
			Controller: &yes, BlockOwnerDeletion: &yes,
		}}
		before, err = deployments.Update(ctx, claimed, metav1.UpdateOptions{})
		return err
	}); err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	if _, err := websites.Create(ctx, website("guarded", map[string]any{"image": "registry.k8s.io/pause:3.10", "replicas": int64(4)}), metav1.CreateOptions{}); err != nil {
		return err
	}
	if err := awaitLine(ctx, p, "controlled by", 60*time.Second); err != nil {
		return fmt.Errorf("the controller met an object it does not own and said nothing: %w", err)
	}

	// Give it long enough that a controller which was going to overwrite the
	// object would have done so.
	time.Sleep(15 * time.Second)
	after, err := deployments.Get(ctx, "guarded", metav1.GetOptions{})
	if err != nil {
		return err
	}
	switch {
	case after.UID != before.UID:
		return fmt.Errorf("the Deployment was replaced — an object owned by something else must not be touched")
	case *after.Spec.Replicas != *before.Spec.Replicas:
		return fmt.Errorf("replicas went from %d to %d on an object owned by a ConfigMap", *before.Spec.Replicas, *after.Spec.Replicas)
	case after.Spec.Template.Spec.Containers[0].Image != before.Spec.Template.Spec.Containers[0].Image:
		return fmt.Errorf("the image was rewritten on an object owned by something else")
	case len(after.GetOwnerReferences()) != 1 || after.GetOwnerReferences()[0].Kind != "ConfigMap":
		return fmt.Errorf("the ownerReferences were rewritten: %v", after.GetOwnerReferences())
	}
	return nil
}

// stageEvents — what happened shows up on the object, where kubectl describe
// will find it.
func stageEvents(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	site, err := websites.Create(ctx, website("announced", map[string]any{"image": "registry.k8s.io/pause:3.9"}), metav1.CreateOptions{})
	if err != nil {
		return err
	}

	var found *corev1.Event
	if err := waitFor(ctx, "an event about the Website", 90*time.Second, func(ctx context.Context) (bool, error) {
		events, err := env.Client.CoreV1().Events(env.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, nil
		}
		for i := range events.Items {
			e := &events.Items[i]
			if e.InvolvedObject.Kind == "Website" && e.InvolvedObject.Name == "announced" {
				found = e
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return fmt.Errorf("%w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	switch {
	case found.InvolvedObject.UID != site.GetUID():
		return fmt.Errorf("the event points at UID %q, not the Website's %q", found.InvolvedObject.UID, site.GetUID())
	case found.Reason == "":
		return fmt.Errorf("the event has no reason, so nothing can be searched for or aggregated on it")
	case found.Source.Component == "" && found.ReportingController == "":
		return fmt.Errorf("the event names no source, so nobody can tell which controller said it")
	}

	// Events are for transitions. A pass every ten seconds must not produce
	// one every ten seconds.
	time.Sleep(25 * time.Second)
	events, err := env.Client.CoreV1().Events(env.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	total := 0
	for i := range events.Items {
		e := &events.Items[i]
		if e.InvolvedObject.Kind == "Website" && e.InvolvedObject.Name == "announced" {
			total += int(max(e.Count, 1))
		}
	}
	if total > 6 {
		return fmt.Errorf("%d events for one Website in half a minute — record transitions, not passes", total)
	}
	return nil
}

// stageMetrics — the loop can be watched from outside the process.
func stageMetrics(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	addr := "127.0.0.1:9137"
	p, cleanup, err := launch(ctx, env, bin, "--metrics-addr="+addr)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "synced", 60*time.Second); err != nil {
		return err
	}

	if _, err := websites.Create(ctx, website("measured", map[string]any{"image": "registry.k8s.io/pause:3.9"}), metav1.CreateOptions{}); err != nil {
		return err
	}
	if _, err := awaitDeployment(ctx, env, "measured", 60*time.Second); err != nil {
		return err
	}

	var body string
	if err := waitFor(ctx, "the metrics endpoint to report a pass", 60*time.Second, func(ctx context.Context) (bool, error) {
		got, err := httpGet(ctx, "http://"+addr+"/metrics")
		if err != nil {
			return false, nil
		}
		body = got
		return strings.Contains(body, "byok8s_reconcile_total"), nil
	}); err != nil {
		return fmt.Errorf("nothing served a metrics endpoint on %s: %w\nthe program said:\n%s", addr, err, tail(p.Stdout()))
	}

	if !strings.Contains(body, "# TYPE byok8s_reconcile_total counter") {
		return fmt.Errorf("byok8s_reconcile_total has no TYPE line — a scraper cannot tell a counter from a gauge:\n%s", tail(body))
	}
	value, err := metricValue(body, "byok8s_reconcile_total")
	if err != nil {
		return err
	}
	if value < 1 {
		return fmt.Errorf("byok8s_reconcile_total is %d after a Website was reconciled", value)
	}
	return nil
}

// stageHealthProbes — the kubelet can ask whether to restart this and whether
// it is ready, and gets different answers to those two questions.
func stageHealthProbes(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}

	addr := "127.0.0.1:9138"
	p, cleanup, err := launch(ctx, env, bin, "--metrics-addr="+addr)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := waitFor(ctx, "/healthz to answer", 60*time.Second, func(ctx context.Context) (bool, error) {
		_, err := httpGet(ctx, "http://"+addr+"/healthz")
		return err == nil, nil
	}); err != nil {
		return fmt.Errorf("nothing answered /healthz on %s: %w\nthe program said:\n%s", addr, err, tail(p.Stdout()))
	}
	if err := waitFor(ctx, "/readyz to answer", 60*time.Second, func(ctx context.Context) (bool, error) {
		_, err := httpGet(ctx, "http://"+addr+"/readyz")
		return err == nil, nil
	}); err != nil {
		return fmt.Errorf("/readyz never became ready: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	if !strings.Contains(p.Stdout(), "synced") {
		return fmt.Errorf("/readyz answered before the cache had synced — that is what readiness is for")
	}

	// The endpoints belong to the process: when it stops, they stop.
	cleanup()
	if err := waitFor(ctx, "the endpoints to go away with the process", 30*time.Second, func(ctx context.Context) (bool, error) {
		_, err := httpGet(ctx, "http://"+addr+"/healthz")
		return err != nil, nil
	}); err != nil {
		return fmt.Errorf("something is still answering on %s after the program stopped — the HTTP server outlived it: %w", addr, err)
	}
	return nil
}

// stageLeaderElection — two instances, one worker. The standby waits, and
// takes over when the leader stops.
func stageLeaderElection(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}

	first, stopFirst, err := launch(ctx, env, bin, "--metrics-addr=127.0.0.1:9139", "--identity=first")
	if err != nil {
		return err
	}
	stoppedFirst := false
	defer func() {
		if !stoppedFirst {
			stopFirst()
		}
	}()
	if err := awaitLine(ctx, first, "leading as first", 60*time.Second); err != nil {
		return fmt.Errorf("the only instance running never became the leader: %w", err)
	}

	second, stopSecond, err := launch(ctx, env, bin, "--metrics-addr=127.0.0.1:9140", "--identity=second")
	if err != nil {
		return err
	}
	defer stopSecond()

	// The standby must wait, not work.
	time.Sleep(20 * time.Second)
	if strings.Contains(second.Stdout(), "leading as second") {
		return fmt.Errorf("both instances are leading — the lease is not being honoured:\n%s", tail(second.Stdout()))
	}
	if done, res := second.Exited(); done {
		return fmt.Errorf("the standby exited (%d) instead of waiting for the lease:\n%s", res.ExitCode, tail(res.Stderr))
	}

	lease, err := env.Client.CoordinationV1().Leases(env.Namespace).Get(ctx, "byok8s-controller", metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("no Lease named byok8s-controller in the namespace: %w", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "first" {
		return fmt.Errorf("the lease is held by %v, not by the instance that is leading", lease.Spec.HolderIdentity)
	}

	// The leader goes away; the standby has to notice and take over.
	stopFirst()
	stoppedFirst = true
	if err := awaitLine(ctx, second, "leading as second", 90*time.Second); err != nil {
		return fmt.Errorf("the standby never took over after the leader stopped: %w", err)
	}
	return nil
}

// stageGracefulShutdown — SIGTERM is an ordinary event: say so, finish the
// work in hand, release the lease, exit 0.
func stageGracefulShutdown(ctx context.Context, env *kube.Env, bin string) error {
	if err := ensureCRD(ctx, env, bin); err != nil {
		return err
	}
	websites, err := websiteClient(env)
	if err != nil {
		return err
	}

	p, cleanupEnv, err := launch(ctx, env, bin, "--metrics-addr=127.0.0.1:9141", "--identity=leaving")
	if err != nil {
		return err
	}
	defer cleanupEnv()
	if err := awaitLine(ctx, p, "leading as leaving", 60*time.Second); err != nil {
		return err
	}
	if _, err := websites.Create(ctx, website("parting", map[string]any{"image": "registry.k8s.io/pause:3.9"}), metav1.CreateOptions{}); err != nil {
		return err
	}
	if _, err := awaitDeployment(ctx, env, "parting", 60*time.Second); err != nil {
		return err
	}

	res := p.Stop(20 * time.Second)
	switch {
	case res.ExitCode != 0:
		return fmt.Errorf("SIGTERM produced exit code %d — an ordinary shutdown must exit 0, or every rollout looks like a crash\nstderr:\n%s",
			res.ExitCode, tail(res.Stderr))
	case !strings.Contains(res.Stdout, "shutting down"):
		return fmt.Errorf("nothing in the output says the program was asked to stop, so an operator cannot tell a deliberate stop from a crash:\n%s", tail(res.Stdout))
	}

	// The lease was handed back, so the next instance leads at once rather
	// than waiting for it to expire.
	next, stopNext, err := launch(ctx, env, bin, "--metrics-addr=127.0.0.1:9142", "--identity=successor")
	if err != nil {
		return err
	}
	defer stopNext()
	if err := awaitLine(ctx, next, "leading as successor", 20*time.Second); err != nil {
		return fmt.Errorf("the next instance had to wait out the lease — releasing it on shutdown is what makes the handover quick: %w", err)
	}
	return nil
}

// --- helpers ---------------------------------------------------------------

// scaleTo writes a replica count straight onto a child, behind the
// controller's back.
//
// It retries on conflict because a Deployment is never idle: its own
// controller writes status to it, and the program under test may be writing
// to it too, so a plain get-then-update loses the race intermittently.
func scaleTo(ctx context.Context, deployments typedappsv1.DeploymentInterface, name string, replicas int32) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		scaled, err := deployments.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		scaled.Spec.Replicas = &replicas
		_, err = deployments.Update(ctx, scaled, metav1.UpdateOptions{})
		return err
	})
}

// scoped writes a kubeconfig whose context selects this stage's namespace and
// returns it as an environment entry, so the program works in its own
// namespace without needing a flag it has not learned yet.
func scoped(env *kube.Env) ([]string, func(), error) {
	dir, err := os.MkdirTemp("", "byok8s-stage-")
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { os.RemoveAll(dir) }
	path, err := env.KubeconfigScoped(dir)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return []string{"KUBECONFIG=" + path}, cleanup, nil
}

// website builds a Website object with this spec, for the assertions that
// need one to exist.
func website(name string, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": group + "/" + version,
		"kind":       "Website",
		"metadata":   map[string]any{"name": name},
		"spec":       spec,
	}}
}

// launch starts the learner's program pointed at this stage's namespace and
// returns it with the cleanup that stops it. Every stage past the first two
// grades a running program, so this is the usual way in.
func launch(ctx context.Context, env *kube.Env, bin string, args ...string) (*runner.Process, func(), error) {
	kc, cleanupEnv, err := scoped(env)
	if err != nil {
		return nil, nil, err
	}
	p, err := runner.Start(ctx, bin, kc, args...)
	if err != nil {
		cleanupEnv()
		return nil, nil, err
	}
	return p, func() {
		p.Stop(5 * time.Second)
		cleanupEnv()
	}, nil
}

// awaitLine waits for the program to print something. A program that has
// already exited will never print it, so that is reported as itself rather
// than as a timeout.
func awaitLine(ctx context.Context, p *runner.Process, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		out := p.Stdout()
		if strings.Contains(out, want) {
			return nil
		}
		if done, res := p.Exited(); done {
			return fmt.Errorf("the program exited (%d) without printing %q\nstdout:\n%s\nstderr:\n%s",
				res.ExitCode, want, tail(res.Stdout), tail(res.Stderr))
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("gave up waiting for %q after %s\nstdout:\n%s\nstderr:\n%s",
				want, timeout, tail(out), tail(p.Stderr()))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// countLines is how a stage checks that something was reported once and not
// twice — a list followed by a watch must not report the same object again.
func countLines(out, want string) int {
	n := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == want {
			n++
		}
	}
	return n
}

// websiteClient talks to the Websites in this stage's namespace.
func websiteClient(env *kube.Env) (dynamic.ResourceInterface, error) {
	c, err := dyn(env)
	if err != nil {
		return nil, err
	}
	return c.Resource(websiteGVR).Namespace(env.Namespace), nil
}

// ensureCRD makes the kind exist before a stage seeds objects of it, by
// letting the program install it exactly as stage 1 asked it to.
func ensureCRD(ctx context.Context, env *kube.Env, bin string) error {
	if crd, err := getCRD(ctx, env); err == nil && established(crd) {
		return nil
	}
	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, crdName, 60*time.Second); err != nil {
		return fmt.Errorf("waiting for the program to install %s: %w", crdName, err)
	}
	return waitFor(ctx, crdName+" to be served", 30*time.Second, func(ctx context.Context) (bool, error) {
		crd, err := getCRD(ctx, env)
		return err == nil && established(crd), nil
	})
}

// countPrefixed counts output lines that start with this prefix, for the many
// lines later stages extend with more detail.
func countPrefixed(out, prefix string) int {
	n := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), prefix) {
			n++
		}
	}
	return n
}

// seedUnowned creates a Deployment shaped like the one the course specifies,
// with no owner: the state left behind by a person who created it by hand, or
// by a controller that did not set owner references.
func seedUnowned(ctx context.Context, env *kube.Env, name string) error {
	one := int32(1)
	labels := map[string]string{"app": name}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &one,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "web", Image: "registry.k8s.io/pause:3.9"}},
				},
			},
		},
	}
	_, err := env.Client.AppsV1().Deployments(env.Namespace).Create(ctx, dep, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// awaitDeployment waits for the controller to create a Deployment and returns
// it. Convergence takes a moment, so every child assertion goes through here
// rather than reading once and failing on a race.
func awaitDeployment(ctx context.Context, env *kube.Env, name string, timeout time.Duration) (*appsv1.Deployment, error) {
	var dep *appsv1.Deployment
	err := waitFor(ctx, "the Deployment "+name, timeout, func(ctx context.Context) (bool, error) {
		got, err := env.Client.AppsV1().Deployments(env.Namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		dep = got
		return true, nil
	})
	return dep, err
}

// updateWebsite applies a change to a Website, retrying if the controller
// rewrote the object underneath — which it does constantly once it is writing
// status, so an assertion that does not retry is a flaky assertion.
func updateWebsite(ctx context.Context, websites dynamic.ResourceInterface, name string, mutate func(*unstructured.Unstructured) error) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest, err := websites.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if err := mutate(latest); err != nil {
			return err
		}
		_, err = websites.Update(ctx, latest, metav1.UpdateOptions{})
		return err
	})
}

// touchWebsite makes a harmless change so the controller is told to look
// again — a stand-in for whatever would have triggered the pass in the stages
// that have not been built yet.
func touchWebsite(ctx context.Context, websites dynamic.ResourceInterface, name string) error {
	return updateWebsite(ctx, websites, name, func(site *unstructured.Unstructured) error {
		anns := site.GetAnnotations()
		if anns == nil {
			anns = map[string]string{}
		}
		anns["byok8s.dev/touched"] = time.Now().Format(time.RFC3339Nano)
		site.SetAnnotations(anns)
		return nil
	})
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

// httpGet reads one of the program's own endpoints.
func httpGet(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	if res.StatusCode != http.StatusOK {
		return string(body), fmt.Errorf("%s: %s", url, res.Status)
	}
	return string(body), nil
}

// metricValue pulls one sample out of the exposition text.
func metricValue(body, name string) (int64, error) {
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, name+" ") {
			continue
		}
		return strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, name)), 10, 64)
	}
	return 0, fmt.Errorf("no sample named %s in:\n%s", name, tail(body))
}

func dyn(env *kube.Env) (dynamic.Interface, error) {
	c, err := dynamic.NewForConfig(env.Config)
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}
	return c, nil
}

func getCRD(ctx context.Context, env *kube.Env) (*unstructured.Unstructured, error) {
	c, err := dyn(env)
	if err != nil {
		return nil, err
	}
	return c.Resource(crdGVR).Get(ctx, crdName, metav1.GetOptions{})
}

// removeCRD deletes the definition and waits for it to go, so a stage grades
// the program's own install path rather than a leftover from the stage before.
func removeCRD(ctx context.Context, env *kube.Env) error {
	c, err := dyn(env)
	if err != nil {
		return err
	}
	// Websites carry a finalizer from stage 19 on, and deleting the definition
	// blocks until every object of that kind is gone — with no controller
	// running, nothing would ever release them.
	if sites, err := c.Resource(websiteGVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{}); err == nil {
		for i := range sites.Items {
			site := &sites.Items[i]
			if len(site.GetFinalizers()) == 0 {
				continue
			}
			if _, err := c.Resource(websiteGVR).Namespace(site.GetNamespace()).Patch(ctx, site.GetName(),
				types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`),
				metav1.PatchOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("release finalizers on %s/%s: %w", site.GetNamespace(), site.GetName(), err)
			}
		}
	}

	client := c.Resource(crdGVR)
	if err := client.Delete(ctx, crdName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete %s: %w", crdName, err)
	}
	return kube.WaitFor(ctx, crdName+" to be removed", func(ctx context.Context) (bool, error) {
		_, err := client.Get(ctx, crdName, metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	})
}

func established(crd *unstructured.Unstructured) bool {
	conds, _, _ := unstructured.NestedSlice(crd.Object, "status", "conditions")
	for _, c := range conds {
		cond, ok := c.(map[string]any)
		if ok && cond["type"] == "Established" && cond["status"] == "True" {
			return true
		}
	}
	return false
}

// waitFor is kube.WaitFor with a stage-local deadline, for the many "the
// cluster should end up like this" assertions a controller course makes.
func waitFor(ctx context.Context, what string, timeout time.Duration, cond wait.ConditionWithContextFunc) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return kube.WaitFor(ctx, what, cond)
}

func tail(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "(nothing)"
	}
	lines := strings.Split(s, "\n")
	if len(lines) > 12 {
		lines = append([]string{"…"}, lines[len(lines)-12:]...)
	}
	return "  " + strings.Join(lines, "\n  ")
}
