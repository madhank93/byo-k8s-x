// Package kube is the harness's own access to the cluster: the fixtures a
// stage seeds, the namespace it runs in, and the assertions it makes.
//
// The contract, inherited from kubeclientlings' exkit and worth restating:
// this package is always correct. When a stage fails, the bug is in the
// learner's program, never in here. That is what lets a failure message point
// at the learner's code without hedging.
package kube

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/madhank93/byo-k8s-x/internal/cluster"
)

// StageLabel marks every namespace the harness creates, so a stray one can be
// swept without touching anything else in the cluster.
const StageLabel = "byok8s.dev/stage"

// Env is one stage's view of the cluster: a client, an isolated namespace, and
// a context bounded by the stage's timeout.
type Env struct {
	Client    kubernetes.Interface
	Config    *rest.Config
	Namespace string
}

// RESTConfig loads the kubeconfig the same way kubectl does, pinned to the
// course's kind context.
//
// The guard is not decoration. These stages create and delete namespaces, and
// a kubeconfig whose current context points at a real cluster would do that
// there. Refusing any non-kind context makes that mistake impossible rather
// than unlikely.
func RESTConfig() (*rest.Config, error) {
	overrides := &clientcmd.ConfigOverrides{CurrentContext: cluster.Context}
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), overrides)

	raw, err := loader.RawConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	name := overrides.CurrentContext
	if name == "" {
		name = raw.CurrentContext
	}
	if !strings.HasPrefix(name, "kind-") {
		return nil, fmt.Errorf("refusing to use context %q: courses only run against local kind clusters", name)
	}
	return loader.ClientConfig()
}

// Begin gives a stage a clean namespace named bko-<stage>.
//
// It deletes any namespace left over from a previous run first — and strips
// finalizers off its contents before deleting, because a stage that failed
// midway can leave an object whose finalizer nothing will ever clear, which
// wedges the namespace in Terminating forever and breaks every later run.
func Begin(ctx context.Context, stage string, timeout time.Duration) (*Env, context.CancelFunc, error) {
	cfg, err := RESTConfig()
	if err != nil {
		return nil, nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("build clientset: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	ns := "bko-" + stage

	if err := reset(ctx, cs, ns); err != nil {
		cancel()
		return nil, nil, err
	}
	return &Env{Client: cs, Config: cfg, Namespace: ns}, cancel, nil
}

func reset(ctx context.Context, cs kubernetes.Interface, ns string) error {
	if _, err := cs.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{}); err == nil {
		if err := clearFinalizers(ctx, cs, ns); err != nil {
			return err
		}
		if err := cs.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete namespace %s: %w", ns, err)
		}
		if err := WaitFor(ctx, "the previous "+ns+" namespace to go away", func(ctx context.Context) (bool, error) {
			_, err := cs.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
			return apierrors.IsNotFound(err), nil
		}); err != nil {
			return err
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get namespace %s: %w", ns, err)
	}

	_, err := cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   ns,
			Labels: map[string]string{StageLabel: strings.TrimPrefix(ns, "bko-")},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create namespace %s: %w", ns, err)
	}
	return nil
}

// clearFinalizers strips finalizers from the ConfigMaps in a namespace about to
// be deleted. Stages that teach finalizers deliberately leave one behind when
// they fail, and without this the namespace never finishes terminating.
func clearFinalizers(ctx context.Context, cs kubernetes.Interface, ns string) error {
	cms, err := cs.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("list configmaps in %s: %w", ns, err)
	}
	for i := range cms.Items {
		cm := &cms.Items[i]
		if len(cm.Finalizers) == 0 {
			continue
		}
		cm.Finalizers = nil
		if _, err := cs.CoreV1().ConfigMaps(ns).Update(ctx, cm, metav1.UpdateOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("clear finalizers on %s/%s: %w", ns, cm.Name, err)
		}
	}
	return nil
}

// WaitFor polls until cond is true or the context expires. Every wait in the
// harness goes through here: a sleep long enough to be reliable is always long
// enough to be slow, and one that is fast is flaky.
func WaitFor(ctx context.Context, what string, cond wait.ConditionWithContextFunc) error {
	if err := wait.PollUntilContextCancel(ctx, 500*time.Millisecond, true, cond); err != nil {
		return fmt.Errorf("timed out waiting for %s: %w", what, err)
	}
	return nil
}

// ServerURL is the API server address the learner's program should discover
// from the kubeconfig, and what stage 1 asserts against.
func (e *Env) ServerURL() string { return e.Config.Host }

// KubeconfigScoped writes a kubeconfig identical to the real one except that
// the context carries this stage's namespace, and returns its path.
//
// A context's namespace is the third thing kubectl consults, after the --namespace
// flag and before "default". Handing the program one of these lets a stage
// isolate itself without the program needing a flag it has not learned yet —
// and it exercises real behaviour rather than a test seam.
func (e *Env) KubeconfigScoped(dir string) (string, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	raw, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{}).RawConfig()
	if err != nil {
		return "", fmt.Errorf("read kubeconfig: %w", err)
	}
	kctx, ok := raw.Contexts[cluster.Context]
	if !ok {
		return "", fmt.Errorf("context %s missing from kubeconfig", cluster.Context)
	}
	kctx.Namespace = e.Namespace
	raw.CurrentContext = cluster.Context

	path := filepath.Join(dir, "kubeconfig")
	if err := clientcmd.WriteToFile(raw, path); err != nil {
		return "", fmt.Errorf("write scoped kubeconfig: %w", err)
	}
	return path, nil
}

// SeedPods creates pods in the stage's namespace and returns their names.
//
// They are never waited on: a pod that exists is enough to be listed, and
// waiting for the image to pull would add half a minute to every stage that
// only needs something to print.
func (e *Env) SeedPods(ctx context.Context, names ...string) error {
	for _, name := range names {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e.Namespace},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:    "app",
					Image:   "registry.k8s.io/pause:3.9",
					Command: nil,
				}},
			},
		}
		if _, err := e.Client.CoreV1().Pods(e.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("seed pod %s: %w", name, err)
		}
	}
	return nil
}

// SeedConfigMap creates a ConfigMap, for stages that need a resource the
// program has no compiled-in type for.
func (e *Env) SeedConfigMap(ctx context.Context, name string) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e.Namespace},
		Data:       map[string]string{"greeting": "hello"},
	}
	if _, err := e.Client.CoreV1().ConfigMaps(e.Namespace).Create(ctx, cm, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("seed configmap %s: %w", name, err)
	}
	return nil
}

// SeedLabeledPod creates one pod carrying labels, for the selector stages.
func (e *Env) SeedLabeledPod(ctx context.Context, name string, labels map[string]string) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e.Namespace, Labels: labels},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	if _, err := e.Client.CoreV1().Pods(e.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("seed pod %s: %w", name, err)
	}
	return nil
}
