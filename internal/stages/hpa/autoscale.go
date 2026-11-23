package hpa

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
)

// The kind cluster already runs the real HPA controller, and two controllers
// acting on one HorizontalPodAutoscaler fight. So the course's autoscaler is
// an Autoscaler: the same spec and status as autoscaling/v2, in a group the
// real controller ignores. Workload is a scale target whose current replica
// count the grader decides, so a recommendation is graded without waiting on
// pods to come and go.
const group = "byok8s.dev"

var (
	crdGVR        = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	autoscalerGVR = schema.GroupVersionResource{Group: group, Version: "v1alpha1", Resource: "autoscalers"}
	workloadGVR   = schema.GroupVersionResource{Group: group, Version: "v1alpha1", Resource: "workloads"}
)

const readyGate = "byok8s.dev/ready"

func crd(plural, kind string, scale bool) *unstructured.Unstructured {
	open := map[string]any{"type": "object", "x-kubernetes-preserve-unknown-fields": true}
	sub := map[string]any{"status": map[string]any{}}
	if scale {
		sub["scale"] = map[string]any{"specReplicasPath": ".spec.replicas", "statusReplicasPath": ".status.replicas", "labelSelectorPath": ".status.selector"}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
		"metadata": map[string]any{"name": plural + "." + group},
		"spec": map[string]any{
			"group": group, "scope": "Namespaced",
			"names": map[string]any{"plural": plural, "singular": strings.ToLower(kind), "kind": kind},
			"versions": []any{map[string]any{
				"name": "v1alpha1", "served": true, "storage": true, "subresources": sub,
				"schema": map[string]any{"openAPIV3Schema": map[string]any{"type": "object", "properties": map[string]any{"spec": open, "status": open}}},
			}},
		},
	}}
}

// ensureCRDs installs Autoscaler and Workload once and leaves them: other
// stages, and other runs, reuse them.
func ensureCRDs(ctx context.Context, dyn dynamic.Interface) error {
	for _, c := range []*unstructured.Unstructured{crd("autoscalers", "Autoscaler", false), crd("workloads", "Workload", true)} {
		if _, err := dyn.Resource(crdGVR).Create(ctx, c, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("install %s: %w", c.GetName(), err)
		}
		name := c.GetName()
		if err := kube.WaitFor(ctx, name+" to be Established", func(ctx context.Context) (bool, error) {
			got, err := dyn.Resource(crdGVR).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			conds, _, _ := unstructured.NestedSlice(got.Object, "status", "conditions")
			for _, c := range conds {
				m, _ := c.(map[string]any)
				if m["type"] == "Established" && m["status"] == "True" {
					return true, nil
				}
			}
			return false, nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// rig is what every autoscaling stage starts from: the definitions, the
// grader's metrics, and the program running as `app autoscale`.
type rig struct {
	env     *kube.Env
	dyn     dynamic.Interface
	metrics *fakeMetrics
	proc    *runner.Process
}

func startRig(ctx context.Context, env *kube.Env, bin string) (*rig, func(), error) {
	dyn, err := dynamic.NewForConfig(env.Config)
	if err != nil {
		return nil, nil, err
	}
	if err := ensureCRDs(ctx, dyn); err != nil {
		return nil, nil, err
	}
	fm, stopMetrics, err := startFakeMetrics(ctx, env)
	if err != nil {
		return nil, nil, err
	}
	kc, cleanupEnv, err := scoped(env)
	if err != nil {
		stopMetrics()
		return nil, nil, err
	}
	p, err := runner.Start(ctx, bin, kc, "autoscale")
	if err != nil {
		cleanupEnv()
		stopMetrics()
		return nil, nil, err
	}
	return &rig{env: env, dyn: dyn, metrics: fm, proc: p}, func() {
		p.Stop(5 * time.Second)
		cleanupEnv()
		stopMetrics()
	}, nil
}

// pinned keeps the autoscaling stages' fixtures to what the stage is about:
// no stabilization, and limits no recommendation here can reach.
var pinned = map[string]any{
	"scaleUp":   map[string]any{"stabilizationWindowSeconds": int64(0), "policies": []any{map[string]any{"type": "Pods", "value": int64(100), "periodSeconds": int64(1)}}},
	"scaleDown": map[string]any{"stabilizationWindowSeconds": int64(0), "policies": []any{map[string]any{"type": "Pods", "value": int64(100), "periodSeconds": int64(1)}}},
}

func cpuMetric(util int64) map[string]any {
	return map[string]any{"type": "Resource", "resource": map[string]any{"name": "cpu",
		"target": map[string]any{"type": "Utilization", "averageUtilization": util}}}
}

func (r *rig) autoscaler(ctx context.Context, name, kind, apiVersion, target string, min, max int64, behavior map[string]any, metrics ...map[string]any) error {
	var ms []any
	for _, m := range metrics {
		ms = append(ms, m)
	}
	spec := map[string]any{
		"scaleTargetRef": map[string]any{"apiVersion": apiVersion, "kind": kind, "name": target},
		"minReplicas":    min, "maxReplicas": max, "metrics": ms, "behavior": behavior,
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": group + "/v1alpha1", "kind": "Autoscaler",
		"metadata": map[string]any{"name": name}, "spec": spec,
	}}
	_, err := r.dyn.Resource(autoscalerGVR).Namespace(r.env.Namespace).Create(ctx, obj, metav1.CreateOptions{})
	return err
}

func (r *rig) toWorkload(ctx context.Context, name, target string, min, max int64, behavior map[string]any, metrics ...map[string]any) error {
	return r.autoscaler(ctx, name, "Workload", group+"/v1alpha1", target, min, max, behavior, metrics...)
}

// workload creates a Workload reporting replicas, backed by that many bare
// pods requesting 50m of CPU each. ready says which pods pass their readiness
// gate; the grader owns the gate, so readiness is never a race with a probe.
func (r *rig) workload(ctx context.Context, name string, replicas int, ready func(i int) bool) ([]string, error) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": group + "/v1alpha1", "kind": "Workload",
		"metadata": map[string]any{"name": name}, "spec": map[string]any{"replicas": int64(replicas)},
	}}
	wl := r.dyn.Resource(workloadGVR).Namespace(r.env.Namespace)
	if _, err := wl.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create workload %s: %w", name, err)
	}
	if err := r.setWorkloadReplicas(ctx, name, replicas); err != nil {
		return nil, err
	}
	var names []string
	var pods []*corev1.Pod
	for i := range replicas {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%d", name, i), Labels: map[string]string{"app": name}},
			Spec: corev1.PodSpec{
				TerminationGracePeriodSeconds: ptr(int64(0)),
				ReadinessGates:                []corev1.PodReadinessGate{{ConditionType: readyGate}},
				Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9",
					Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m")}}}},
			},
		}
		pods = append(pods, p)
		names = append(names, p.Name)
	}
	if err := seedRunning(ctx, r.env, pods...); err != nil {
		return nil, err
	}
	for i, n := range names {
		status := "False"
		if ready == nil || ready(i) {
			status = "True"
		}
		patch := fmt.Sprintf(`{"status":{"conditions":[{"type":%q,"status":%q}]}}`, readyGate, status)
		if _, err := r.env.Client.CoreV1().Pods(r.env.Namespace).Patch(ctx, n, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{}, "status"); err != nil {
			return nil, fmt.Errorf("set readiness of %s: %w", n, err)
		}
		want := status == "True"
		if err := kube.WaitFor(ctx, "pod "+n+" readiness to settle", func(ctx context.Context) (bool, error) {
			p, err := r.env.Client.CoreV1().Pods(r.env.Namespace).Get(ctx, n, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			for _, c := range p.Status.Conditions {
				if c.Type == corev1.PodReady {
					return (c.Status == corev1.ConditionTrue) == want, nil
				}
			}
			return false, nil
		}); err != nil {
			return nil, err
		}
	}
	// A pod that turned ready this instant is still inside the window the
	// real controller uses to distrust a fresh sample.
	time.Sleep(2 * time.Second)
	return names, nil
}

func (r *rig) setWorkloadReplicas(ctx context.Context, name string, replicas int) error {
	patch := fmt.Sprintf(`{"status":{"replicas":%d,"selector":"app=%s"}}`, replicas, name)
	_, err := r.dyn.Resource(workloadGVR).Namespace(r.env.Namespace).Patch(ctx, name, types.MergePatchType, []byte(patch), metav1.PatchOptions{}, "status")
	return err
}

// deployment creates pause pods requesting requestMilli each.
func (r *rig) deployment(ctx context.Context, name string, replicas int32, requestMilli int64) error {
	lbl := map[string]string{"app": name}
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: lbl},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: lbl},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: ptr(int64(0)),
					Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9",
						Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: *resource.NewMilliQuantity(requestMilli, resource.DecimalSI)}}}},
				},
			},
		},
	}
	_, err := r.env.Client.AppsV1().Deployments(r.env.Namespace).Create(ctx, d, metav1.CreateOptions{})
	return err
}

// hpaStatus is the part of an Autoscaler's status the stages judge.
type hpaStatus struct {
	CurrentReplicas *int32       `json:"currentReplicas"`
	DesiredReplicas *int32       `json:"desiredReplicas"`
	LastScaleTime   *metav1.Time `json:"lastScaleTime"`
	CurrentMetrics  []struct {
		Type     string `json:"type"`
		Resource *struct {
			Name    string `json:"name"`
			Current struct {
				AverageUtilization *int32 `json:"averageUtilization"`
			} `json:"current"`
		} `json:"resource"`
	} `json:"currentMetrics"`
}

func (s hpaStatus) String() string {
	b, _ := json.Marshal(s)
	return string(b)
}

func (s hpaStatus) cpuUtil() (int32, bool) {
	for _, m := range s.CurrentMetrics {
		if m.Resource != nil && m.Resource.Name == "cpu" && m.Resource.Current.AverageUtilization != nil {
			return *m.Resource.Current.AverageUtilization, true
		}
	}
	return 0, false
}

func (r *rig) status(ctx context.Context, name string) (hpaStatus, error) {
	var s hpaStatus
	obj, err := r.dyn.Resource(autoscalerGVR).Namespace(r.env.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return s, err
	}
	raw, _ := json.Marshal(obj.Object["status"])
	json.Unmarshal(raw, &s)
	return s, nil
}

// await waits up to 40s for an Autoscaler's status to satisfy ok, which is
// several of the program's sync periods.
func (r *rig) await(ctx context.Context, name, what string, ok func(hpaStatus) bool) (hpaStatus, error) {
	var last hpaStatus
	waitCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	err := kube.WaitFor(waitCtx, what, func(ctx context.Context) (bool, error) {
		if done, res := r.proc.Exited(); done {
			return false, fmt.Errorf("the program exited (%d)\nstdout:\n%s\nstderr:\n%s", res.ExitCode, tail(res.Stdout), tail(res.Stderr))
		}
		s, err := r.status(ctx, name)
		if err != nil {
			return false, nil
		}
		last = s
		return ok(s), nil
	})
	if err != nil {
		return last, fmt.Errorf("autoscaler %s: %w\nits status: %s\nthe program said:\n%s", name, err, last, tail(r.proc.Stdout()+r.proc.Stderr()))
	}
	return last, nil
}

// expect waits for the program to report cpu utilization util — proof it has
// read the latest metrics — alongside desired replicas.
func (r *rig) expect(ctx context.Context, name string, util, desired int32, why string) error {
	_, err := r.await(ctx, name, fmt.Sprintf("cpu utilization %d%% and desiredReplicas %d (%s)", util, desired, why), func(s hpaStatus) bool {
		u, ok := s.cpuUtil()
		return ok && u == util && s.DesiredReplicas != nil && *s.DesiredReplicas == desired
	})
	return err
}

func (r *rig) expectDesired(ctx context.Context, name string, desired int32, why string) error {
	_, err := r.await(ctx, name, fmt.Sprintf("desiredReplicas %d (%s)", desired, why), func(s hpaStatus) bool {
		return s.DesiredReplicas != nil && *s.DesiredReplicas == desired
	})
	return err
}

// holds checks that cond stays true for d, sampling the status throughout.
func (r *rig) holds(ctx context.Context, name string, d time.Duration, what string, cond func(hpaStatus) bool) error {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		s, err := r.status(ctx, name)
		if err == nil && !cond(s) {
			return fmt.Errorf("autoscaler %s: %s, and its status became %s", name, what, s)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return nil
}

// stageHPAWatch — `app autoscale` finds every Autoscaler and reports its
// target's replica count, read through the target's scale subresource.
func stageHPAWatch(ctx context.Context, env *kube.Env, bin string) error {
	r, stop, err := startRig(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()
	if _, err := r.workload(ctx, "batch", 3, nil); err != nil {
		return err
	}
	if err := r.deployment(ctx, "api", 2, 0); err != nil {
		return err
	}
	if err := r.toWorkload(ctx, "batch", "batch", 1, 5, pinned, cpuMetric(50)); err != nil {
		return err
	}
	if err := r.autoscaler(ctx, "api", "Deployment", "apps/v1", "api", 1, 5, pinned, cpuMetric(50)); err != nil {
		return err
	}
	current := func(n int32) func(hpaStatus) bool {
		return func(s hpaStatus) bool { return s.CurrentReplicas != nil && *s.CurrentReplicas == n }
	}
	if _, err := r.await(ctx, "batch", "status.currentReplicas 3, from the Workload's scale", current(3)); err != nil {
		return err
	}
	if _, err := r.await(ctx, "api", "status.currentReplicas 2, from the Deployment's scale", current(2)); err != nil {
		return err
	}
	// Nothing about the Autoscaler changes here, so only a periodic resync
	// notices.
	if err := r.setWorkloadReplicas(ctx, "batch", 2); err != nil {
		return err
	}
	if _, err := r.await(ctx, "batch", "status.currentReplicas to follow the Workload to 2 — sync every Autoscaler on a timer, not only when it changes", current(2)); err != nil {
		return err
	}
	return nil
}

// stageScalingFormula — desired = ceil(current × utilization / target),
// clamped to the Autoscaler's bounds.
func stageScalingFormula(ctx context.Context, env *kube.Env, bin string) error {
	r, stop, err := startRig(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()
	if _, err := r.workload(ctx, "web", 4, nil); err != nil {
		return err
	}
	r.metrics.setCPU(each(75))
	if err := r.toWorkload(ctx, "web", "web", 3, 10, pinned, cpuMetric(50)); err != nil {
		return err
	}
	if err := r.expect(ctx, "web", 150, 10, "4 pods at 150% of a 50% target want 12, and maxReplicas is 10"); err != nil {
		return err
	}
	r.metrics.setCPU(each(65))
	if err := r.expect(ctx, "web", 130, 6, "ceil(4 × 130/50) = ceil(10.4/2) = 6 — round up, never to nearest"); err != nil {
		return err
	}
	r.metrics.setCPU(each(10))
	return r.expect(ctx, "web", 20, 3, "4 pods at 20% want 2, and minReplicas is 3")
}

// stageTolerance — a ratio within 10% of 1.0 changes nothing.
func stageTolerance(ctx context.Context, env *kube.Env, bin string) error {
	r, stop, err := startRig(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()
	if _, err := r.workload(ctx, "web", 4, nil); err != nil {
		return err
	}
	r.metrics.setCPU(each(27))
	if err := r.toWorkload(ctx, "web", "web", 1, 10, pinned, cpuMetric(50)); err != nil {
		return err
	}
	if err := r.expect(ctx, "web", 54, 4, "54% against a 50% target is a ratio of 1.08, inside the 0.1 tolerance, so 4 stays 4"); err != nil {
		return err
	}
	if err := r.holds(ctx, "web", 4*time.Second, "a ratio of 1.08 must not move 4 replicas", func(s hpaStatus) bool {
		return s.DesiredReplicas != nil && *s.DesiredReplicas == 4
	}); err != nil {
		return err
	}
	r.metrics.setCPU(each(29))
	return r.expect(ctx, "web", 58, 5, "a ratio of 1.16 is outside the tolerance: ceil(4 × 1.16) = 5")
}

// stageReadinessWindow — for CPU, an unready pod's usage is not evidence: it
// is dropped, and when the rest say scale up it counts as zero.
func stageReadinessWindow(ctx context.Context, env *kube.Env, bin string) error {
	r, stop, err := startRig(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()
	names, err := r.workload(ctx, "web", 4, func(i int) bool { return i < 2 })
	if err != nil {
		return err
	}
	unready := map[string]bool{names[2]: true, names[3]: true}
	load := func(ready, warming int64) func(string, int) (int64, bool) {
		return func(pod string, _ int) (int64, bool) {
			if unready[pod] {
				return warming, true
			}
			return ready, true
		}
	}
	r.metrics.setCPU(load(20, 75))
	if err := r.toWorkload(ctx, "web", "web", 1, 10, pinned, cpuMetric(50)); err != nil {
		return err
	}
	if err := r.expect(ctx, "web", 40, 2, "two ready pods at 40%; the two unready ones spinning at 150% are starting up, not serving, and are left out"); err != nil {
		return err
	}
	r.metrics.setCPU(load(50, 75))
	return r.expect(ctx, "web", 100, 4, "the ready pods at 100% say scale up; counting the unready pods as idle brings the ratio to 1.0, so nothing changes")
}

// stageMissingMetrics — a pod with no metrics is assumed to be at 100% of its
// request when scaling down, and idle when scaling up.
func stageMissingMetrics(ctx context.Context, env *kube.Env, bin string) error {
	r, stop, err := startRig(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()
	names, err := r.workload(ctx, "web", 4, nil)
	if err != nil {
		return err
	}
	missing := names[3]
	load := func(v int64) func(string, int) (int64, bool) {
		return func(pod string, _ int) (int64, bool) { return v, pod != missing }
	}
	r.metrics.setCPU(load(5))
	if err := r.toWorkload(ctx, "web", "web", 1, 10, pinned, cpuMetric(50)); err != nil {
		return err
	}
	if err := r.expect(ctx, "web", 10, 3, "three pods at 10% say scale down; the pod with no metrics counts as 100% of its request, so (15+50)/200 → ceil(4 × 0.65) = 3"); err != nil {
		return err
	}
	r.metrics.setCPU(load(50))
	return r.expect(ctx, "web", 100, 6, "three pods at 100% say scale up; the pod with no metrics counts as idle, so 150/200 → ceil(4 × 1.5) = 6")
}

// stageScaleSubresource — the program acts on its recommendation, through
// /scale, so a Deployment and a custom resource scale the same way.
func stageScaleSubresource(ctx context.Context, env *kube.Env, bin string) error {
	r, stop, err := startRig(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()
	if err := r.deployment(ctx, "web", 1, 50); err != nil {
		return err
	}
	if _, err := r.workload(ctx, "batch", 2, nil); err != nil {
		return err
	}
	// A fixed total spread over however many pods run: the load a real
	// service sees, so the replica count settles where total/target says.
	total := int64(150)
	r.metrics.setCPU(func(pod string, n int) (int64, bool) {
		if len(pod) > 6 && pod[:6] == "batch-" {
			return 75, true
		}
		return total / int64(n), true
	})
	if err := r.autoscaler(ctx, "web", "Deployment", "apps/v1", "web", 1, 4, pinned, cpuMetric(50)); err != nil {
		return err
	}
	if err := r.toWorkload(ctx, "batch", "batch", 1, 5, pinned, cpuMetric(50)); err != nil {
		return err
	}

	replicas := func(n int32) error {
		waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		var got int32
		return kube.WaitFor(waitCtx, fmt.Sprintf("Deployment web to be scaled to %d", n), func(ctx context.Context) (bool, error) {
			d, err := env.Client.AppsV1().Deployments(env.Namespace).Get(ctx, "web", metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			got = *d.Spec.Replicas
			return got == n, nil
		})
	}
	if err := replicas(4); err != nil {
		return fmt.Errorf("150m of load against 25m per pod wants 6 pods and maxReplicas is 4: %w\nthe program said:\n%s", err, tail(r.proc.Stdout()+r.proc.Stderr()))
	}
	if _, err := r.await(ctx, "web", "status.currentReplicas 4 and a lastScaleTime", func(s hpaStatus) bool {
		return s.CurrentReplicas != nil && *s.CurrentReplicas == 4 && s.LastScaleTime != nil
	}); err != nil {
		return err
	}

	waitCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	var spec int64
	if err := kube.WaitFor(waitCtx, "Workload batch to be scaled to 5", func(ctx context.Context) (bool, error) {
		w, err := r.dyn.Resource(workloadGVR).Namespace(env.Namespace).Get(ctx, "batch", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		spec, _, _ = unstructured.NestedInt64(w.Object, "spec", "replicas")
		return spec == 5, nil
	}); err != nil {
		return fmt.Errorf("%w (spec.replicas is %d) — a Workload is no Deployment: write its scale subresource, which every scalable kind has\nthe program said:\n%s", err, spec, tail(r.proc.Stdout()+r.proc.Stderr()))
	}

	total = 30
	if err := replicas(2); err != nil {
		return fmt.Errorf("30m of load against 25m per pod wants 2 pods: %w", err)
	}
	return nil
}

// stageStabilization — scaling down follows the highest recommendation made
// within the window, so a brief dip does not shed pods.
func stageStabilization(ctx context.Context, env *kube.Env, bin string) error {
	r, stop, err := startRig(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()
	if _, err := r.workload(ctx, "web", 4, nil); err != nil {
		return err
	}
	behavior := map[string]any{
		"scaleUp":   pinned["scaleUp"],
		"scaleDown": map[string]any{"stabilizationWindowSeconds": int64(20), "policies": []any{map[string]any{"type": "Pods", "value": int64(100), "periodSeconds": int64(1)}}},
	}
	r.metrics.setCPU(each(100))
	if err := r.toWorkload(ctx, "web", "web", 1, 8, behavior, cpuMetric(50)); err != nil {
		return err
	}
	if err := r.expect(ctx, "web", 200, 8, "4 pods at 200% want 16, capped at 8"); err != nil {
		return err
	}
	time.Sleep(3 * time.Second)

	r.metrics.setCPU(each(10))
	if _, err := r.await(ctx, "web", "cpu utilization 20%", func(s hpaStatus) bool {
		u, ok := s.cpuUtil()
		return ok && u == 20
	}); err != nil {
		return err
	}
	if err := r.holds(ctx, "web", 8*time.Second, "the load fell seconds ago and a 20s scale-down window still holds a recommendation of 8, so desiredReplicas must not drop below the current 4", func(s hpaStatus) bool {
		return s.DesiredReplicas != nil && *s.DesiredReplicas >= 4
	}); err != nil {
		return err
	}
	return r.expectDesired(ctx, "web", 2, "once the window has passed, the low recommendation stands")
}

// stageBehaviorPolicies — scale-up policies cap how many pods a period may
// add, and selectPolicy picks the most or least generous.
func stageBehaviorPolicies(ctx context.Context, env *kube.Env, bin string) error {
	r, stop, err := startRig(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()
	for _, name := range []string{"most", "least"} {
		if err := r.deployment(ctx, name, 2, 20); err != nil {
			return err
		}
	}
	r.metrics.setCPU(each(100))
	policies := []any{
		map[string]any{"type": "Pods", "value": int64(2), "periodSeconds": int64(60)},
		map[string]any{"type": "Percent", "value": int64(50), "periodSeconds": int64(60)},
	}
	for name, sel := range map[string]string{"most": "Max", "least": "Min"} {
		behavior := map[string]any{
			"scaleUp":   map[string]any{"stabilizationWindowSeconds": int64(0), "selectPolicy": sel, "policies": policies},
			"scaleDown": pinned["scaleDown"],
		}
		if err := r.autoscaler(ctx, name, "Deployment", "apps/v1", name, 1, 10, behavior, cpuMetric(50)); err != nil {
			return err
		}
	}
	spec := func(ctx context.Context, name string) int32 {
		d, err := env.Client.AppsV1().Deployments(env.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return -1
		}
		return *d.Spec.Replicas
	}
	want := map[string]int32{"most": 4, "least": 3}
	waitCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	if err := kube.WaitFor(waitCtx, "most to reach 4 and least 3", func(ctx context.Context) (bool, error) {
		return spec(ctx, "most") == 4 && spec(ctx, "least") == 3, nil
	}); err != nil {
		return fmt.Errorf("%w — both start at 2 and want 10; per 60s, Pods 2 allows 4 and Percent 50 allows 3: Max takes 4, Min takes 3 (most is at %d, least at %d)\nthe program said:\n%s",
			err, spec(ctx, "most"), spec(ctx, "least"), tail(r.proc.Stdout()+r.proc.Stderr()))
	}
	end := time.Now().Add(15 * time.Second)
	for time.Now().Before(end) {
		for name, n := range want {
			if got := spec(ctx, name); got > n {
				return fmt.Errorf("Deployment %s grew to %d within the same 60s period — the limit counts from the replicas at the period's start, not the current count", name, got)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

func podsMetric(name string, avg string) map[string]any {
	return map[string]any{"type": "Pods", "pods": map[string]any{"metric": map[string]any{"name": name},
		"target": map[string]any{"type": "AverageValue", "averageValue": avg}}}
}

// stageCustomMetrics — Pods and Object metrics from custom.metrics.k8s.io.
func stageCustomMetrics(ctx context.Context, env *kube.Env, bin string) error {
	r, stop, err := startRig(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()
	if _, err := r.workload(ctx, "rps", 4, nil); err != nil {
		return err
	}
	if _, err := r.workload(ctx, "front", 2, nil); err != nil {
		return err
	}
	rps := int64(30000)
	r.metrics.setPodMetric("requests_per_second", func(pod string) (int64, bool) { return rps, len(pod) > 4 && pod[:4] == "rps-" })
	r.metrics.setObject("services/front/hits", 250000)

	if err := r.toWorkload(ctx, "rps", "rps", 1, 10, pinned, podsMetric("requests_per_second", "10")); err != nil {
		return err
	}
	obj := map[string]any{"type": "Object", "object": map[string]any{
		"describedObject": map[string]any{"apiVersion": "v1", "kind": "Service", "name": "front"},
		"metric":          map[string]any{"name": "hits"},
		"target":          map[string]any{"type": "Value", "value": "100"}}}
	if err := r.toWorkload(ctx, "front", "front", 1, 10, pinned, obj); err != nil {
		return err
	}
	if err := r.expectDesired(ctx, "rps", 10, "4 pods averaging 30 requests_per_second against a target of 10 want 12, capped at 10"); err != nil {
		return err
	}
	if err := r.expectDesired(ctx, "front", 5, "Service front's hits is 250 against a target value of 100: ceil(2 pods × 2.5) = 5"); err != nil {
		return err
	}
	rps = 15000
	r.metrics.setPodMetric("requests_per_second", func(pod string) (int64, bool) { return rps, len(pod) > 4 && pod[:4] == "rps-" })
	return r.expectDesired(ctx, "rps", 6, "an average of 15 against 10: ceil(4 × 1.5) = 6")
}

func externalMetric(queue, kind, key, val string) map[string]any {
	return map[string]any{"type": "External", "external": map[string]any{
		"metric": map[string]any{"name": "queue_depth", "selector": map[string]any{"matchLabels": map[string]any{"queue": queue}}},
		"target": map[string]any{"type": kind, key: val}}}
}

// stageExternalMetrics — a metric about something outside the cluster,
// selected by label, as a total or per pod.
func stageExternalMetrics(ctx context.Context, env *kube.Env, bin string) error {
	r, stop, err := startRig(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()
	if _, err := r.workload(ctx, "jobs", 2, nil); err != nil {
		return err
	}
	if _, err := r.workload(ctx, "mail", 2, nil); err != nil {
		return err
	}
	depth := map[string]int64{"jobs": 130000, "mail": 80000}
	r.metrics.setExternal(func(metric string, sel labels.Selector) (int64, bool) {
		if metric != "queue_depth" {
			return 0, false
		}
		for q, v := range depth {
			if sel.String() == "queue="+q {
				return v, true
			}
		}
		// Unselected, the metric is every queue at once.
		return 999000, true
	})
	if err := r.toWorkload(ctx, "jobs", "jobs", 1, 10, pinned, externalMetric("jobs", "AverageValue", "averageValue", "20")); err != nil {
		return err
	}
	if err := r.toWorkload(ctx, "mail", "mail", 1, 10, pinned, externalMetric("mail", "Value", "value", "50")); err != nil {
		return err
	}
	if err := r.expectDesired(ctx, "jobs", 7, "queue=jobs holds 130 against an average of 20 per pod: ceil(130/20) = 7"); err != nil {
		return err
	}
	return r.expectDesired(ctx, "mail", 4, "queue=mail holds 80 against a value of 50: ceil(2 pods × 1.6) = 4")
}

// stageMultiMetric — each metric makes its own recommendation and the largest
// wins; a metric that cannot be read blocks scaling down.
func stageMultiMetric(ctx context.Context, env *kube.Env, bin string) error {
	r, stop, err := startRig(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()
	if _, err := r.workload(ctx, "web", 4, nil); err != nil {
		return err
	}
	setRPS := func(v int64) {
		r.metrics.setPodMetric("requests_per_second", func(string) (int64, bool) { return v, v >= 0 })
	}
	r.metrics.setCPU(each(50))
	setRPS(15000)
	if err := r.toWorkload(ctx, "web", "web", 1, 10, pinned, cpuMetric(50), podsMetric("requests_per_second", "10")); err != nil {
		return err
	}
	if err := r.expect(ctx, "web", 100, 8, "cpu at 100% wants 8 and requests_per_second at 15 wants 6: the larger wins"); err != nil {
		return err
	}
	setRPS(25000)
	if err := r.expectDesired(ctx, "web", 10, "requests_per_second at 25 wants 10, more than cpu's 8"); err != nil {
		return err
	}

	setRPS(-1)
	r.metrics.setCPU(each(5))
	if _, err := r.await(ctx, "web", "cpu utilization 10%", func(s hpaStatus) bool {
		u, ok := s.cpuUtil()
		return ok && u == 10
	}); err != nil {
		return err
	}
	return r.holds(ctx, "web", 8*time.Second, "requests_per_second cannot be read, and cpu alone says scale down to 1 — with a metric unknown, a scale-down is not safe, so desiredReplicas must not fall below the current 4", func(s hpaStatus) bool {
		return s.DesiredReplicas != nil && *s.DesiredReplicas >= 4
	})
}
