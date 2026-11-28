package kubelet

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
)

// image is what every stage's pods run. It is pulled once into a namespace
// the course keeps and copied into each run's own, so a stage never waits on
// a registry.
const (
	image      = "busybox:1.36"
	imageRef   = "docker.io/library/busybox:1.36"
	imageCache = "byok8s-images"
)

// seed puts the stage's image into the kubelet's containerd namespace.
func (w *work) seed() error {
	arch, err := runner.NodeArch(w.ctx)
	if err != nil {
		return err
	}
	arch = "linux/" + arch
	script := fmt.Sprintf(`set -e
ctr -n %[1]s images ls -q | grep -qx %[2]s || ctr -n %[1]s images pull --platform %[3]s %[2]s >/dev/null
ctr -n %[1]s images export --platform %[3]s - %[2]s | ctr -n %[4]s images import --platform %[3]s - >/dev/null`,
		imageCache, imageRef, arch, w.name)
	res, err := node.Exec(w.ctx, 60*time.Second, "sh", "-c", script)
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("seed %s into containerd namespace %s: %s", imageRef, w.name, res.Stderr)
	}
	return nil
}

// pod is a pod bound straight to the program's Node, in the host's network
// namespace: pod networking is the cni course's, not this one's.
func (w *work) pod(name string, restart corev1.RestartPolicy, cs ...corev1.Container) *corev1.Pod {
	for i := range cs {
		if cs[i].Image == "" {
			cs[i].Image = image
		}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: w.env.Namespace},
		Spec: corev1.PodSpec{
			NodeName:                      w.name,
			HostNetwork:                   true,
			RestartPolicy:                 restart,
			AutomountServiceAccountToken:  new(bool),
			TerminationGracePeriodSeconds: ptr(int64(2)),
			Containers:                    cs,
		},
	}
}

func (w *work) create(p *corev1.Pod) (*corev1.Pod, error) {
	got, err := w.env.Client.CoreV1().Pods(p.Namespace).Create(w.ctx, p, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create pod %s: %w", p.Name, err)
	}
	return got, nil
}

// waitPod waits until pod name satisfies ok, reporting why not.
func (w *work) waitPod(name, what string, timeout time.Duration, ok func(*corev1.Pod) error) (*corev1.Pod, error) {
	ctx, cancel := context.WithTimeout(w.ctx, timeout)
	defer cancel()
	var last error
	var got *corev1.Pod
	err := kube.WaitFor(ctx, what, func(ctx context.Context) (bool, error) {
		if err := w.alive(); err != nil {
			return false, err
		}
		p, err := w.env.Client.CoreV1().Pods(w.env.Namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			last = fmt.Errorf("there is no pod %s", name)
			return false, nil
		}
		if err != nil {
			return false, nil
		}
		got = p
		last = ok(p)
		return last == nil, nil
	})
	if err != nil && last != nil {
		return got, fmt.Errorf("%w: %v", err, last)
	}
	return got, err
}

// logPath is where the kubelet writes a container's output: one file per
// run of it, numbered by restart count.
func (w *work) logPath(p *corev1.Pod, container string, restart int) string {
	return fmt.Sprintf("%s/logs/%s_%s_%s/%s/%d.log", w.root, p.Namespace, p.Name, p.UID, container, restart)
}

// log is that file's contents, empty while it does not exist.
func (w *work) log(p *corev1.Pod, container string, restart int) string {
	res, err := node.Exec(w.ctx, 30*time.Second, "cat", w.logPath(p, container, restart))
	if err != nil || res.ExitCode != 0 {
		return ""
	}
	return res.Stdout
}

// waitLog waits for a container's log to contain want.
func (w *work) waitLog(p *corev1.Pod, container string, restart int, want string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(w.ctx, timeout)
	defer cancel()
	var got string
	err := kube.WaitFor(ctx, fmt.Sprintf("container %s of pod %s to write %q", container, p.Name, want), func(ctx context.Context) (bool, error) {
		if err := w.alive(); err != nil {
			return false, err
		}
		got = w.log(p, container, restart)
		return strings.Contains(got, want), nil
	})
	if err != nil {
		if got == "" {
			return fmt.Errorf("%w: %s is empty or missing", err, w.logPath(p, container, restart))
		}
		return fmt.Errorf("%w: %s holds:\n%s", err, w.logPath(p, container, restart), tail(got))
	}
	return nil
}

// ctrContainer is a container in the kubelet's containerd namespace, with
// its task's pid and status when it has one.
type ctrContainer struct {
	ID     string
	Labels map[string]string
	PID    int
	Status string
}

const (
	labelPodUID    = "io.kubernetes.pod.uid"
	labelContainer = "io.kubernetes.container.name"
)

// containers lists what the kubelet has in containerd.
func (w *work) containers() ([]ctrContainer, error) {
	res, err := node.Exec(w.ctx, 30*time.Second, "sh", "-c", `c="ctr -n $1"
for id in $($c c ls -q); do $c c info "$id" | tr -d '\n'; echo; done
echo ---
$c t ls`, "list", w.name)
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("list containers in containerd namespace %s: %s", w.name, res.Stderr)
	}
	infos, tasks, _ := strings.Cut(res.Stdout, "---\n")
	var out []ctrContainer
	byID := map[string]int{}
	for _, l := range strings.Split(strings.TrimSpace(infos), "\n") {
		if l == "" {
			continue
		}
		var c ctrContainer
		if err := json.Unmarshal([]byte(l), &c); err != nil {
			return nil, fmt.Errorf("read ctr container info: %w", err)
		}
		byID[c.ID] = len(out)
		out = append(out, c)
	}
	for _, l := range strings.Split(tasks, "\n")[1:] {
		f := strings.Fields(l)
		if len(f) < 3 {
			continue
		}
		if i, ok := byID[f[0]]; ok {
			out[i].PID, _ = strconv.Atoi(f[1])
			out[i].Status = f[2]
		}
	}
	return out, nil
}

// of is the containers that ran container name of pod uid.
func of(cs []ctrContainer, p *corev1.Pod, name string) []ctrContainer {
	var out []ctrContainer
	for _, c := range cs {
		if c.Labels[labelPodUID] == string(p.UID) && c.Labels[labelContainer] == name {
			out = append(out, c)
		}
	}
	return out
}

// running waits for container name of pod p to have a running task, and
// returns it.
func (w *work) running(p *corev1.Pod, name string, timeout time.Duration) (ctrContainer, error) {
	ctx, cancel := context.WithTimeout(w.ctx, timeout)
	defer cancel()
	var got ctrContainer
	var seen []ctrContainer
	err := kube.WaitFor(ctx, fmt.Sprintf("container %s of pod %s to be running in containerd", name, p.Name), func(ctx context.Context) (bool, error) {
		if err := w.alive(); err != nil {
			return false, err
		}
		cs, err := w.containers()
		if err != nil {
			return false, err
		}
		seen = cs
		for _, c := range of(cs, p, name) {
			if c.Status == "RUNNING" {
				got = c
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return got, fmt.Errorf("%w; containerd namespace %s holds %s", err, w.name, describe(seen))
	}
	return got, nil
}

func describe(cs []ctrContainer) string {
	if len(cs) == 0 {
		return "no containers"
	}
	var b []string
	for _, c := range cs {
		st := c.Status
		if st == "" {
			st = "no task"
		}
		b = append(b, fmt.Sprintf("%s (pod uid %q, container %q: %s)", c.ID, c.Labels[labelPodUID], c.Labels[labelContainer], st))
	}
	return strings.Join(b, ", ")
}

// cmdline is the argv of node pid.
func cmdline(ctx context.Context, pid int) (string, error) {
	res, err := node.Exec(ctx, 30*time.Second, "sh", "-c", `tr '\0' ' ' < /proc/$1/cmdline`, "cmdline", strconv.Itoa(pid))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(res.Stdout), nil
}

func ptr[T any](v T) *T { return &v }

// stageRunContainer — the kubelet watches the pods bound to its Node and
// runs each of their containers in containerd: the pod's command and args
// replacing the image's, labelled with the pod and container, output in a
// log file, in the host's network namespace.
func stageRunContainer(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	if _, err := w.waitNode("Node "+w.name+" to be registered", 30*time.Second, registered(w.name)); err != nil {
		return err
	}

	token := randHex(4)
	p, err := w.create(w.pod("run", corev1.RestartPolicyNever,
		corev1.Container{Name: "greet", Command: []string{"sh", "-c", `echo "$0 $1"`}, Args: []string{"hello", token}},
		corev1.Container{Name: "sleeper", Command: []string{"sleep"}, Args: []string{"3600"}},
	))
	if err != nil {
		return err
	}
	c, err := w.running(p, "sleeper", 40*time.Second)
	if err != nil {
		return err
	}
	if got, err := cmdline(ctx, c.PID); err != nil {
		return err
	} else if got != "sleep 3600" {
		return fmt.Errorf("container sleeper runs %q; its command and args are \"sleep 3600\"", got)
	}
	res, err := node.Exec(ctx, 30*time.Second, "sh", "-c", `[ "$(readlink /proc/$1/ns/net)" = "$(readlink /proc/1/ns/net)" ]`, "netns", strconv.Itoa(c.PID))
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("container sleeper has a network namespace of its own; a hostNetwork pod shares the node's")
	}
	return w.waitLog(p, "greet", 0, "hello "+token, 20*time.Second)
}

func podCondition(p *corev1.Pod, t corev1.PodConditionType) corev1.ConditionStatus {
	for _, c := range p.Status.Conditions {
		if c.Type == t {
			return c.Status
		}
	}
	return "unset"
}

func containerStatus(p *corev1.Pod, name string) *corev1.ContainerStatus {
	for i := range p.Status.ContainerStatuses {
		if p.Status.ContainerStatuses[i].Name == name {
			return &p.Status.ContainerStatuses[i]
		}
	}
	return nil
}

// phase waits for pod name to reach phase, then hands back the pod.
func (w *work) phase(name string, phase corev1.PodPhase, timeout time.Duration) (*corev1.Pod, error) {
	return w.waitPod(name, "pod "+name+" to be "+string(phase), timeout, func(p *corev1.Pod) error {
		if p.Status.Phase != phase {
			return fmt.Errorf("its phase is %q; status is %s", p.Status.Phase, statusJSON(p))
		}
		return nil
	})
}

func statusJSON(p *corev1.Pod) string {
	b, _ := json.Marshal(p.Status)
	return string(b)
}

// stagePodStatus — the kubelet writes each pod's status back: its phase,
// addresses, start time, conditions, and a status per container that names
// the containerd container and image it is running, or why it is not.
func stagePodStatus(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	facts, err := w.machine()
	if err != nil {
		return err
	}
	for _, p := range []*corev1.Pod{
		w.pod("main", corev1.RestartPolicyNever, corev1.Container{Name: "main", Command: []string{"sleep", "3600"}}),
		w.pod("ok", corev1.RestartPolicyNever, corev1.Container{Name: "main", Command: []string{"sh", "-c", "exit 0"}}),
		w.pod("fail", corev1.RestartPolicyNever, corev1.Container{Name: "main", Command: []string{"sh", "-c", "exit 3"}}),
		w.pod("nopull", corev1.RestartPolicyNever, corev1.Container{Name: "main", Image: "byok8s.invalid/nope:1"}),
	} {
		if _, err := w.create(p); err != nil {
			return err
		}
	}

	p, err := w.phase("main", corev1.PodRunning, 40*time.Second)
	if err != nil {
		return err
	}
	s := p.Status
	switch {
	case s.HostIP != facts["ip"]:
		return fmt.Errorf("pod main's status.hostIP is %q; the node's InternalIP is %s", s.HostIP, facts["ip"])
	case s.PodIP != facts["ip"]:
		return fmt.Errorf("pod main's status.podIP is %q; a hostNetwork pod has the node's address, %s", s.PodIP, facts["ip"])
	case s.StartTime == nil:
		return fmt.Errorf("pod main has no status.startTime; set it when the kubelet first takes the pod on")
	}
	for _, t := range []corev1.PodConditionType{corev1.PodScheduled, corev1.PodInitialized, corev1.ContainersReady, corev1.PodReady} {
		if got := podCondition(p, t); got != corev1.ConditionTrue {
			return fmt.Errorf("pod main's %s condition is %s; with its one container running it should be True", t, got)
		}
	}
	cs := containerStatus(p, "main")
	if cs == nil {
		return fmt.Errorf("pod main has no containerStatuses entry for container main: %s", statusJSON(p))
	}
	c, err := w.running(p, "main", 10*time.Second)
	if err != nil {
		return err
	}
	switch {
	case cs.State.Running == nil || cs.State.Running.StartedAt.IsZero():
		return fmt.Errorf("container main's state is %+v; it is running, with a startedAt", cs.State)
	case cs.ContainerID != "containerd://"+c.ID:
		return fmt.Errorf("container main's containerID is %q; it runs as containerd container %s, so containerd://%s", cs.ContainerID, c.ID, c.ID)
	case cs.Image != image:
		return fmt.Errorf("container main's image is %q; it is the spec's image, %s", cs.Image, image)
	case !strings.Contains(cs.ImageID, "@sha256:"):
		return fmt.Errorf("container main's imageID is %q; it names the image by digest, NAME@sha256:…", cs.ImageID)
	case !cs.Ready || cs.Started == nil || !*cs.Started || cs.RestartCount != 0:
		return fmt.Errorf("container main is ready=%v started=%v restartCount=%d; a running container with no probes is ready and started, and has not restarted", cs.Ready, cs.Started, cs.RestartCount)
	}

	for _, want := range []struct {
		pod    string
		phase  corev1.PodPhase
		code   int32
		reason string
	}{
		{"ok", corev1.PodSucceeded, 0, "Completed"},
		{"fail", corev1.PodFailed, 3, "Error"},
	} {
		p, err := w.phase(want.pod, want.phase, 30*time.Second)
		if err != nil {
			return err
		}
		cs := containerStatus(p, "main")
		if cs == nil || cs.State.Terminated == nil {
			return fmt.Errorf("pod %s's container main is not terminated: %s", want.pod, statusJSON(p))
		}
		t := cs.State.Terminated
		switch {
		case t.ExitCode != want.code || t.Reason != want.reason:
			return fmt.Errorf("pod %s's container main terminated with exitCode %d reason %q; expected %d %q", want.pod, t.ExitCode, t.Reason, want.code, want.reason)
		case t.StartedAt.IsZero() || t.FinishedAt.IsZero() || !strings.HasPrefix(t.ContainerID, "containerd://"):
			return fmt.Errorf("pod %s's container main terminated state %+v; it carries startedAt, finishedAt and containerID", want.pod, *t)
		case cs.Ready:
			return fmt.Errorf("pod %s's container main is ready; an exited container is not", want.pod)
		}
	}

	_, err = w.waitPod("nopull", "pod nopull to report its image pull failing", 30*time.Second, func(p *corev1.Pod) error {
		cs := containerStatus(p, "main")
		if p.Status.Phase != corev1.PodPending || cs == nil || cs.State.Waiting == nil ||
			(cs.State.Waiting.Reason != "ErrImagePull" && cs.State.Waiting.Reason != "ImagePullBackOff") {
			return fmt.Errorf("an image that cannot be pulled leaves the pod Pending with container main waiting for reason ErrImagePull or ImagePullBackOff; status is %s", statusJSON(p))
		}
		return nil
	})
	return err
}

// gone waits for pod p to leave the API server.
func (w *work) gone(p *corev1.Pod, timeout time.Duration, why string) error {
	ctx, cancel := context.WithTimeout(w.ctx, timeout)
	defer cancel()
	err := kube.WaitFor(ctx, "pod "+p.Name+" to be deleted", func(ctx context.Context) (bool, error) {
		if err := w.alive(); err != nil {
			return false, err
		}
		_, err := w.env.Client.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	})
	if err != nil {
		return fmt.Errorf("%w: %s", err, why)
	}
	return nil
}

// stopped waits until pod p has no running task left in containerd.
func (w *work) stopped(p *corev1.Pod, timeout time.Duration, why string) error {
	ctx, cancel := context.WithTimeout(w.ctx, timeout)
	defer cancel()
	var seen []ctrContainer
	err := kube.WaitFor(ctx, "pod "+p.Name+"'s containers to stop", func(ctx context.Context) (bool, error) {
		cs, err := w.containers()
		if err != nil {
			return false, err
		}
		seen = nil
		for _, c := range cs {
			if c.Labels[labelPodUID] == string(p.UID) && c.Status == "RUNNING" {
				seen = append(seen, c)
			}
		}
		return len(seen) == 0, nil
	})
	if err != nil {
		return fmt.Errorf("%w: %s; still running: %s", err, why, describe(seen))
	}
	return nil
}

// loop is a container that runs until it is killed; on SIGTERM it says so
// and exits, unless trap is empty, when it ignores SIGTERM.
func loop(trap string) []string {
	return []string{"sh", "-c", fmt.Sprintf(`trap '%s' TERM; echo up; while true; do sleep 0.2; done`, trap)}
}

// stageGracefulDelete — deleting a pod is a request to the kubelet: it
// sends each container SIGTERM, waits out the grace period, kills what is
// left, and only then deletes the pod for good. A pod deleted outright is
// killed at once.
func stageGracefulDelete(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	pods := w.env.Client.CoreV1().Pods(w.env.Namespace)

	term := w.pod("term", corev1.RestartPolicyAlways, corev1.Container{Name: "main", Command: loop("echo got TERM; exit 0")})
	term.Spec.TerminationGracePeriodSeconds = ptr(int64(60))
	stubborn := w.pod("stubborn", corev1.RestartPolicyAlways, corev1.Container{Name: "main", Command: loop("")})
	stubborn.Spec.TerminationGracePeriodSeconds = ptr(int64(4))
	forced := w.pod("forced", corev1.RestartPolicyAlways, corev1.Container{Name: "main", Command: loop("")})
	forced.Spec.TerminationGracePeriodSeconds = ptr(int64(60))
	var created []*corev1.Pod
	for _, p := range []*corev1.Pod{term, stubborn, forced} {
		p, err := w.create(p)
		if err != nil {
			return err
		}
		created = append(created, p)
	}
	for _, p := range created {
		if err := w.waitLog(p, "main", 0, "up", 40*time.Second); err != nil {
			return err
		}
	}
	term, stubborn, forced = created[0], created[1], created[2]

	for _, p := range created[:2] {
		if err := pods.Delete(ctx, p.Name, metav1.DeleteOptions{}); err != nil {
			return err
		}
	}
	if err := w.waitLog(term, "main", 0, "got TERM", 10*time.Second); err != nil {
		return fmt.Errorf("pod term was deleted, so its container gets SIGTERM: %w", err)
	}
	if err := w.gone(term, 15*time.Second, "its container exited on SIGTERM, so the kubelet deletes the pod then, not at the end of its 60-second grace period"); err != nil {
		return err
	}

	// Two seconds into a four-second grace period, the container that
	// ignores SIGTERM is still running and its pod still exists.
	time.Sleep(2 * time.Second)
	if _, err := pods.Get(ctx, stubborn.Name, metav1.GetOptions{}); err != nil {
		return fmt.Errorf("pod stubborn is gone two seconds into its four-second grace period: %v; a container that ignores SIGTERM gets the whole period", err)
	}
	if _, err := w.running(stubborn, "main", 5*time.Second); err != nil {
		return fmt.Errorf("two seconds into its four-second grace period: %w", err)
	}
	if err := w.stopped(stubborn, 10*time.Second, "the grace period is over, so the kubelet sends SIGKILL"); err != nil {
		return err
	}
	if err := w.gone(stubborn, 10*time.Second, "its container has been killed, so the kubelet deletes the pod"); err != nil {
		return err
	}

	if err := pods.Delete(ctx, forced.Name, metav1.DeleteOptions{GracePeriodSeconds: new(int64)}); err != nil {
		return err
	}
	return w.stopped(forced, 10*time.Second, "pod forced was deleted with --grace-period=0 and is gone from the API, so the kubelet kills its container at once")
}

// stageRestartPolicy — the kubelet runs an exited container again when the
// pod's restartPolicy says to, a fresh containerd container per run, and
// backs off between runs of one that keeps exiting: 10s, then 20s, doubling.
func stageRestartPolicy(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	exit := func(code int) []string { return []string{"sh", "-c", fmt.Sprintf("echo run; exit %d", code)} }
	var crash *corev1.Pod
	for _, p := range []*corev1.Pod{
		w.pod("crash", corev1.RestartPolicyAlways, corev1.Container{Name: "main", Command: exit(1)}),
		w.pod("always-ok", corev1.RestartPolicyAlways, corev1.Container{Name: "main", Command: exit(0)}),
		w.pod("onfailure-ok", corev1.RestartPolicyOnFailure, corev1.Container{Name: "main", Command: exit(0)}),
		w.pod("onfailure-bad", corev1.RestartPolicyOnFailure, corev1.Container{Name: "main", Command: exit(2)}),
	} {
		p, err := w.create(p)
		if err != nil {
			return err
		}
		if p.Name == "crash" {
			crash = p
		}
	}

	// Watch crash through two restarts, noting when each happened and that
	// it waited in CrashLoopBackOff between them.
	var at [3]time.Time
	at[0] = time.Now()
	backedOff := false
	var last *corev1.Pod
	ctx2, cancel := context.WithTimeout(ctx, 50*time.Second)
	defer cancel()
	err = kube.WaitFor(ctx2, "pod crash to restart twice", func(ctx context.Context) (bool, error) {
		if err := w.alive(); err != nil {
			return false, err
		}
		p, err := w.env.Client.CoreV1().Pods(w.env.Namespace).Get(ctx, "crash", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		last = p
		cs := containerStatus(p, "main")
		if cs == nil {
			return false, nil
		}
		if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
			backedOff = true
		}
		if n := cs.RestartCount; n > 0 && n < 3 && at[n].IsZero() {
			at[n] = time.Now()
		}
		return cs.RestartCount >= 2, nil
	})
	if err != nil {
		return fmt.Errorf("%w: restartPolicy Always runs an exited container again; status is %s", err, statusJSON(last))
	}
	cs := containerStatus(last, "main")
	switch {
	case last.Status.Phase != corev1.PodRunning:
		return fmt.Errorf("pod crash is %s; a pod whose containers will run again is Running", last.Status.Phase)
	case cs.LastTerminationState.Terminated == nil || cs.LastTerminationState.Terminated.ExitCode != 1:
		return fmt.Errorf("container main's lastState is %+v; it is the run that exited 1", cs.LastTerminationState)
	case !backedOff:
		return fmt.Errorf("container main never showed state waiting with reason CrashLoopBackOff between restarts")
	case at[2].Sub(at[1]) < 14*time.Second:
		return fmt.Errorf("container main restarted %s after its first restart; the back-off doubles to 20s", at[2].Sub(at[1]).Round(time.Second))
	}
	for n := range 3 {
		if !strings.Contains(w.log(crash, "main", n), "run") {
			return fmt.Errorf("%s does not hold run %d's output; each run logs to a file of its own", w.logPath(crash, "main", n), n)
		}
	}
	cs2, err := w.containers()
	if err != nil {
		return err
	}
	if n := len(of(cs2, crash, "main")); n < 2 {
		return fmt.Errorf("containerd holds %d containers for crash's main; each run is a new container, and the last one stays for lastState", n)
	}

	if _, err := w.waitPod("always-ok", "pod always-ok to restart", 5*time.Second, func(p *corev1.Pod) error {
		if cs := containerStatus(p, "main"); cs == nil || cs.RestartCount == 0 {
			return fmt.Errorf("restartPolicy Always runs a container again even after it exits 0")
		}
		return nil
	}); err != nil {
		return err
	}
	p, err := w.phase("onfailure-ok", corev1.PodSucceeded, 5*time.Second)
	if err != nil {
		return err
	}
	if cs := containerStatus(p, "main"); cs.RestartCount != 0 {
		return fmt.Errorf("pod onfailure-ok restarted a container that exited 0")
	}
	_, err = w.waitPod("onfailure-bad", "pod onfailure-bad to restart", 5*time.Second, func(p *corev1.Pod) error {
		if cs := containerStatus(p, "main"); cs == nil || cs.RestartCount == 0 || p.Status.Phase != corev1.PodRunning {
			return fmt.Errorf("restartPolicy OnFailure runs a container that exited 2 again, and the pod stays Running; status is %s", statusJSON(p))
		}
		return nil
	})
	return err
}

func initStatus(p *corev1.Pod, name string) *corev1.ContainerStatus {
	for i := range p.Status.InitContainerStatuses {
		if p.Status.InitContainerStatuses[i].Name == name {
			return &p.Status.InitContainerStatuses[i]
		}
	}
	return nil
}

// stageInitContainers — init containers run one at a time, in order, each to
// a clean exit, before any app container starts; the pod is Pending and not
// Initialized until they have, and a failed one is retried or fails the pod
// as its restartPolicy says.
func stageInitContainers(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	ordered := w.pod("ordered", corev1.RestartPolicyAlways, corev1.Container{Name: "app", Command: []string{"sleep", "3600"}})
	ordered.Spec.InitContainers = []corev1.Container{
		{Name: "first", Image: image, Command: []string{"sh", "-c", "sleep 4"}},
		{Name: "second", Image: image, Command: []string{"sh", "-c", "sleep 2"}},
	}
	fail := w.pod("fail", corev1.RestartPolicyNever, corev1.Container{Name: "app", Command: []string{"sleep", "3600"}})
	fail.Spec.InitContainers = []corev1.Container{{Name: "first", Image: image, Command: []string{"sh", "-c", "exit 1"}}}
	retry := w.pod("retry", corev1.RestartPolicyOnFailure, corev1.Container{Name: "app", Command: []string{"sleep", "3600"}})
	retry.Spec.InitContainers = []corev1.Container{{Name: "first", Image: image, Command: []string{"sh", "-c", "exit 1"}}}
	pods := map[string]*corev1.Pod{}
	for _, p := range []*corev1.Pod{ordered, fail, retry} {
		got, err := w.create(p)
		if err != nil {
			return err
		}
		pods[p.Name] = got
	}

	// While the first init container runs, nothing else does.
	if _, err := w.running(pods["ordered"], "first", 30*time.Second); err != nil {
		return err
	}
	p, err := w.waitPod("ordered", "pod ordered to report it is initializing", 5*time.Second, func(p *corev1.Pod) error {
		cs := containerStatus(p, "app")
		switch {
		case p.Status.Phase != corev1.PodPending || podCondition(p, corev1.PodInitialized) != corev1.ConditionFalse:
			return fmt.Errorf("while init container first runs the pod is Pending and Initialized is False; phase is %q, Initialized %s", p.Status.Phase, podCondition(p, corev1.PodInitialized))
		case initStatus(p, "first") == nil || initStatus(p, "first").State.Running == nil:
			return fmt.Errorf("status.initContainerStatuses should show first running: %s", statusJSON(p))
		case cs == nil || cs.State.Waiting == nil || cs.State.Waiting.Reason != "PodInitializing":
			return fmt.Errorf("app container app waits for reason PodInitializing while init containers run: %s", statusJSON(p))
		}
		return nil
	})
	if err != nil {
		return err
	}
	cs, err := w.containers()
	if err != nil {
		return err
	}
	if n := len(of(cs, p, "second")) + len(of(cs, p, "app")); n != 0 {
		return fmt.Errorf("while init container first runs, containerd already holds %d containers for second or app; they wait their turn", n)
	}

	p, err = w.waitPod("ordered", "pod ordered to be Running", 30*time.Second, func(p *corev1.Pod) error {
		if p.Status.Phase != corev1.PodRunning || podCondition(p, corev1.PodInitialized) != corev1.ConditionTrue {
			return fmt.Errorf("once its init containers are done the pod is Initialized and Running; status is %s", statusJSON(p))
		}
		return nil
	})
	if err != nil {
		return err
	}
	first, second, app := initStatus(p, "first"), initStatus(p, "second"), containerStatus(p, "app")
	switch {
	case first == nil || second == nil || first.State.Terminated == nil || second.State.Terminated == nil:
		return fmt.Errorf("both init containers should show terminated: %s", statusJSON(p))
	case first.State.Terminated.ExitCode != 0 || second.State.Terminated.ExitCode != 0:
		return fmt.Errorf("both init containers exited 0: %s", statusJSON(p))
	case second.State.Terminated.StartedAt.Before(&first.State.Terminated.FinishedAt):
		return fmt.Errorf("init container second started at %s, before first finished at %s; they run one at a time", second.State.Terminated.StartedAt, first.State.Terminated.FinishedAt)
	case app.State.Running == nil || app.State.Running.StartedAt.Before(&second.State.Terminated.FinishedAt):
		return fmt.Errorf("container app should start after the last init container finishes: %s", statusJSON(p))
	}

	p, err = w.phase("fail", corev1.PodFailed, 20*time.Second)
	if err != nil {
		return fmt.Errorf("an init container that fails in a restartPolicy Never pod fails the pod: %w", err)
	}
	if cs, err := w.containers(); err != nil {
		return err
	} else if len(of(cs, p, "app")) != 0 {
		return fmt.Errorf("pod fail's init container failed, yet its app container was created")
	}
	_, err = w.waitPod("retry", "pod retry to retry its init container", 20*time.Second, func(p *corev1.Pod) error {
		s := initStatus(p, "first")
		if p.Status.Phase != corev1.PodPending || s == nil || s.RestartCount == 0 {
			return fmt.Errorf("restartPolicy OnFailure runs a failed init container again, and the pod stays Pending; status is %s", statusJSON(p))
		}
		return nil
	})
	return err
}

// envOf parses the output of env into a map.
func envOf(out string) map[string]string {
	m := map[string]string{}
	for _, l := range strings.Split(out, "\n") {
		if k, v, ok := strings.Cut(l, "="); ok {
			m[k] = v
		}
	}
	return m
}

// stageEnv — the kubelet builds each container's environment: literal
// values with $(VAR) references expanded, values from the pod's own fields
// and resources, from ConfigMaps and Secrets, and whole ConfigMaps by
// envFrom, on top of the image's own environment.
func stageEnv(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	cs := env.Client.CoreV1()
	token := randHex(4)
	if _, err := cs.ConfigMaps(env.Namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "settings"},
		Data:       map[string]string{"color": "blue-" + token, "A": "1", "B": "2"},
	}, metav1.CreateOptions{}); err != nil {
		return err
	}
	if _, err := cs.Secrets(env.Namespace).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds"},
		Data:       map[string][]byte{"password": []byte("s3cret-" + token)},
	}, metav1.CreateOptions{}); err != nil {
		return err
	}
	field := func(path string) *corev1.EnvVarSource {
		return &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: path}}
	}
	c := corev1.Container{
		Name:    "main",
		Command: []string{"sh", "-c", `env; echo "expanded=$(GREETING)"; sleep 3600`},
		Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		}},
		EnvFrom: []corev1.EnvFromSource{{Prefix: "CM_", ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "settings"}}}},
		Env: []corev1.EnvVar{
			{Name: "GREETING", Value: "hello"},
			{Name: "COMPOSED", Value: "$(GREETING)-there"},
			{Name: "ESCAPED", Value: "$$(GREETING)"},
			{Name: "UNDEFINED", Value: "$(NOPE)"},
			{Name: "POD_NAME", ValueFrom: field("metadata.name")},
			{Name: "POD_NAMESPACE", ValueFrom: field("metadata.namespace")},
			{Name: "POD_UID", ValueFrom: field("metadata.uid")},
			{Name: "NODE_NAME", ValueFrom: field("spec.nodeName")},
			{Name: "HOST_IP", ValueFrom: field("status.hostIP")},
			{Name: "POD_IP", ValueFrom: field("status.podIP")},
			{Name: "APP_LABEL", ValueFrom: field("metadata.labels['app']")},
			{Name: "MEM_LIMIT", ValueFrom: &corev1.EnvVarSource{ResourceFieldRef: &corev1.ResourceFieldSelector{Resource: "limits.memory"}}},
			{Name: "COLOR", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "settings"}, Key: "color"}}},
			{Name: "PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "creds"}, Key: "password"}}},
			{Name: "CM_A", Value: "overridden"},
		},
	}
	pod := w.pod("env", corev1.RestartPolicyNever, c)
	pod.Labels = map[string]string{"app": "web-" + token}
	missing := w.pod("missing", corev1.RestartPolicyNever, corev1.Container{
		Name: "main", Command: []string{"sleep", "3600"},
		Env: []corev1.EnvVar{{Name: "X", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "absent"}, Key: "x"}}}},
	})
	p, err := w.create(pod)
	if err != nil {
		return err
	}
	if _, err := w.create(missing); err != nil {
		return err
	}
	facts, err := w.machine()
	if err != nil {
		return err
	}
	if err := w.waitLog(p, "main", 0, "expanded=", 40*time.Second); err != nil {
		return err
	}
	got := envOf(w.log(p, "main", 0))
	for _, want := range [][2]string{
		{"GREETING", "hello"},
		{"COMPOSED", "hello-there"},
		{"ESCAPED", "$(GREETING)"},
		{"UNDEFINED", "$(NOPE)"},
		{"expanded", "hello"},
		{"POD_NAME", "env"},
		{"POD_NAMESPACE", env.Namespace},
		{"POD_UID", string(p.UID)},
		{"NODE_NAME", w.name},
		{"HOST_IP", facts["ip"]},
		{"POD_IP", facts["ip"]},
		{"APP_LABEL", "web-" + token},
		{"MEM_LIMIT", "67108864"},
		{"COLOR", "blue-" + token},
		{"PASSWORD", "s3cret-" + token},
		{"CM_A", "overridden"},
		{"CM_B", "2"},
		{"CM_color", "blue-" + token},
	} {
		if got[want[0]] != want[1] {
			return fmt.Errorf("in container main, %s is %q; expected %q\nthe container's environment:\n%s", want[0], got[want[0]], want[1], tail(w.log(p, "main", 0)))
		}
	}
	if !strings.Contains(got["PATH"], "/bin") {
		return fmt.Errorf("in container main, PATH is %q; the image's own environment comes first, and busybox sets PATH", got["PATH"])
	}

	_, err = w.waitPod("missing", "pod missing to report its missing ConfigMap", 20*time.Second, func(p *corev1.Pod) error {
		cs := containerStatus(p, "main")
		if cs == nil || cs.State.Waiting == nil || cs.State.Waiting.Reason != "CreateContainerConfigError" {
			return fmt.Errorf("an env var from a ConfigMap that does not exist leaves the container waiting with reason CreateContainerConfigError; status is %s", statusJSON(p))
		}
		return nil
	})
	return err
}

// nodeTest runs test(1) in the node, true when it exits 0.
func nodeTest(ctx context.Context, args ...string) (bool, error) {
	res, err := node.Exec(ctx, 30*time.Second, append([]string{"test"}, args...)...)
	if err != nil {
		return false, err
	}
	return res.ExitCode == 0, nil
}

// stageVolumes — the kubelet makes a pod's emptyDir and hostPath volumes
// and mounts them where each container asks: an emptyDir shared by the
// pod's containers and removed with the pod, in memory when asked, and a
// hostPath of the node's own, read-only when the mount says so.
func stageVolumes(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	token := randHex(4)
	hostDir := w.root + "/host-" + token
	created := w.root + "/created-" + token
	if err := node.WriteFile(ctx, hostDir+"/in", []byte("from-host-"+token+"\n"), 0o644); err != nil {
		return err
	}

	p := w.pod("volumes", corev1.RestartPolicyNever,
		corev1.Container{
			Name:         "writer",
			Command:      []string{"sh", "-c", "echo shared-" + token + " > /data/msg; sleep 3600"},
			VolumeMounts: []corev1.VolumeMount{{Name: "shared", MountPath: "/data"}},
		},
		corev1.Container{
			Name: "reader",
			Command: []string{"sh", "-c", `until [ -f /data/msg ]; do sleep 0.2; done; cat /data/msg; cat /host/in
touch /host/x 2>/dev/null && echo host=writable || echo host=readonly
echo made > /made/file
grep ' /cache ' /proc/mounts; echo done; sleep 3600`},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "shared", MountPath: "/data"},
				{Name: "host", MountPath: "/host", ReadOnly: true},
				{Name: "made", MountPath: "/made"},
				{Name: "cache", MountPath: "/cache"},
			},
		},
	)
	dirOrCreate := corev1.HostPathDirectoryOrCreate
	p.Spec.Volumes = []corev1.Volume{
		{Name: "shared", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "host", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: hostDir}}},
		{Name: "made", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: created, Type: &dirOrCreate}}},
		{Name: "cache", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}}},
	}
	if p, err = w.create(p); err != nil {
		return err
	}
	if err := w.waitLog(p, "reader", 0, "done", 40*time.Second); err != nil {
		return err
	}
	out := w.log(p, "reader", 0)
	for _, want := range []struct{ text, why string }{
		{"shared-" + token, "container reader reads what writer wrote to the emptyDir both mount at /data"},
		{"from-host-" + token, "container reader reads the node's " + hostDir + "/in through the hostPath volume at /host"},
		{"host=readonly", "the hostPath mount at /host is readOnly, so a write to it fails"},
		{"tmpfs", "the emptyDir at /cache has medium Memory, so it is a tmpfs"},
	} {
		if !strings.Contains(out, want.text) {
			return fmt.Errorf("%s; its output was:\n%s", want.why, tail(out))
		}
	}
	if ok, err := nodeTest(ctx, "-f", created+"/file"); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("hostPath type DirectoryOrCreate should create %s on the node, and what reader wrote to /made should land in it", created)
	}
	emptyDir := fmt.Sprintf("%s/pods/%s/volumes/kubernetes.io~empty-dir/shared", w.root, p.UID)
	if ok, err := nodeTest(ctx, "-f", emptyDir+"/msg"); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("the emptyDir shared lives in the pod's directory on the node, %s; there is no msg there", emptyDir)
	}

	if err := w.env.Client.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{}); err != nil {
		return err
	}
	if err := w.gone(p, 20*time.Second, "the pod was deleted"); err != nil {
		return err
	}
	podDir := fmt.Sprintf("%s/pods/%s", w.root, p.UID)
	ctx2, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := kube.WaitFor(ctx2, "the pod's directory to be removed", func(ctx context.Context) (bool, error) {
		ok, err := nodeTest(ctx, "-e", podDir)
		return !ok, err
	}); err != nil {
		return fmt.Errorf("%w: %s still exists; an emptyDir is deleted with its pod", err, podDir)
	}
	if ok, err := nodeTest(ctx, "-f", hostDir+"/in"); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("%s/in is gone; a hostPath belongs to the node and outlives the pod", hostDir)
	}
	return nil
}

// stageConfigVolumes — ConfigMap and Secret volumes put an object's keys in
// files: every key, or the items asked for at the paths asked for, with
// the file modes asked for, Secrets on tmpfs, and the files following the
// object when it changes.
func stageConfigVolumes(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	cms := env.Client.CoreV1().ConfigMaps(env.Namespace)
	token := randHex(4)
	cm, err := cms.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "app-config"},
		Data:       map[string]string{"app.conf": "v1-" + token, "other": "x"},
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	if _, err := env.Client.CoreV1().Secrets(env.Namespace).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "app-secret"},
		Data:       map[string][]byte{"token": []byte("sek-" + token)},
	}, metav1.CreateOptions{}); err != nil {
		return err
	}
	p := w.pod("config", corev1.RestartPolicyNever, corev1.Container{
		Name: "main",
		Command: []string{"sh", "-c", `stat -L -c 'mode %a %n' /items/nested/renamed.conf /sec/token
echo "secret=$(cat /sec/token)"; echo "other=$(cat /cfg/other)"
grep -q ' /sec tmpfs ' /proc/mounts && echo sec=tmpfs
echo "optional=$(ls /opt | wc -l)"
while true; do echo "cfg=$(cat /cfg/app.conf)"; sleep 1; done`},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "cfg", MountPath: "/cfg"},
			{Name: "items", MountPath: "/items"},
			{Name: "sec", MountPath: "/sec"},
			{Name: "opt", MountPath: "/opt"},
		},
	})
	ref := func(name string) corev1.LocalObjectReference { return corev1.LocalObjectReference{Name: name} }
	p.Spec.Volumes = []corev1.Volume{
		{Name: "cfg", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: ref("app-config")}}},
		{Name: "items", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: ref("app-config"),
			Items:                []corev1.KeyToPath{{Key: "app.conf", Path: "nested/renamed.conf", Mode: ptr(int32(0o600))}},
		}}},
		{Name: "sec", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "app-secret", DefaultMode: ptr(int32(0o400))}}},
		{Name: "opt", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: ref("absent"), Optional: ptr(true)}}},
	}
	if p, err = w.create(p); err != nil {
		return err
	}
	missing := w.pod("missing", corev1.RestartPolicyNever, corev1.Container{
		Name: "main", Command: []string{"sleep", "3600"},
		VolumeMounts: []corev1.VolumeMount{{Name: "cfg", MountPath: "/cfg"}},
	})
	missing.Spec.Volumes = []corev1.Volume{{Name: "cfg", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: ref("absent")}}}}
	if missing, err = w.create(missing); err != nil {
		return err
	}

	if err := w.waitLog(p, "main", 0, "cfg=v1-"+token, 40*time.Second); err != nil {
		return err
	}
	out := w.log(p, "main", 0)
	for _, want := range []struct{ text, why string }{
		{"mode 600 /items/nested/renamed.conf", "item app.conf is at path nested/renamed.conf with mode 0600"},
		{"mode 400 /sec/token", "the Secret volume's defaultMode is 0400"},
		{"secret=sek-" + token, "the Secret's key token is the file /sec/token"},
		{"other=x", "a ConfigMap volume without items holds every key"},
		{"sec=tmpfs", "a Secret volume is a tmpfs, so its contents never touch the node's disk"},
		{"optional=0", "an optional ConfigMap that does not exist is an empty volume"},
	} {
		if !strings.Contains(out, want.text) {
			return fmt.Errorf("%s; container main printed:\n%s", want.why, tail(out))
		}
	}

	cm.Data["app.conf"] = "v2-" + token
	if _, err := cms.Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		return err
	}
	if err := w.waitLog(p, "main", 0, "cfg=v2-"+token, 30*time.Second); err != nil {
		return fmt.Errorf("the ConfigMap changed, and its volume follows it while the pod runs: %w", err)
	}

	_, err = w.waitPod("missing", "pod missing to wait for its ConfigMap", 5*time.Second, func(p *corev1.Pod) error {
		cs := containerStatus(p, "main")
		if cs == nil || cs.State.Waiting == nil || cs.State.Waiting.Reason != "ContainerCreating" {
			return fmt.Errorf("a volume whose ConfigMap does not exist cannot be set up, so the container waits in ContainerCreating; status is %s", statusJSON(p))
		}
		return nil
	})
	if err != nil {
		return err
	}
	if cs, err := w.containers(); err != nil {
		return err
	} else if len(of(cs, missing, "main")) != 0 {
		return fmt.Errorf("pod missing's container was created though its volume could not be set up")
	}
	return nil
}

// review asks the API server who a token belongs to, for audience aud.
func (w *work) review(token, aud string) (*authenticationv1.TokenReviewStatus, error) {
	tr := &authenticationv1.TokenReview{Spec: authenticationv1.TokenReviewSpec{Token: token}}
	if aud != "" {
		tr.Spec.Audiences = []string{aud}
	}
	got, err := w.env.Client.AuthenticationV1().TokenReviews().Create(w.ctx, tr, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("review the token: %w", err)
	}
	return &got.Status, nil
}

// stageServiceAccountToken — the kubelet fills projected volumes: the
// kube-api-access volume admission adds to every pod, holding a token the
// API server issued for this pod's service account and bound to the pod,
// the cluster's CA and the pod's namespace; and a token for an audience of
// the pod's choosing.
func stageServiceAccountToken(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	const sa = "/var/run/secrets/kubernetes.io/serviceaccount"
	p := w.pod("token", corev1.RestartPolicyNever, corev1.Container{
		Name: "main",
		Command: []string{"sh", "-c", `echo "token=$(cat ` + sa + `/token)"
echo "ns=$(cat ` + sa + `/namespace)"
head -1 ` + sa + `/ca.crt
echo "vault=$(cat /vault/token)"
echo done; sleep 3600`},
		VolumeMounts: []corev1.VolumeMount{{Name: "vault", MountPath: "/vault"}},
	})
	p.Spec.AutomountServiceAccountToken = nil
	p.Spec.Volumes = []corev1.Volume{{Name: "vault", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
		Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
			Audience: "vault", ExpirationSeconds: ptr(int64(600)), Path: "token",
		}}},
	}}}}
	// The namespace's default ServiceAccount is made by a controller, a
	// moment after the namespace.
	if err := kube.WaitFor(ctx, "ServiceAccount default", func(ctx context.Context) (bool, error) {
		_, err := env.Client.CoreV1().ServiceAccounts(env.Namespace).Get(ctx, "default", metav1.GetOptions{})
		return err == nil, nil
	}); err != nil {
		return err
	}
	if p, err = w.create(p); err != nil {
		return err
	}
	if err := w.waitLog(p, "main", 0, "done", 40*time.Second); err != nil {
		return err
	}
	out := envOf(w.log(p, "main", 0))
	log := w.log(p, "main", 0)
	if out["ns"] != env.Namespace {
		return fmt.Errorf("%s/namespace holds %q; it is the pod's namespace, %s\ncontainer main printed:\n%s", sa, out["ns"], env.Namespace, tail(log))
	}
	if !strings.Contains(log, "-----BEGIN CERTIFICATE-----") {
		return fmt.Errorf("%s/ca.crt should be the cluster CA from ConfigMap kube-root-ca.crt\ncontainer main printed:\n%s", sa, tail(log))
	}
	st, err := w.review(out["token"], "")
	if err != nil {
		return err
	}
	user := "system:serviceaccount:" + env.Namespace + ":default"
	switch {
	case !st.Authenticated:
		return fmt.Errorf("the API server does not accept %s/token (%s); request it with the TokenRequest API for the pod's service account", sa, st.Error)
	case st.User.Username != user:
		return fmt.Errorf("%s/token authenticates as %s; it is the pod's service account, %s", sa, st.User.Username, user)
	case !slices.Equal(st.User.Extra["authentication.kubernetes.io/pod-name"], []string{p.Name}):
		return fmt.Errorf("%s/token is not bound to pod %s (its extra is %v); set the TokenRequest's boundObjectRef to the pod", sa, p.Name, st.User.Extra)
	}
	if st, err := w.review(out["vault"], "vault"); err != nil {
		return err
	} else if !st.Authenticated || !slices.Contains(st.Audiences, "vault") {
		return fmt.Errorf("/vault/token is not a token for audience vault (authenticated %v, audiences %v, %s)", st.Authenticated, st.Audiences, st.Error)
	}
	return nil
}

// cgroupFile reads a cgroup v2 interface file of node pid's cgroup.
func cgroupFile(ctx context.Context, pid int, name string) (string, error) {
	res, err := node.Exec(ctx, 30*time.Second, "sh", "-c", `cat "/sys/fs/cgroup$(sed -n 's/^0:://p' /proc/$1/cgroup)/$2"`, "cg", strconv.Itoa(pid), name)
	if err != nil {
		return "", err
	}
	if res.ExitCode != 0 {
		return "", fmt.Errorf("read %s of pid %d's cgroup: %s", name, pid, res.Stderr)
	}
	return strings.TrimSpace(res.Stdout), nil
}

// stageResources — the kubelet admits a pod only if its requests fit what
// the node has left, turns limits into the container's cgroup, and says so
// when the kernel kills a container for going over its memory limit.
func stageResources(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	res := func(limCPU, limMem, reqCPU string) corev1.ResourceRequirements {
		r := corev1.ResourceRequirements{Limits: corev1.ResourceList{}, Requests: corev1.ResourceList{}}
		if limCPU != "" {
			r.Limits[corev1.ResourceCPU] = resource.MustParse(limCPU)
		}
		if limMem != "" {
			r.Limits[corev1.ResourceMemory] = resource.MustParse(limMem)
			r.Requests[corev1.ResourceMemory] = resource.MustParse(limMem)
		}
		if reqCPU != "" {
			r.Requests[corev1.ResourceCPU] = resource.MustParse(reqCPU)
		}
		return r
	}
	limited, err := w.create(w.pod("limited", corev1.RestartPolicyNever, corev1.Container{
		Name: "main", Command: []string{"sleep", "3600"}, Resources: res("500m", "64Mi", "100m"),
	}))
	if err != nil {
		return err
	}
	oom, err := w.create(w.pod("oom", corev1.RestartPolicyNever, corev1.Container{
		Name: "main", Command: []string{"sh", "-c", "dd if=/dev/zero of=/dev/null bs=64M count=1"}, Resources: res("", "16Mi", ""),
	}))
	if err != nil {
		return err
	}

	c, err := w.running(limited, "main", 40*time.Second)
	if err != nil {
		return err
	}
	for _, want := range [][3]string{
		{"memory.max", "67108864", "limits.memory 64Mi"},
		{"cpu.max", "50000 100000", "limits.cpu 500m: 50ms of every 100ms"},
	} {
		got, err := cgroupFile(ctx, c.PID, want[0])
		if err != nil {
			return err
		}
		if got != want[1] {
			return fmt.Errorf("container main of pod limited has %s %q in its cgroup; %s is %q", want[0], got, want[2], want[1])
		}
	}

	p, err := w.phase("oom", corev1.PodFailed, 30*time.Second)
	if err != nil {
		return err
	}
	if cs := containerStatus(p, "main"); cs == nil || cs.State.Terminated == nil || cs.State.Terminated.Reason != "OOMKilled" || cs.State.Terminated.ExitCode != 137 {
		return fmt.Errorf("container main of pod oom went over its 16Mi limit and the kernel killed it, so it terminated with exitCode 137 and reason OOMKilled; status is %s", statusJSON(p))
	}
	_ = oom

	// Ask for more CPU than the node has: the kubelet turns it away.
	n, err := w.env.Client.CoreV1().Nodes().Get(ctx, w.name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	cpu := n.Status.Allocatable[corev1.ResourceCPU]
	big := cpu.DeepCopy()
	big.Add(resource.MustParse("1"))
	tooBig := w.pod("toobig", corev1.RestartPolicyNever, corev1.Container{Name: "main", Command: []string{"sleep", "3600"}})
	tooBig.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: big}
	if tooBig, err = w.create(tooBig); err != nil {
		return err
	}
	p, err = w.phase("toobig", corev1.PodFailed, 20*time.Second)
	if err != nil {
		return fmt.Errorf("pod toobig requests %s CPUs on a node with %s: %w", big.String(), cpu.String(), err)
	}
	if p.Status.Reason != "OutOfcpu" || !strings.Contains(p.Status.Message, "cpu") {
		return fmt.Errorf("pod toobig was rejected with reason %q message %q; the reason is OutOfcpu and the message says what did not fit", p.Status.Reason, p.Status.Message)
	}
	if cs, err := w.containers(); err != nil {
		return err
	} else if len(of(cs, tooBig, "main")) != 0 {
		return fmt.Errorf("pod toobig was rejected, yet containerd holds a container for it")
	}
	return nil
}

func execProbe(period int32, threshold int32, cmd ...string) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:     corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: cmd}},
		PeriodSeconds:    period,
		FailureThreshold: threshold,
	}
}

func httpProbe(port int, path string) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromInt32(int32(port))}},
		PeriodSeconds:    1,
		FailureThreshold: 2,
	}
}

// serve is a container command that serves index.html over HTTP on port.
func serve(port int) []string {
	return []string{"sh", "-c", fmt.Sprintf("mkdir -p /www && echo ok > /www/index.html && exec httpd -f -p %d -h /www", port)}
}

// restarts is container name's restartCount in pod p, -1 with no status.
func restarts(p *corev1.Pod, name string) int32 {
	if cs := containerStatus(p, name); cs != nil {
		return cs.RestartCount
	}
	return -1
}

// stageLivenessProbe — the kubelet runs each container's liveness probe,
// exec or HTTP, and kills a container that fails it enough times so it
// runs again; a startup probe holds the liveness probe off until it passes,
// and kills a container that never does.
func stageLivenessProbe(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	port := 20000 + int(randByte())*20
	pod := func(name string, c corev1.Container) *corev1.Pod {
		c.Name = "main"
		return w.pod(name, corev1.RestartPolicyAlways, c)
	}
	sleep := []string{"sleep", "3600"}
	for _, p := range []*corev1.Pod{
		pod("dies", corev1.Container{
			Command:       []string{"sh", "-c", "touch /tmp/healthy; sleep 3; rm /tmp/healthy; sleep 3600"},
			LivenessProbe: execProbe(1, 2, "cat", "/tmp/healthy"),
		}),
		pod("healthy", corev1.Container{Command: sleep, LivenessProbe: execProbe(1, 1, "true")}),
		pod("slow-start", corev1.Container{
			Command:       []string{"sh", "-c", "sleep 4; touch /tmp/started; sleep 3600"},
			StartupProbe:  execProbe(1, 15, "cat", "/tmp/started"),
			LivenessProbe: execProbe(1, 1, "cat", "/tmp/started"),
		}),
		pod("never-starts", corev1.Container{Command: sleep, StartupProbe: execProbe(1, 3, "false")}),
		pod("http-ok", corev1.Container{Command: serve(port), LivenessProbe: httpProbe(port, "/index.html")}),
		pod("http-bad", corev1.Container{Command: serve(port + 1), LivenessProbe: httpProbe(port+1, "/missing")}),
	} {
		if _, err := w.create(p); err != nil {
			return err
		}
	}

	// slow-start reports started only once its startup probe passes.
	if _, err := w.waitPod("slow-start", "pod slow-start to run, not yet started", 30*time.Second, func(p *corev1.Pod) error {
		cs := containerStatus(p, "main")
		if cs == nil || cs.State.Running == nil || cs.Started == nil || *cs.Started {
			return fmt.Errorf("while its startup probe fails, a running container reports started=false; status is %s", statusJSON(p))
		}
		return nil
	}); err != nil {
		return err
	}

	for _, name := range []string{"dies", "never-starts", "http-bad"} {
		why := map[string]string{
			"dies":         "its liveness probe fails twice in a row from three seconds in, so the kubelet kills the container and it runs again",
			"never-starts": "its startup probe fails three times, so the kubelet kills the container and it runs again",
			"http-bad":     "its HTTP liveness probe gets 404, a failure, so the kubelet kills the container and it runs again",
		}[name]
		if _, err := w.waitPod(name, "pod "+name+" to restart", 40*time.Second, func(p *corev1.Pod) error {
			if restarts(p, "main") < 1 {
				return fmt.Errorf("%s; status is %s", why, statusJSON(p))
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for _, name := range []string{"healthy", "slow-start", "http-ok"} {
		p, err := w.env.Client.CoreV1().Pods(w.env.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		cs := containerStatus(p, "main")
		switch {
		case cs == nil || cs.State.Running == nil:
			return fmt.Errorf("pod %s's container should be running: %s", name, statusJSON(p))
		case cs.RestartCount != 0:
			return fmt.Errorf("pod %s's container passes its probes, yet it restarted %d times", name, cs.RestartCount)
		case cs.Started == nil || !*cs.Started:
			return fmt.Errorf("pod %s's container should report started=true by now: %s", name, statusJSON(p))
		}
	}
	return nil
}

// stageReadinessProbe — a readiness probe decides whether a running
// container is ready, and so whether its pod is; failing it marks the
// container not ready without restarting it.
func stageReadinessProbe(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	port := 20000 + int(randByte())*20 + 10
	flap := w.pod("flap", corev1.RestartPolicyAlways, corev1.Container{
		Name:           "main",
		Command:        []string{"sh", "-c", "sleep 3; touch /tmp/ready; sleep 6; rm /tmp/ready; sleep 3600"},
		ReadinessProbe: execProbe(1, 1, "cat", "/tmp/ready"),
	})
	tcp := w.pod("tcp", corev1.RestartPolicyAlways, corev1.Container{
		Name:    "main",
		Command: serve(port),
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("web")}},
			PeriodSeconds: 1,
		},
		Ports: []corev1.ContainerPort{{Name: "web", ContainerPort: int32(port)}},
	})
	for _, p := range []*corev1.Pod{flap, tcp} {
		if _, err := w.create(p); err != nil {
			return err
		}
	}
	readiness := func(want bool, why string, timeout time.Duration) error {
		_, err := w.waitPod("flap", fmt.Sprintf("pod flap to be ready=%v", want), timeout, func(p *corev1.Pod) error {
			cs := containerStatus(p, "main")
			wantCond := corev1.ConditionFalse
			if want {
				wantCond = corev1.ConditionTrue
			}
			switch {
			case cs == nil || cs.State.Running == nil:
				return fmt.Errorf("container main should be running: %s", statusJSON(p))
			case cs.Ready != want || podCondition(p, corev1.PodReady) != wantCond || podCondition(p, corev1.ContainersReady) != wantCond:
				return fmt.Errorf("%s, so container main is ready=%v and the pod's Ready and ContainersReady are %s; status is %s", why, want, wantCond, statusJSON(p))
			}
			return nil
		})
		return err
	}
	if err := readiness(false, "its readiness probe has not passed yet", 30*time.Second); err != nil {
		return err
	}
	if err := readiness(true, "its readiness probe passes from three seconds in", 15*time.Second); err != nil {
		return err
	}
	if err := readiness(false, "its readiness probe fails again from nine seconds in", 15*time.Second); err != nil {
		return err
	}
	p, err := w.env.Client.CoreV1().Pods(w.env.Namespace).Get(ctx, "flap", metav1.GetOptions{})
	if err != nil {
		return err
	}
	if restarts(p, "main") != 0 {
		return fmt.Errorf("pod flap restarted; failing a readiness probe never restarts a container")
	}
	_, err = w.waitPod("tcp", "pod tcp to be ready", 10*time.Second, func(p *corev1.Pod) error {
		if cs := containerStatus(p, "main"); cs == nil || !cs.Ready {
			return fmt.Errorf("its tcpSocket readiness probe on the named port web (%d) connects, so it is ready; status is %s", port, statusJSON(p))
		}
		return nil
	})
	return err
}

// stageLifecycleHooks — the kubelet runs a container's postStart hook as
// soon as it starts, killing it if the hook fails, and its preStop hook
// before the SIGTERM that stops it.
func stageLifecycleHooks(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	say := func(what string) *corev1.LifecycleHandler {
		return &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"sh", "-c", "echo " + what + " > /proc/1/fd/1"}}}
	}
	hooks := w.pod("hooks", corev1.RestartPolicyAlways, corev1.Container{
		Name:      "main",
		Command:   loop("echo got-term; exit 0"),
		Lifecycle: &corev1.Lifecycle{PostStart: say("post-start"), PreStop: say("pre-stop")},
	})
	hooks.Spec.TerminationGracePeriodSeconds = ptr(int64(10))
	badHook := w.pod("bad-hook", corev1.RestartPolicyAlways, corev1.Container{
		Name:      "main",
		Command:   []string{"sleep", "3600"},
		Lifecycle: &corev1.Lifecycle{PostStart: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"false"}}}},
	})
	p, err := w.create(hooks)
	if err != nil {
		return err
	}
	if _, err := w.create(badHook); err != nil {
		return err
	}
	if err := w.waitLog(p, "main", 0, "post-start", 40*time.Second); err != nil {
		return fmt.Errorf("the postStart hook runs in the container once it has started: %w", err)
	}
	if err := w.env.Client.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{}); err != nil {
		return err
	}
	if err := w.waitLog(p, "main", 0, "got-term", 15*time.Second); err != nil {
		return err
	}
	out := w.log(p, "main", 0)
	pre, term := strings.Index(out, "pre-stop"), strings.Index(out, "got-term")
	if pre < 0 || pre > term {
		return fmt.Errorf("the preStop hook runs, and finishes, before the container gets SIGTERM; its output was:\n%s", tail(out))
	}
	_, err = w.waitPod("bad-hook", "pod bad-hook to restart", 30*time.Second, func(p *corev1.Pod) error {
		if restarts(p, "main") < 1 {
			return fmt.Errorf("its postStart hook fails, so the kubelet kills the container and it runs again; status is %s", statusJSON(p))
		}
		return nil
	})
	return err
}

// mirrorAnnotation marks a mirror pod; its value is the static pod's UID.
const mirrorAnnotation = "kubernetes.io/config.mirror"

// stageStaticPod — the kubelet runs pods from manifest files in
// --pod-manifest-path with no API server involved, and publishes each as a
// mirror pod so the cluster can see it: one it recreates if deleted, and
// takes down when the file goes.
func stageStaticPod(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	manifest := w.root + "/manifests/web.yaml"
	if err := node.WriteFile(ctx, manifest, []byte(fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: web
  namespace: %s
spec:
  hostNetwork: true
  terminationGracePeriodSeconds: 2
  containers:
  - name: main
    image: %s
    command: ["sleep", "3600"]
`, env.Namespace, image)), 0o644); err != nil {
		return err
	}
	mirror := "web-" + w.name
	m, err := w.waitPod(mirror, "mirror pod "+mirror, 40*time.Second, func(p *corev1.Pod) error {
		switch {
		case p.Annotations[mirrorAnnotation] == "":
			return fmt.Errorf("pod %s has no %s annotation, so it is not a mirror pod", p.Name, mirrorAnnotation)
		case p.Spec.NodeName != w.name:
			return fmt.Errorf("mirror pod %s is bound to %q; it is the static pod of node %s", p.Name, p.Spec.NodeName, w.name)
		case p.Status.Phase != corev1.PodRunning:
			return fmt.Errorf("mirror pod %s is %q; it carries the static pod's status, Running", p.Name, p.Status.Phase)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("a manifest in --pod-manifest-path is a static pod named NAME-NODE, published as a mirror pod: %w", err)
	}
	if !slices.ContainsFunc(m.OwnerReferences, func(o metav1.OwnerReference) bool { return o.Kind == "Node" && o.Name == w.name }) {
		return fmt.Errorf("mirror pod %s's ownerReferences are %v; the Node owns it", mirror, m.OwnerReferences)
	}
	static := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID(m.Annotations[mirrorAnnotation])}}
	c, err := w.running(static, "main", 10*time.Second)
	if err != nil {
		return fmt.Errorf("the static pod's containers are labelled with its own UID, the value of %s: %w", mirrorAnnotation, err)
	}

	// Deleting the mirror pod does not touch the static pod.
	if err := w.env.Client.CoreV1().Pods(m.Namespace).Delete(ctx, mirror, metav1.DeleteOptions{}); err != nil {
		return err
	}
	if _, err := w.waitPod(mirror, "mirror pod "+mirror+" to be recreated", 30*time.Second, func(p *corev1.Pod) error {
		if p.UID == m.UID || p.DeletionTimestamp != nil {
			return fmt.Errorf("the deleted mirror pod has not been replaced; the kubelet recreates it, since the static pod still runs")
		}
		return nil
	}); err != nil {
		return err
	}
	if again, err := w.running(static, "main", 5*time.Second); err != nil {
		return err
	} else if again.ID != c.ID {
		return fmt.Errorf("deleting the mirror pod restarted the static pod's container; the static pod is the file's, not the API's")
	}

	if _, err := node.Exec(ctx, 30*time.Second, "rm", "-f", manifest); err != nil {
		return err
	}
	if err := w.stopped(static, 30*time.Second, "the manifest is gone, so the static pod stops"); err != nil {
		return err
	}
	return w.gone(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: mirror, Namespace: m.Namespace}}, 20*time.Second, "the static pod stopped, so its mirror pod goes")
}

// stageReconcile — the kubelet's state is containerd's, not its own memory:
// stopping it leaves its pods running, and a restarted kubelet adopts the
// containers of pods that still exist and removes those whose pods are
// gone.
func stageReconcile(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	keep, err := w.create(w.pod("keep", corev1.RestartPolicyAlways, corev1.Container{Name: "main", Command: []string{"sleep", "3600"}}))
	if err != nil {
		return err
	}
	orphan, err := w.create(w.pod("orphan", corev1.RestartPolicyAlways, corev1.Container{Name: "main", Command: []string{"sleep", "3600"}}))
	if err != nil {
		return err
	}
	if _, err := w.phase("keep", corev1.PodRunning, 40*time.Second); err != nil {
		return err
	}
	c, err := w.running(keep, "main", 10*time.Second)
	if err != nil {
		return err
	}
	if _, err := w.running(orphan, "main", 10*time.Second); err != nil {
		return err
	}

	w.p.Stop(10 * time.Second)
	if again, err := w.running(keep, "main", 3*time.Second); err != nil || again.ID != c.ID {
		return fmt.Errorf("the kubelet was stopped and took pod keep's container with it; a kubelet's pods outlive it")
	}
	if err := w.env.Client.CoreV1().Pods(orphan.Namespace).Delete(ctx, orphan.Name, metav1.DeleteOptions{GracePeriodSeconds: new(int64)}); err != nil {
		return err
	}
	if err := w.launch(); err != nil {
		return err
	}

	ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var left []ctrContainer
	if err := kube.WaitFor(ctx2, "the orphan's container to be removed", func(ctx context.Context) (bool, error) {
		if err := w.alive(); err != nil {
			return false, err
		}
		cs, err := w.containers()
		if err != nil {
			return false, err
		}
		left = of(cs, orphan, "main")
		return len(left) == 0, nil
	}); err != nil {
		return fmt.Errorf("%w: pod orphan was deleted while the kubelet was down, so the restarted kubelet finds its container with no pod and removes it; containerd still holds %s", err, describe(left))
	}
	if again, err := w.running(keep, "main", 5*time.Second); err != nil {
		return err
	} else if again.ID != c.ID {
		return fmt.Errorf("pod keep's container was replaced after the restart; a running container of a pod that still exists is adopted as it is")
	}
	p, err := w.env.Client.CoreV1().Pods(keep.Namespace).Get(ctx, keep.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if p.Status.Phase != corev1.PodRunning || restarts(p, "main") != 0 || containerStatus(p, "main").ContainerID != "containerd://"+c.ID {
		return fmt.Errorf("after the restart pod keep should still be Running, on the same container, with no restarts; status is %s", statusJSON(p))
	}
	return nil
}

// stageEviction — the kubelet evicts a pod whose emptyDir outgrows its
// sizeLimit: its containers are killed, nothing restarts them, and the pod
// stays behind, Failed with reason Evicted, to say why.
func stageEviction(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	scratch := func(name, limit string, mb int) *corev1.Pod {
		p := w.pod(name, corev1.RestartPolicyAlways, corev1.Container{
			Name:         "main",
			Command:      []string{"sh", "-c", fmt.Sprintf("dd if=/dev/zero of=/data/f bs=1M count=%d; sleep 3600", mb)},
			VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
		})
		size := resource.MustParse(limit)
		p.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &size}}}}
		return p
	}
	hog, err := w.create(scratch("hog", "1Mi", 4))
	if err != nil {
		return err
	}
	if _, err := w.create(scratch("fine", "10Mi", 1)); err != nil {
		return err
	}
	p, err := w.phase("hog", corev1.PodFailed, 40*time.Second)
	if err != nil {
		return fmt.Errorf("pod hog writes 4MiB to an emptyDir with sizeLimit 1Mi, so the kubelet evicts it: %w", err)
	}
	switch {
	case p.Status.Reason != "Evicted":
		return fmt.Errorf("pod hog failed with reason %q; an evicted pod's reason is Evicted", p.Status.Reason)
	case !strings.Contains(p.Status.Message, "data"):
		return fmt.Errorf("pod hog's message is %q; it should name the volume, data, that went over its limit", p.Status.Message)
	case podCondition(p, corev1.DisruptionTarget) != corev1.ConditionTrue:
		return fmt.Errorf("an evicted pod carries condition DisruptionTarget=True (reason TerminationByKubelet); status is %s", statusJSON(p))
	}
	if err := w.stopped(hog, 15*time.Second, "pod hog was evicted, so its containers are killed"); err != nil {
		return err
	}
	time.Sleep(3 * time.Second)
	p, err = w.env.Client.CoreV1().Pods(hog.Namespace).Get(ctx, hog.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("pod hog is gone (%v); an evicted pod stays, Failed, until someone deletes it", err)
	}
	if p.Status.Phase != corev1.PodFailed || restarts(p, "main") != 0 {
		return fmt.Errorf("an evicted pod is done: nothing restarts it, whatever its restartPolicy; status is %s", statusJSON(p))
	}
	if cs, err := w.containers(); err != nil {
		return err
	} else if c := of(cs, hog, "main"); len(c) != 1 || c[0].Status == "RUNNING" {
		return fmt.Errorf("pod hog's container should have been killed and not run again; containerd holds %s", describe(c))
	}
	p, err = w.env.Client.CoreV1().Pods(hog.Namespace).Get(ctx, "fine", metav1.GetOptions{})
	if err != nil {
		return err
	}
	if p.Status.Phase != corev1.PodRunning {
		return fmt.Errorf("pod fine stays inside its 10Mi limit and should still be Running; status is %s", statusJSON(p))
	}
	return nil
}

// podLogs is what the API server's pods/log subresource returns, which it
// fetches from the kubelet named in the Node's daemonEndpoints.
func (w *work) podLogs(name string, opts *corev1.PodLogOptions) (string, error) {
	b, err := w.env.Client.CoreV1().Pods(w.env.Namespace).GetLogs(name, opts).DoRaw(w.ctx)
	return string(b), err
}

// stageLogsAPI — the kubelet serves its containers' logs over HTTPS, where
// the API server finds it through the Node's daemonEndpoints, so kubectl
// logs works: the current run, the previous one, and the last few lines.
func stageLogsAPI(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.seed(); err != nil {
		return err
	}
	if _, err := w.waitNode("Node "+w.name+" to publish its kubelet port", 30*time.Second, func(n *corev1.Node) error {
		if got := n.Status.DaemonEndpoints.KubeletEndpoint.Port; got != int32(w.port) {
			return fmt.Errorf("status.daemonEndpoints.kubeletEndpoint.port is %d; the kubelet serves on --port %d, and the API server finds it there", got, w.port)
		}
		return nil
	}); err != nil {
		return err
	}
	token := randHex(4)
	if _, err := w.create(w.pod("count", corev1.RestartPolicyAlways, corev1.Container{
		Name: "main", Command: []string{"sh", "-c", "for i in 1 2 3 4 5; do echo " + token + "-$i; done; sleep 3600"},
	})); err != nil {
		return err
	}
	if _, err := w.create(w.pod("crash", corev1.RestartPolicyAlways, corev1.Container{
		Name: "main", Command: []string{"sh", "-c", `echo "run $(cat /proc/sys/kernel/random/uuid)"; exit 1`},
	})); err != nil {
		return err
	}

	var got string
	ctx2, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	var last error
	if err := kube.WaitFor(ctx2, "kubectl logs count", func(ctx context.Context) (bool, error) {
		got, last = w.podLogs("count", &corev1.PodLogOptions{Container: "main"})
		return last == nil && strings.Contains(got, token+"-5"), nil
	}); err != nil {
		return fmt.Errorf("%w: the API server asks the kubelet for GET /containerLogs/NAMESPACE/POD/CONTAINER; got %q, %v", err, got, last)
	}
	for i := 1; i <= 5; i++ {
		if !strings.Contains(got, fmt.Sprintf("%s-%d\n", token, i)) {
			return fmt.Errorf("the logs of pod count should hold all five lines; got:\n%s", got)
		}
	}
	if got, err := w.podLogs("count", &corev1.PodLogOptions{Container: "main", TailLines: ptr(int64(2))}); err != nil {
		return err
	} else if got != fmt.Sprintf("%s-4\n%s-5\n", token, token) {
		return fmt.Errorf("with tailLines=2 the logs are the last two lines, %s-4 and %s-5; got %q", token, token, got)
	}

	if _, err := w.waitPod("crash", "pod crash to restart", 30*time.Second, func(p *corev1.Pod) error {
		if restarts(p, "main") < 1 {
			return fmt.Errorf("its container exits 1, so it runs again")
		}
		return nil
	}); err != nil {
		return err
	}
	cur, err := w.podLogs("crash", &corev1.PodLogOptions{Container: "main"})
	if err != nil {
		return err
	}
	prev, err := w.podLogs("crash", &corev1.PodLogOptions{Container: "main", Previous: true})
	if err != nil {
		return fmt.Errorf("kubectl logs --previous: %w", err)
	}
	if !strings.HasPrefix(cur, "run ") || !strings.HasPrefix(prev, "run ") || cur == prev {
		return fmt.Errorf("pod crash's logs are %q and with previous=true %q; previous is the run before the current one, which logged a different uuid", cur, prev)
	}
	return nil
}
