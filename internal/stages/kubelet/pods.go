package kubelet

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
