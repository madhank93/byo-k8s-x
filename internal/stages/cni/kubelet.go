package cni

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
)

// stageKubeletDrivesIt — installed as the node's network, the plugin is what
// containerd calls when the kubelet starts and stops a pod.
//
// For the length of the stage every new pod on the first worker gets the
// learner's network; clean puts kindnet back however the stage ends.
func stageKubeletDrivesIt(w *work) error {
	list := map[string]any{"cniVersion": "1.0.0", "name": "byok8s", "plugins": []any{
		map[string]any{"type": "byok8s", "bridge": w.bridge(), "ipam": w.pool(w.subnet())},
	}}
	b, _ := json.Marshal(list)
	if _, err := w.sh(node1, "install -m 0755 "+runner.NodeBin+" /opt/cni/bin/byok8s"); err != nil {
		return err
	}
	// containerd uses the first config in the directory by name, so this one
	// wins over kindnet's 10-kindnet.conflist.
	if err := node1.WriteFile(w.ctx, "/etc/cni/net.d/00-byok8s.conflist", b, 0o644); err != nil {
		return err
	}
	image, err := w.sh(node1, `crictl images | awk '$1 ~ /pause$/ {print $1 ":" $2; exit}'`)
	if err != nil {
		return err
	}
	image = strings.TrimSpace(image)
	if image == "" {
		return fmt.Errorf("no pause image in %s to run a pod from", node1)
	}

	pods := w.env.Client.CoreV1().Pods(w.env.Namespace)
	zero := int64(0)
	pod, err := pods.Create(w.ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "cni-" + w.token},
		Spec: corev1.PodSpec{
			NodeName:                      string(node1),
			TerminationGracePeriodSeconds: &zero,
			Tolerations:                   []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			Containers: []corev1.Container{{
				Name: "pause", Image: image, ImagePullPolicy: corev1.PullNever,
			}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	defer pods.Delete(context.Background(), pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero})

	wait := func(what string, d time.Duration, f func(context.Context) (bool, error)) error {
		ctx, cancel := context.WithTimeout(w.ctx, d)
		defer cancel()
		return kube.WaitFor(ctx, what, f)
	}
	err = wait("the pod to run", 60*time.Second, func(ctx context.Context) (bool, error) {
		pod, err = pods.Get(ctx, pod.Name, metav1.GetOptions{})
		return err == nil && pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "", nil
	})
	if err != nil {
		return fmt.Errorf("%w\n%s", err, w.sandboxEvents(pod.Name))
	}
	_, pool, _ := net.ParseCIDR(w.subnet())
	if ip := net.ParseIP(pod.Status.PodIP); ip == nil || !pool.Contains(ip) {
		return fmt.Errorf("the pod's IP is %s, not from %s: the kubelet's runtime did not use the plugin", pod.Status.PodIP, w.subnet())
	}
	members := func() (int, error) {
		out, err := w.sh(node1, "ip -o link show master "+w.bridge()+" 2>/dev/null | wc -l")
		if err != nil {
			return 0, err
		}
		var n int
		fmt.Sscan(out, &n)
		return n, nil
	}
	if n, err := members(); err != nil || n == 0 {
		return fmt.Errorf("the pod runs, but nothing is attached to %s on %s (%v)", w.bridge(), node1, err)
	}

	if err := pods.Delete(w.ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		return err
	}
	if err := wait("the pod to go", 60*time.Second, func(ctx context.Context) (bool, error) {
		_, err := pods.Get(ctx, pod.Name, metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	}); err != nil {
		return err
	}
	return wait("DEL to detach the pod's veth from "+w.bridge(), 30*time.Second, func(context.Context) (bool, error) {
		n, err := members()
		return err == nil && n == 0, nil
	})
}

// sandboxEvents is what the kubelet said about a pod, which is where a CNI
// failure surfaces ("failed to setup network for sandbox …").
func (w *work) sandboxEvents(pod string) string {
	evs, err := w.env.Client.CoreV1().Events(w.env.Namespace).List(w.ctx, metav1.ListOptions{FieldSelector: "involvedObject.name=" + pod})
	if err != nil || len(evs.Items) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("events:")
	for _, e := range evs.Items {
		fmt.Fprintf(&b, "\n  %s: %s", e.Reason, e.Message)
	}
	return b.String()
}
