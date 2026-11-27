package csi

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/madhank93/byo-k8s-x/internal/kube"
)

// provisionerImage is the external-provisioner the last stage runs against the
// driver; it is what turns a PersistentVolumeClaim into CreateVolume.
const provisionerImage = "registry.k8s.io/sig-storage/csi-provisioner:v5.2.0"

// csiDriver registers the driver's CSIDriver object, which tells the kubelet
// how to treat its volumes: no attach step, and which lifecycles it serves.
func (w *work) csiDriver(mode storagev1.VolumeLifecycleMode) (func(), error) {
	f := false
	t := true
	d := &storagev1.CSIDriver{
		ObjectMeta: metav1.ObjectMeta{Name: w.name},
		Spec: storagev1.CSIDriverSpec{
			AttachRequired:       &f,
			PodInfoOnMount:       &t,
			VolumeLifecycleModes: []storagev1.VolumeLifecycleMode{mode},
		},
	}
	cs := w.env.Client.StorageV1().CSIDrivers()
	if _, err := cs.Create(w.ctx, d, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create CSIDriver %s: %w", w.name, err)
	}
	return func() { cs.Delete(context.Background(), w.name, metav1.DeleteOptions{}) }, nil
}

// pod is a busybox pod that writes marker into the volume mounted at /data.
func pod(name, marker string, vol corev1.VolumeSource) *corev1.Pod {
	grace := int64(1)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: &grace,
			NodeSelector:                  map[string]string{"kubernetes.io/hostname": nodeID},
			Containers: []corev1.Container{{
				Name:         "app",
				Image:        "busybox:1.36",
				Command:      []string{"sh", "-c", "echo " + marker + " > /data/hello && sleep 3600"},
				VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
			}},
			Volumes: []corev1.Volume{{Name: "data", VolumeSource: vol}},
		},
	}
}

// runPod creates p and waits for it to run, returning it. The cleanup it
// returns deletes the pod and waits for the kubelet to tear its volumes down,
// which needs the driver still serving.
func (w *work) runPod(p *corev1.Pod, timeout time.Duration) (*corev1.Pod, func(), error) {
	pods := w.env.Client.CoreV1().Pods(w.env.Namespace)
	if _, err := pods.Create(w.ctx, p, metav1.CreateOptions{}); err != nil {
		return nil, nil, fmt.Errorf("create pod %s: %w", p.Name, err)
	}
	cleanup := func() { w.deletePod(p.Name) }
	ctx, cancel := context.WithTimeout(w.ctx, timeout)
	defer cancel()
	var got *corev1.Pod
	err := kube.WaitFor(ctx, "pod "+p.Name+" to be Running", func(ctx context.Context) (bool, error) {
		if done, res := w.proc.Exited(); done {
			return false, fmt.Errorf("the driver exited with %d:\n%s", res.ExitCode, tail(res.Stderr))
		}
		cur, err := pods.Get(ctx, p.Name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		got = cur
		return cur.Status.Phase == corev1.PodRunning, nil
	})
	if err != nil {
		return nil, cleanup, fmt.Errorf("%w\n%s", err, w.events(p.Name))
	}
	return got, cleanup, nil
}

// deletePod deletes a pod and waits for it to go, which is when the kubelet
// has unpublished its volumes.
func (w *work) deletePod(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pods := w.env.Client.CoreV1().Pods(w.env.Namespace)
	if err := pods.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	err := kube.WaitFor(ctx, "pod "+name+" to go, which waits on the kubelet unmounting its volumes", func(ctx context.Context) (bool, error) {
		_, err := pods.Get(ctx, name, metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	})
	if err != nil {
		zero := int64(0)
		pods.Delete(context.Background(), name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
		return fmt.Errorf("%w\n%s", err, w.events(name))
	}
	return nil
}

// events is what the cluster said about an object in the stage namespace,
// for a failure message.
func (w *work) events(name string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	list, err := w.env.Client.CoreV1().Events(w.env.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return ""
	}
	var out []string
	for _, e := range list.Items {
		if e.InvolvedObject.Name == name {
			out = append(out, fmt.Sprintf("%s %s: %s", e.Type, e.Reason, e.Message))
		}
	}
	if len(out) == 0 {
		return ""
	}
	return "events for " + name + ":\n" + tail(strings.Join(out, "\n"))
}

// volumeMount is where the kubelet published a pod's CSI volume.
func volumeMount(p *corev1.Pod, volName string) string {
	return fmt.Sprintf("/var/lib/kubelet/pods/%s/volumes/kubernetes.io~csi/%s/mount", p.UID, volName)
}

func stageEphemeralInline(ctx context.Context, env *kube.Env, bin string) error {
	w, err := registered(ctx, env, "ephemeral-inline", bin)
	if err != nil {
		return err
	}
	defer w.close()
	undo, err := w.csiDriver(storagev1.VolumeLifecycleEphemeral)
	if err != nil {
		return err
	}
	defer undo()

	marker := "inline-" + randHex(4)
	p, cleanup, err := w.runPod(pod("inline", marker, corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{
		Driver:           w.name,
		VolumeAttributes: map[string]string{"size": "4Mi"},
	}}), 2*time.Minute)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return fmt.Errorf("a pod with an inline CSI volume: %w", err)
	}
	mnt := volumeMount(p, "data")
	if err := w.wantMounted(mnt, "the pod's inline volume"); err != nil {
		return err
	}
	if err := waitFile(w, mnt+"/hello", marker); err != nil {
		return err
	}
	if err := w.deletePod(p.Name); err != nil {
		return fmt.Errorf("deleting the pod: %w", err)
	}
	if err := w.wantUnmounted(mnt, "after the pod was deleted"); err != nil {
		return err
	}
	left, err := w.sh(fmt.Sprintf("grep -rl %s %s 2>/dev/null || true", marker, w.root))
	if err != nil {
		return err
	}
	if l := lines(left); len(l) > 0 {
		return fmt.Errorf("the pod is gone, yet what it wrote to its inline volume is still in --root (%v); an ephemeral volume lives and dies with its pod", l)
	}
	return nil
}

// waitFile waits for a pod's container to have written want to path.
func waitFile(w *work, path, want string) error {
	ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()
	var got string
	err := kube.WaitFor(ctx, "the pod's write to reach "+path, func(ctx context.Context) (bool, error) {
		got, _ = w.read(path)
		return strings.TrimSpace(got) == want, nil
	})
	if err != nil {
		return fmt.Errorf("%w: it holds %q, the pod wrote %q", err, got, want)
	}
	return nil
}

func stageSidecars(ctx context.Context, env *kube.Env, bin string) error {
	w, err := registered(ctx, env, "sidecars-drive-it", bin)
	if err != nil {
		return err
	}
	defer w.close()
	undo, err := w.csiDriver(storagev1.VolumeLifecyclePersistent)
	if err != nil {
		return err
	}
	defer undo()
	defer w.cleanPVs()
	undoRBAC, err := w.provisionerRBAC()
	if err != nil {
		return err
	}
	defer undoRBAC()

	root := int64(0)
	prov := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "csi-provisioner"},
		Spec: corev1.PodSpec{
			ServiceAccountName: "csi-provisioner",
			NodeName:           nodeID,
			SecurityContext:    &corev1.PodSecurityContext{RunAsUser: &root},
			Containers: []corev1.Container{{
				Name:  "csi-provisioner",
				Image: provisionerImage,
				Args: []string{"--csi-address=/csi/csi.sock", "--leader-election=false",
					"--feature-gates=Topology=true", "--timeout=30s", "--v=3"},
				VolumeMounts: []corev1.VolumeMount{{Name: "socket", MountPath: "/csi"}},
			}},
			Volumes: []corev1.Volume{{Name: "socket", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: "/var/lib/kubelet/plugins/" + w.name,
			}}}},
		},
	}
	_, cleanupProv, err := w.runPod(prov, 3*time.Minute)
	if cleanupProv != nil {
		defer cleanupProv()
	}
	if err != nil {
		return fmt.Errorf("the external-provisioner: %w", err)
	}

	wffc := storagev1.VolumeBindingWaitForFirstConsumer
	del := corev1.PersistentVolumeReclaimDelete
	sc := &storagev1.StorageClass{
		ObjectMeta:        metav1.ObjectMeta{Name: w.name},
		Provisioner:       w.name,
		VolumeBindingMode: &wffc,
		ReclaimPolicy:     &del,
	}
	scs := env.Client.StorageV1().StorageClasses()
	if _, err := scs.Create(ctx, sc, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create StorageClass: %w", err)
	}
	defer scs.Delete(context.Background(), sc.Name, metav1.DeleteOptions{})

	pvcs := env.Client.CoreV1().PersistentVolumeClaims(env.Namespace)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &sc.Name,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("16Mi")}},
		},
	}
	if _, err := pvcs.Create(ctx, pvc, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create PersistentVolumeClaim: %w", err)
	}
	defer pvcs.Delete(context.Background(), pvc.Name, metav1.DeleteOptions{})

	marker := "pvc-" + randHex(4)
	app, cleanupApp, err := w.runPod(pod("app", marker, corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}), 2*time.Minute)
	if cleanupApp != nil {
		defer cleanupApp()
	}
	if err != nil {
		return fmt.Errorf("a pod using a claim of the driver's StorageClass: %w\n%s\nprovisioner log:\n%s", err, w.events("data"), w.podLog("csi-provisioner"))
	}

	claim, err := pvcs.Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		return err
	}
	pv, err := env.Client.CoreV1().PersistentVolumes().Get(ctx, claim.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("the claim's PersistentVolume %q: %w", claim.Spec.VolumeName, err)
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != w.name {
		return fmt.Errorf("PersistentVolume %s is not a volume of driver %s", pv.Name, w.name)
	}
	if !pinned(pv) {
		return fmt.Errorf("PersistentVolume %s has node affinity %v; the provisioner builds it from CreateVolume's accessible_topology, which should pin it to %s", pv.Name, pv.Spec.NodeAffinity, nodeID)
	}
	handle := pv.Spec.CSI.VolumeHandle
	mnt := volumeMount(app, pv.Name)
	if err := w.wantMounted(mnt, "the pod's volume"); err != nil {
		return err
	}
	if err := waitFile(w, mnt+"/hello", marker); err != nil {
		return err
	}

	if err := w.deletePod("app"); err != nil {
		return fmt.Errorf("deleting the pod: %w", err)
	}
	if m, err := w.mountsUnder("/var/lib/kubelet/plugins/kubernetes.io/csi/" + w.name); err != nil {
		return err
	} else if len(m) > 0 {
		return fmt.Errorf("the pod is gone and the kubelet unstaged its volume, yet mounts remain: %v", m)
	}
	if err := pvcs.Delete(ctx, "data", metav1.DeleteOptions{}); err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	err = kube.WaitFor(wctx, "the provisioner to delete PersistentVolume "+pv.Name+" through DeleteVolume", func(ctx context.Context) (bool, error) {
		_, err := env.Client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	})
	if err != nil {
		return fmt.Errorf("%w\nprovisioner log:\n%s", err, w.podLog("csi-provisioner"))
	}
	ids, err := w.volumeIDs()
	if err != nil {
		return err
	}
	if slices.Contains(ids, handle) {
		return fmt.Errorf("the PersistentVolume is gone but the driver still lists volume %s", handle)
	}
	return nil
}

func pinned(pv *corev1.PersistentVolume) bool {
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return false
	}
	for _, t := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for _, e := range t.MatchExpressions {
			if e.Key == topologyKey && slices.Equal(e.Values, []string{nodeID}) {
				return true
			}
		}
	}
	return false
}

func (w *work) podLog(name string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tailLines := int64(30)
	b, err := w.env.Client.CoreV1().Pods(w.env.Namespace).GetLogs(name, &corev1.PodLogOptions{TailLines: &tailLines}).DoRaw(ctx)
	if err != nil {
		return err.Error()
	}
	return tail(string(b))
}

// cleanPVs removes any PersistentVolume of this driver a failed run left;
// PersistentVolumes are cluster-scoped, so no namespace reset reaches them.
func (w *work) cleanPVs() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pvs := w.env.Client.CoreV1().PersistentVolumes()
	list, err := pvs.List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}
	for _, pv := range list.Items {
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != w.name {
			continue
		}
		pvs.Patch(ctx, pv.Name, types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
		pvs.Delete(ctx, pv.Name, metav1.DeleteOptions{})
	}
}

// provisionerRBAC lets the provisioner's service account do its job, and
// returns the cleanup for the cluster-scoped half.
func (w *work) provisionerRBAC() (func(), error) {
	ctx := w.ctx
	c := w.env.Client
	if _, err := c.CoreV1().ServiceAccounts(w.env.Namespace).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "csi-provisioner"}}, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create ServiceAccount: %w", err)
	}
	name := "byok8s-" + w.name
	rw := []string{"get", "list", "watch", "create", "update", "patch", "delete"}
	ro := []string{"get", "list", "watch"}
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"persistentvolumes"}, Verbs: rw},
			{APIGroups: []string{""}, Resources: []string{"persistentvolumeclaims"}, Verbs: []string{"get", "list", "watch", "update", "patch"}},
			{APIGroups: []string{""}, Resources: []string{"events"}, Verbs: rw},
			{APIGroups: []string{"events.k8s.io"}, Resources: []string{"events"}, Verbs: rw},
			{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: ro},
			{APIGroups: []string{"storage.k8s.io"}, Resources: []string{"storageclasses", "csinodes", "volumeattachments"}, Verbs: ro},
			{APIGroups: []string{"snapshot.storage.k8s.io"}, Resources: []string{"volumesnapshots", "volumesnapshotcontents"}, Verbs: ro},
		},
	}
	if _, err := c.RbacV1().ClusterRoles().Create(ctx, role, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create ClusterRole: %w", err)
	}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: name},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "csi-provisioner", Namespace: w.env.Namespace}},
	}
	undo := func() {
		c.RbacV1().ClusterRoleBindings().Delete(context.Background(), name, metav1.DeleteOptions{})
		c.RbacV1().ClusterRoles().Delete(context.Background(), name, metav1.DeleteOptions{})
	}
	if _, err := c.RbacV1().ClusterRoleBindings().Create(ctx, binding, metav1.CreateOptions{}); err != nil {
		undo()
		return nil, fmt.Errorf("create ClusterRoleBinding: %w", err)
	}
	return undo, nil
}
