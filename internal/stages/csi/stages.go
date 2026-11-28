package csi

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/madhank93/byo-k8s-x/internal/kube"
)

func init() {
	register(Stage{Slug: "identity", Run: stageIdentity})
	register(Stage{Slug: "controller-capabilities", Run: stageControllerCapabilities})
	register(Stage{Slug: "create-volume", Run: stageCreateVolume})
	register(Stage{Slug: "delete-volume", Run: stageDeleteVolume})
	register(Stage{Slug: "node-info", Run: stageNodeInfo})
	register(Stage{Slug: "registration", Run: stageRegistration})
	register(Stage{Slug: "topology", Run: stageTopology})
	register(Stage{Slug: "node-stage", Run: stageNodeStage})
	register(Stage{Slug: "node-publish", Run: stageNodePublish})
	register(Stage{Slug: "node-unpublish", Run: stageNodeUnpublish})
	register(Stage{Slug: "filesystem", Run: stageFilesystem})
	register(Stage{Slug: "expansion", Run: stageExpansion})
	register(Stage{Slug: "controller-publish", Run: stageControllerPublish})
	register(Stage{Slug: "ephemeral-inline", Run: stageEphemeralInline})
	register(Stage{Slug: "snapshot-create", Run: stageSnapshotCreate})
	register(Stage{Slug: "snapshot-restore", Run: stageSnapshotRestore})
	register(Stage{Slug: "clone", Run: stageClone})
	register(Stage{Slug: "capacity", Run: stageCapacity})
	register(Stage{Slug: "idempotency", Run: stageIdempotency})
	register(Stage{Slug: "sidecars-drive-it", Run: stageSidecars})
}

// running is the common start of a stage: a driver, serving.
func running(ctx context.Context, env *kube.Env, slug, bin string, extra ...string) (*work, error) {
	w, err := newWork(ctx, env, slug, bin)
	if err != nil {
		return nil, err
	}
	w.args = extra
	if err := w.start(); err != nil {
		w.close()
		return nil, err
	}
	return w, nil
}

func stageIdentity(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, "identity", bin)
	if err != nil {
		return err
	}
	defer w.close()

	// What a crashed driver leaves behind: a file where the socket goes.
	if err := node.WriteFile(ctx, w.sock, []byte("stale"), 0o644); err != nil {
		return err
	}
	if err := w.start(); err != nil {
		return fmt.Errorf("with a stale file at %s from an earlier run: %w", w.sock, err)
	}

	c, cancel := w.call()
	defer cancel()
	info, err := w.id.GetPluginInfo(c, &csipb.GetPluginInfoRequest{})
	if err != nil {
		return fmt.Errorf("GetPluginInfo: %w", err)
	}
	if info.Name != w.name {
		return fmt.Errorf("GetPluginInfo named the driver %q; the name is the one --name gave, %q", info.Name, w.name)
	}
	if info.VendorVersion == "" {
		return fmt.Errorf("GetPluginInfo has no vendor_version; the spec requires one")
	}
	svc, _, err := w.pluginCaps()
	if err != nil {
		return err
	}
	if !slices.Contains(svc, csipb.PluginCapability_Service_CONTROLLER_SERVICE) {
		return fmt.Errorf("GetPluginCapabilities advertises %v; a driver that provisions volumes has CONTROLLER_SERVICE", svc)
	}

	res := w.stop(10 * time.Second)
	if res.ExitCode != 0 {
		return fmt.Errorf("on SIGTERM the driver should stop serving and exit 0, it exited %d:\n%s", res.ExitCode, tail(res.Stderr))
	}
	if w.try("test -e " + w.sock) {
		return fmt.Errorf("the driver exited and left %s behind; remove the socket on the way out", w.sock)
	}
	return nil
}

// probes calls each controller and node RPC a capability stands for, with an
// empty request: an advertised one must be implemented, and one that is not
// advertised must say Unimplemented.
func stageControllerCapabilities(ctx context.Context, env *kube.Env, bin string) error {
	w, err := running(ctx, env, "controller-capabilities", bin)
	if err != nil {
		return err
	}
	defer w.close()

	caps, err := w.ctlCaps()
	if err != nil {
		return err
	}
	if !slices.Contains(caps, csipb.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME) {
		return fmt.Errorf("ControllerGetCapabilities advertises %v; it should include CREATE_DELETE_VOLUME", caps)
	}
	type probe = func(context.Context) error
	ctl := map[csipb.ControllerServiceCapability_RPC_Type]probe{
		csipb.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME: func(c context.Context) error {
			_, err := w.ctl.CreateVolume(c, &csipb.CreateVolumeRequest{})
			return err
		},
		csipb.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME: func(c context.Context) error {
			_, err := w.ctl.ControllerPublishVolume(c, &csipb.ControllerPublishVolumeRequest{})
			return err
		},
		csipb.ControllerServiceCapability_RPC_LIST_VOLUMES: func(c context.Context) error {
			_, err := w.ctl.ListVolumes(c, &csipb.ListVolumesRequest{})
			return err
		},
		csipb.ControllerServiceCapability_RPC_GET_CAPACITY: func(c context.Context) error {
			_, err := w.ctl.GetCapacity(c, &csipb.GetCapacityRequest{})
			return err
		},
		csipb.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT: func(c context.Context) error {
			_, err := w.ctl.CreateSnapshot(c, &csipb.CreateSnapshotRequest{})
			return err
		},
		csipb.ControllerServiceCapability_RPC_LIST_SNAPSHOTS: func(c context.Context) error {
			_, err := w.ctl.ListSnapshots(c, &csipb.ListSnapshotsRequest{})
			return err
		},
		csipb.ControllerServiceCapability_RPC_EXPAND_VOLUME: func(c context.Context) error {
			_, err := w.ctl.ControllerExpandVolume(c, &csipb.ControllerExpandVolumeRequest{})
			return err
		},
	}
	for c, call := range ctl {
		if err := consistent(w, c.String(), slices.Contains(caps, c), call); err != nil {
			return err
		}
	}

	ncaps, err := w.nodeCaps()
	if err != nil {
		return err
	}
	nd := map[csipb.NodeServiceCapability_RPC_Type]probe{
		csipb.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME: func(c context.Context) error {
			_, err := w.nd.NodeStageVolume(c, &csipb.NodeStageVolumeRequest{})
			return err
		},
		csipb.NodeServiceCapability_RPC_EXPAND_VOLUME: func(c context.Context) error {
			_, err := w.nd.NodeExpandVolume(c, &csipb.NodeExpandVolumeRequest{})
			return err
		},
	}
	for c, call := range nd {
		if err := consistent(w, "node "+c.String(), slices.Contains(ncaps, c), call); err != nil {
			return err
		}
	}
	return nil
}

func consistent(w *work, what string, advertised bool, call func(context.Context) error) error {
	c, cancel := w.call()
	defer cancel()
	unimpl := status.Code(call(c)) == codes.Unimplemented
	switch {
	case advertised && unimpl:
		return fmt.Errorf("%s is advertised, but its RPC answers Unimplemented", what)
	case !advertised && !unimpl:
		return fmt.Errorf("%s is not advertised, so its RPC should answer Unimplemented; a CO only calls what a driver advertises", what)
	}
	return nil
}

func stageCreateVolume(ctx context.Context, env *kube.Env, bin string) error {
	w, err := running(ctx, env, "create-volume", bin)
	if err != nil {
		return err
	}
	defer w.close()

	want := 10*mi + 1
	v, err := w.create(createReq("pvc-a", want))
	if err != nil {
		return err
	}
	if v.CapacityBytes < want {
		return fmt.Errorf("asked for at least %d bytes, the volume has capacity_bytes %d", want, v.CapacityBytes)
	}
	again, err := w.create(createReq("pvc-a", want))
	if err != nil {
		return fmt.Errorf("the same CreateVolume twice must succeed both times (a CO retries): %w", err)
	}
	if again.VolumeId != v.VolumeId {
		return fmt.Errorf("the same name created twice gave volume ids %q and %q; the name is the idempotency key", v.VolumeId, again.VolumeId)
	}
	other, err := w.create(createReq("pvc-b", 5*mi))
	if err != nil {
		return err
	}
	if other.VolumeId == v.VolumeId {
		return fmt.Errorf("two names were given the same volume id %q", v.VolumeId)
	}

	c, cancel := w.call()
	defer cancel()
	_, err = w.ctl.CreateVolume(c, createReq("pvc-a", 50*mi))
	if err := wantCode(err, codes.AlreadyExists, "the name pvc-a again with a different capacity"); err != nil {
		return err
	}
	_, err = w.ctl.CreateVolume(c, createReq("", mi))
	if err := wantCode(err, codes.InvalidArgument, "CreateVolume with no name"); err != nil {
		return err
	}
	noCaps := createReq("pvc-c", mi)
	noCaps.VolumeCapabilities = nil
	_, err = w.ctl.CreateVolume(c, noCaps)
	if err := wantCode(err, codes.InvalidArgument, "CreateVolume with no volume_capabilities"); err != nil {
		return err
	}
	block := createReq("pvc-d", mi)
	block.VolumeCapabilities[0].AccessType = &csipb.VolumeCapability_Block{Block: &csipb.VolumeCapability_BlockVolume{}}
	_, err = w.ctl.CreateVolume(c, block)
	if err := wantCode(err, codes.InvalidArgument, "CreateVolume for a raw block volume, which this driver does not offer"); err != nil {
		return err
	}
	bad := createReq("pvc-e", 20*mi)
	bad.CapacityRange.LimitBytes = 10 * mi
	_, err = w.ctl.CreateVolume(c, bad)
	return wantCodeIn(err, []codes.Code{codes.OutOfRange, codes.InvalidArgument}, "CreateVolume with required_bytes above limit_bytes")
}

func stageDeleteVolume(ctx context.Context, env *kube.Env, bin string) error {
	w, err := running(ctx, env, "delete-volume", bin)
	if err != nil {
		return err
	}
	defer w.close()

	if err := w.wantCtlCap(csipb.ControllerServiceCapability_RPC_LIST_VOLUMES); err != nil {
		return err
	}
	keep, err := w.create(createReq("pvc-keep", mi))
	if err != nil {
		return err
	}
	gone, err := w.create(createReq("pvc-gone", mi))
	if err != nil {
		return err
	}
	ids, err := w.volumeIDs()
	if err != nil {
		return err
	}
	if !slices.Contains(ids, keep.VolumeId) || !slices.Contains(ids, gone.VolumeId) {
		return fmt.Errorf("ListVolumes reports %q; both volumes just created should be in it", ids)
	}

	if err := w.delete(gone.VolumeId); err != nil {
		return err
	}
	if err := w.delete(gone.VolumeId); err != nil {
		return fmt.Errorf("deleting a volume that is already gone must succeed (a CO retries): %w", err)
	}
	if err := w.delete("no-such-volume"); err != nil {
		return fmt.Errorf("deleting an id the driver never made must succeed: %w", err)
	}
	c, cancel := w.call()
	defer cancel()
	_, err = w.ctl.DeleteVolume(c, &csipb.DeleteVolumeRequest{})
	if err := wantCode(err, codes.InvalidArgument, "DeleteVolume with no volume_id"); err != nil {
		return err
	}
	if ids, err = w.volumeIDs(); err != nil {
		return err
	}
	if slices.Contains(ids, gone.VolumeId) {
		return fmt.Errorf("ListVolumes still reports %s after DeleteVolume", gone.VolumeId)
	}

	if err := w.restart(); err != nil {
		return err
	}
	if ids, err = w.volumeIDs(); err != nil {
		return err
	}
	if !slices.Equal(ids, []string{keep.VolumeId}) {
		return fmt.Errorf("after a restart ListVolumes reports %q, want just %s; the driver keeps its volumes in --root, not in memory", ids, keep.VolumeId)
	}
	again, err := w.create(createReq("pvc-keep", mi))
	if err != nil {
		return fmt.Errorf("after a restart: %w", err)
	}
	if again.VolumeId != keep.VolumeId {
		return fmt.Errorf("after a restart CreateVolume pvc-keep made a new volume %s; it should return the existing %s", again.VolumeId, keep.VolumeId)
	}
	return nil
}

func stageNodeInfo(ctx context.Context, env *kube.Env, bin string) error {
	w, err := running(ctx, env, "node-info", bin)
	if err != nil {
		return err
	}
	defer w.close()

	c, cancel := w.call()
	defer cancel()
	r, err := w.nd.NodeGetInfo(c, &csipb.NodeGetInfoRequest{})
	if err != nil {
		return fmt.Errorf("NodeGetInfo: %w", err)
	}
	if r.NodeId != nodeID {
		return fmt.Errorf("NodeGetInfo says node_id %q; --node-id is %q", r.NodeId, nodeID)
	}
	seg := r.GetAccessibleTopology().GetSegments()
	if len(seg) != 1 || seg[topologyKey] != nodeID {
		return fmt.Errorf("NodeGetInfo's accessible_topology is %v, want {%s: %s}", seg, topologyKey, nodeID)
	}
	if _, err := w.nodeCaps(); err != nil {
		return err
	}
	return nil
}

// stageRegistration hands the driver to the real kubelet, which finds it
// through the registration socket and records it in the node's CSINode.
func stageRegistration(ctx context.Context, env *kube.Env, bin string) error {
	w, err := registered(ctx, env, "registration", bin)
	if err != nil {
		return err
	}
	defer w.close()
	if res := w.stop(10 * time.Second); res.ExitCode != 0 {
		return fmt.Errorf("on SIGTERM the driver should exit 0, it exited %d:\n%s", res.ExitCode, tail(res.Stderr))
	}
	if w.try("test -e " + w.reg) {
		return fmt.Errorf("the driver exited and left %s behind; the kubelet deregisters a driver when its registration socket goes", w.reg)
	}
	return w.wantDeregistered()
}

// registered is running with the registration socket the kubelet watches,
// once the kubelet has taken the driver on.
func registered(ctx context.Context, env *kube.Env, slug, bin string) (*work, error) {
	w, err := newWork(ctx, env, slug, bin)
	if err != nil {
		return nil, err
	}
	w.args = []string{"--registration-socket", w.reg}
	if err := w.start(); err != nil {
		w.close()
		return nil, err
	}
	if err := w.wantRegistered(); err != nil {
		w.close()
		return nil, err
	}
	return w, nil
}

func (w *work) wantRegistered() error {
	ctx, cancel := context.WithTimeout(w.ctx, 60*time.Second)
	defer cancel()
	var last string
	err := kube.WaitFor(ctx, "the kubelet to list "+w.name+" in CSINode "+nodeID, func(ctx context.Context) (bool, error) {
		if done, res := w.proc.Exited(); done {
			return false, fmt.Errorf("the driver exited with %d:\n%s", res.ExitCode, tail(res.Stderr))
		}
		cn, err := w.env.Client.StorageV1().CSINodes().Get(ctx, nodeID, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		for _, d := range cn.Spec.Drivers {
			if d.Name != w.name {
				continue
			}
			last = fmt.Sprintf("nodeID %q, topologyKeys %v", d.NodeID, d.TopologyKeys)
			return d.NodeID == nodeID && slices.Contains(d.TopologyKeys, topologyKey), nil
		}
		return false, nil
	})
	if err != nil {
		if last != "" {
			return fmt.Errorf("%w; it is there with %s", err, last)
		}
		return fmt.Errorf("%w. The kubelet watches /var/lib/kubelet/plugins_registry for a socket serving the registration API, calls GetInfo, then NodeGetInfo on the endpoint it returned", err)
	}
	return nil
}

func (w *work) wantDeregistered() error {
	ctx, cancel := context.WithTimeout(w.ctx, 60*time.Second)
	defer cancel()
	return kube.WaitFor(ctx, "the kubelet to drop "+w.name+" from CSINode "+nodeID+" once the driver stopped", func(ctx context.Context) (bool, error) {
		cn, err := w.env.Client.StorageV1().CSINodes().Get(ctx, nodeID, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		for _, d := range cn.Spec.Drivers {
			if d.Name == w.name {
				return false, nil
			}
		}
		return true, nil
	})
}

func stageTopology(ctx context.Context, env *kube.Env, bin string) error {
	w, err := running(ctx, env, "topology", bin)
	if err != nil {
		return err
	}
	defer w.close()

	svc, _, err := w.pluginCaps()
	if err != nil {
		return err
	}
	if !slices.Contains(svc, csipb.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS) {
		return fmt.Errorf("GetPluginCapabilities advertises %v; a driver whose volumes live on one node has VOLUME_ACCESSIBILITY_CONSTRAINTS", svc)
	}
	here := &csipb.Topology{Segments: map[string]string{topologyKey: nodeID}}
	there := &csipb.Topology{Segments: map[string]string{topologyKey: "byok8s-elsewhere"}}
	onlyHere := func(v *csipb.Volume, what string) error {
		at := v.GetAccessibleTopology()
		if len(at) != 1 || len(at[0].Segments) != 1 || at[0].Segments[topologyKey] != nodeID {
			return fmt.Errorf("%s: the volume's accessible_topology is %v; it lives on %s only, so want [{%s: %s}]", what, at, nodeID, topologyKey, nodeID)
		}
		return nil
	}

	v, err := w.create(createReq("pvc-any", mi))
	if err != nil {
		return err
	}
	if err := onlyHere(v, "with no accessibility_requirements"); err != nil {
		return err
	}
	req := createReq("pvc-pick", mi)
	req.AccessibilityRequirements = &csipb.TopologyRequirement{Requisite: []*csipb.Topology{there, here}, Preferred: []*csipb.Topology{there, here}}
	if v, err = w.create(req); err != nil {
		return err
	}
	if err := onlyHere(v, "with this node among the requisite topologies"); err != nil {
		return err
	}
	req = createReq("pvc-nowhere", mi)
	req.AccessibilityRequirements = &csipb.TopologyRequirement{Requisite: []*csipb.Topology{there}, Preferred: []*csipb.Topology{there}}
	c, cancel := w.call()
	defer cancel()
	_, err = w.ctl.CreateVolume(c, req)
	return wantCode(err, codes.ResourceExhausted, "CreateVolume whose only requisite topology is another node")
}

func stageNodeStage(ctx context.Context, env *kube.Env, bin string) error {
	w, err := running(ctx, env, "node-stage", bin)
	if err != nil {
		return err
	}
	defer w.close()

	if err := w.wantNodeCap(csipb.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME); err != nil {
		return err
	}
	v, err := w.create(createReq("pvc-a", mi))
	if err != nil {
		return err
	}
	staging, err := w.stage(v.VolumeId, mountCap(""))
	if err != nil {
		return err
	}
	if err := w.wantMounted(staging, "after NodeStageVolume"); err != nil {
		return err
	}
	if _, err := w.stage(v.VolumeId, mountCap("")); err != nil {
		return fmt.Errorf("staging a volume that is already staged must succeed: %w", err)
	}
	if err := w.wantMounted(staging, "after NodeStageVolume twice"); err != nil {
		return err
	}
	if err := w.write(staging+"/hello", "staged"); err != nil {
		return err
	}

	// The same volume staged a second time elsewhere shares its data.
	w2 := w.co + "/stage/second"
	if _, err := w.sh("mkdir -p " + w2); err != nil {
		return err
	}
	c, cancel := w.call()
	defer cancel()
	if _, err := w.nd.NodeStageVolume(c, &csipb.NodeStageVolumeRequest{VolumeId: v.VolumeId, StagingTargetPath: w2, VolumeCapability: mountCap("")}); err != nil {
		return fmt.Errorf("NodeStageVolume %s at a second path: %w", v.VolumeId, err)
	}
	if got, err := w.read(w2 + "/hello"); err != nil || got != "staged" {
		return fmt.Errorf("a file written through one staging path is not at the other (%q, %v); staging mounts the volume's own storage", got, err)
	}

	_, err = w.nd.NodeStageVolume(c, &csipb.NodeStageVolumeRequest{VolumeId: "no-such-volume", StagingTargetPath: staging, VolumeCapability: mountCap("")})
	if err := wantCode(err, codes.NotFound, "NodeStageVolume for a volume the driver never made"); err != nil {
		return err
	}
	_, err = w.nd.NodeStageVolume(c, &csipb.NodeStageVolumeRequest{VolumeId: v.VolumeId, VolumeCapability: mountCap("")})
	if err := wantCode(err, codes.InvalidArgument, "NodeStageVolume with no staging_target_path"); err != nil {
		return err
	}
	_, err = w.nd.NodeStageVolume(c, &csipb.NodeStageVolumeRequest{VolumeId: v.VolumeId, StagingTargetPath: staging})
	return wantCode(err, codes.InvalidArgument, "NodeStageVolume with no volume_capability")
}

func stageNodePublish(ctx context.Context, env *kube.Env, bin string) error {
	w, err := running(ctx, env, "node-publish", bin)
	if err != nil {
		return err
	}
	defer w.close()

	v, staging, target, err := w.up("pvc-a", mi)
	if err != nil {
		return err
	}
	if err := w.wantMounted(target, "after NodePublishVolume"); err != nil {
		return err
	}
	if err := w.write(staging+"/from-stage", "s"); err != nil {
		return err
	}
	if got, err := w.read(target + "/from-stage"); err != nil || got != "s" {
		return fmt.Errorf("a file written in the staging path is not visible at the target (%q, %v); publish mounts the staged volume", got, err)
	}
	if err := w.write(target+"/from-pod", "p"); err != nil {
		return fmt.Errorf("the published volume is not writable: %w", err)
	}
	if _, err := w.publish(v.VolumeId, staging, "pvc-a", mountCap(""), false); err != nil {
		return fmt.Errorf("publishing to a target that is already published must succeed: %w", err)
	}
	if err := w.wantMounted(target, "after NodePublishVolume twice"); err != nil {
		return err
	}

	ro, err := w.publish(v.VolumeId, staging, "reader", mountCap(""), true)
	if err != nil {
		return err
	}
	if got, err := w.read(ro + "/from-pod"); err != nil || got != "p" {
		return fmt.Errorf("the read-only publish does not show the volume's files (%q, %v)", got, err)
	}
	if w.try(fmt.Sprintf("touch %s/nope", ro)) {
		return fmt.Errorf("a file could be created through a readonly publish at %s; mount it read-only", ro)
	}
	if err := w.write(target+"/still", "w"); err != nil {
		return fmt.Errorf("after a read-only publish elsewhere the first target is no longer writable: %w", err)
	}

	c, cancel := w.call()
	defer cancel()
	_, err = w.nd.NodePublishVolume(c, &csipb.NodePublishVolumeRequest{VolumeId: "no-such-volume", StagingTargetPath: staging, TargetPath: w.co + "/pods/x/mount", VolumeCapability: mountCap("")})
	if err := wantCode(err, codes.NotFound, "NodePublishVolume for a volume the driver never made"); err != nil {
		return err
	}
	_, err = w.nd.NodePublishVolume(c, &csipb.NodePublishVolumeRequest{VolumeId: v.VolumeId, StagingTargetPath: staging, VolumeCapability: mountCap("")})
	return wantCode(err, codes.InvalidArgument, "NodePublishVolume with no target_path")
}

func stageNodeUnpublish(ctx context.Context, env *kube.Env, bin string) error {
	w, err := running(ctx, env, "node-unpublish", bin)
	if err != nil {
		return err
	}
	defer w.close()

	v, staging, target, err := w.up("pvc-a", mi)
	if err != nil {
		return err
	}
	if err := w.write(target+"/keep", "kept"); err != nil {
		return err
	}
	if err := w.unpublish(v.VolumeId, target); err != nil {
		return err
	}
	if err := w.wantUnmounted(target, "after NodeUnpublishVolume"); err != nil {
		return err
	}
	if w.try("test -e " + target) {
		return fmt.Errorf("NodeUnpublishVolume left %s behind; the driver created the target, so it removes it", target)
	}
	if err := w.unpublish(v.VolumeId, target); err != nil {
		return fmt.Errorf("unpublishing a target that is already gone must succeed: %w", err)
	}
	if err := w.unstage(v.VolumeId, staging); err != nil {
		return err
	}
	if err := w.wantUnmounted(staging, "after NodeUnstageVolume"); err != nil {
		return err
	}
	if err := w.unstage(v.VolumeId, staging); err != nil {
		return fmt.Errorf("unstaging a volume that is not staged must succeed: %w", err)
	}
	c, cancel := w.call()
	defer cancel()
	_, err = w.nd.NodeUnpublishVolume(c, &csipb.NodeUnpublishVolumeRequest{VolumeId: v.VolumeId})
	if err := wantCode(err, codes.InvalidArgument, "NodeUnpublishVolume with no target_path"); err != nil {
		return err
	}
	_, err = w.nd.NodeUnstageVolume(c, &csipb.NodeUnstageVolumeRequest{StagingTargetPath: staging})
	if err := wantCode(err, codes.InvalidArgument, "NodeUnstageVolume with no volume_id"); err != nil {
		return err
	}

	if _, err := w.stage(v.VolumeId, mountCap("")); err != nil {
		return err
	}
	if target, err = w.publish(v.VolumeId, staging, "again", mountCap(""), false); err != nil {
		return err
	}
	if got, err := w.read(target + "/keep"); err != nil || got != "kept" {
		return fmt.Errorf("after unpublish, unstage and back, the volume lost its data (%q, %v); unmounting must not touch what is stored", got, err)
	}
	if err := w.down(v.VolumeId, staging, target); err != nil {
		return err
	}
	if m, err := w.mountsUnder(w.co); err != nil {
		return err
	} else if len(m) > 0 {
		return fmt.Errorf("everything is unpublished and unstaged, yet mounts remain: %v", m)
	}
	if err := w.delete(v.VolumeId); err != nil {
		return err
	}
	fresh, staging, target, err := w.up("pvc-a", mi)
	if err != nil {
		return err
	}
	if w.try("test -e " + target + "/keep") {
		return fmt.Errorf("a volume created after DeleteVolume of the same name (%s) still holds the old one's files", fresh.VolumeId)
	}
	return w.down(fresh.VolumeId, staging, target)
}

func stageFilesystem(ctx context.Context, env *kube.Env, bin string) error {
	w, err := running(ctx, env, "filesystem", bin)
	if err != nil {
		return err
	}
	defer w.close()

	req := createReq("pvc-ext4", 64*mi)
	req.VolumeCapabilities = []*csipb.VolumeCapability{mountCap("ext4")}
	v, err := w.create(req)
	if err != nil {
		return err
	}
	staging, err := w.stage(v.VolumeId, mountCap("ext4"))
	if err != nil {
		return err
	}
	if err := w.wantMounted(staging, "after NodeStageVolume"); err != nil {
		return err
	}
	fs, err := w.sh("findmnt -rn -o FSTYPE -M " + staging)
	if err != nil {
		return err
	}
	if fs = strings.TrimSpace(fs); fs != "ext4" {
		return fmt.Errorf("the volume is staged as %s; it asked for an ext4 filesystem", fs)
	}
	if err := wantSize(w, staging, 64*mi); err != nil {
		return err
	}
	target, err := w.publish(v.VolumeId, staging, "pvc-ext4", mountCap("ext4"), false)
	if err != nil {
		return err
	}
	if err := w.write(target+"/keep", "kept"); err != nil {
		return err
	}
	if err := w.down(v.VolumeId, staging, target); err != nil {
		return err
	}
	if l, err := w.loopsUnder(w.root); err != nil {
		return err
	} else if len(l) > 0 {
		return fmt.Errorf("the volume is unstaged, but its loop device is still attached: %v", l)
	}
	if staging, err = w.stage(v.VolumeId, mountCap("ext4")); err != nil {
		return err
	}
	if got, err := w.read(staging + "/keep"); err != nil || got != "kept" {
		return fmt.Errorf("staged a second time, the filesystem lost its files (%q, %v); format only a device that has no filesystem yet", got, err)
	}
	if err := w.unstage(v.VolumeId, staging); err != nil {
		return err
	}

	bad := createReq("pvc-zfs", 64*mi)
	bad.VolumeCapabilities = []*csipb.VolumeCapability{mountCap("zfs")}
	c, cancel := w.call()
	defer cancel()
	_, err = w.ctl.CreateVolume(c, bad)
	if err := wantCode(err, codes.InvalidArgument, "CreateVolume with fs_type zfs, which the driver cannot make"); err != nil {
		return err
	}
	if err := w.delete(v.VolumeId); err != nil {
		return err
	}
	return nil
}

// wantSize checks the filesystem at path is about want bytes: an ext4
// filesystem keeps a few percent for its own metadata.
func wantSize(w *work, path string, want int64) error {
	out, err := w.sh("df -B1 --output=size " + path + " | tail -1")
	if err != nil {
		return err
	}
	got, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	if err != nil {
		return err
	}
	if got < want*8/10 || got > want {
		return fmt.Errorf("the filesystem at %s is %d bytes; for a %d-byte volume it should be a little under that", path, got, want)
	}
	return nil
}

func stageExpansion(ctx context.Context, env *kube.Env, bin string) error {
	w, err := running(ctx, env, "expansion", bin)
	if err != nil {
		return err
	}
	defer w.close()

	if err := w.wantCtlCap(csipb.ControllerServiceCapability_RPC_EXPAND_VOLUME); err != nil {
		return err
	}
	if err := w.wantNodeCap(csipb.NodeServiceCapability_RPC_EXPAND_VOLUME); err != nil {
		return err
	}
	_, exp, err := w.pluginCaps()
	if err != nil {
		return err
	}
	if !slices.Contains(exp, csipb.PluginCapability_VolumeExpansion_ONLINE) {
		return fmt.Errorf("GetPluginCapabilities advertises volume expansion %v; a filesystem this driver grows while mounted is ONLINE", exp)
	}

	req := createReq("pvc-grow", 64*mi)
	req.VolumeCapabilities = []*csipb.VolumeCapability{mountCap("ext4")}
	v, err := w.create(req)
	if err != nil {
		return err
	}
	staging, err := w.stage(v.VolumeId, mountCap("ext4"))
	if err != nil {
		return err
	}
	target, err := w.publish(v.VolumeId, staging, "pvc-grow", mountCap("ext4"), false)
	if err != nil {
		return err
	}
	if err := w.write(target+"/keep", "kept"); err != nil {
		return err
	}

	c, cancel := w.call()
	defer cancel()
	r, err := w.ctl.ControllerExpandVolume(c, &csipb.ControllerExpandVolumeRequest{VolumeId: v.VolumeId, CapacityRange: &csipb.CapacityRange{RequiredBytes: 128 * mi}, VolumeCapability: mountCap("ext4")})
	if err != nil {
		return fmt.Errorf("ControllerExpandVolume to 128Mi: %w", err)
	}
	if r.CapacityBytes < 128*mi {
		return fmt.Errorf("ControllerExpandVolume to 128Mi answered capacity_bytes %d", r.CapacityBytes)
	}
	if !r.NodeExpansionRequired {
		return fmt.Errorf("ControllerExpandVolume says node_expansion_required false, yet the filesystem on the volume has not grown; the node has to resize it")
	}
	nr, err := w.nd.NodeExpandVolume(c, &csipb.NodeExpandVolumeRequest{VolumeId: v.VolumeId, VolumePath: target, StagingTargetPath: staging, CapacityRange: &csipb.CapacityRange{RequiredBytes: 128 * mi}, VolumeCapability: mountCap("ext4")})
	if err != nil {
		return fmt.Errorf("NodeExpandVolume: %w", err)
	}
	if nr.CapacityBytes != 0 && nr.CapacityBytes < 128*mi {
		return fmt.Errorf("NodeExpandVolume answered capacity_bytes %d, below the 128Mi asked for", nr.CapacityBytes)
	}
	if err := wantSize(w, target, 128*mi); err != nil {
		return fmt.Errorf("after expanding to 128Mi: %w", err)
	}
	if got, err := w.read(target + "/keep"); err != nil || got != "kept" {
		return fmt.Errorf("expanding lost the volume's files (%q, %v)", got, err)
	}
	if _, err := w.ctl.ControllerExpandVolume(c, &csipb.ControllerExpandVolumeRequest{VolumeId: v.VolumeId, CapacityRange: &csipb.CapacityRange{RequiredBytes: 128 * mi}, VolumeCapability: mountCap("ext4")}); err != nil {
		return fmt.Errorf("expanding again to the size it already has must succeed: %w", err)
	}
	_, err = w.ctl.ControllerExpandVolume(c, &csipb.ControllerExpandVolumeRequest{VolumeId: v.VolumeId, CapacityRange: &csipb.CapacityRange{RequiredBytes: 32 * mi}, VolumeCapability: mountCap("ext4")})
	if err := wantCodeIn(err, []codes.Code{codes.OutOfRange, codes.InvalidArgument}, "ControllerExpandVolume to a size below the current one"); err != nil {
		return err
	}
	_, err = w.ctl.ControllerExpandVolume(c, &csipb.ControllerExpandVolumeRequest{VolumeId: "no-such-volume", CapacityRange: &csipb.CapacityRange{RequiredBytes: 128 * mi}})
	if err := wantCode(err, codes.NotFound, "ControllerExpandVolume of a volume the driver never made"); err != nil {
		return err
	}
	return w.down(v.VolumeId, staging, target)
}

func stageControllerPublish(ctx context.Context, env *kube.Env, bin string) error {
	w, err := running(ctx, env, "controller-publish", bin)
	if err != nil {
		return err
	}
	defer w.close()

	if err := w.wantCtlCap(csipb.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME); err != nil {
		return err
	}
	v, err := w.create(createReq("pvc-a", mi))
	if err != nil {
		return err
	}
	c, cancel := w.call()
	defer cancel()
	pub := &csipb.ControllerPublishVolumeRequest{VolumeId: v.VolumeId, NodeId: nodeID, VolumeCapability: mountCap("")}
	if _, err := w.ctl.ControllerPublishVolume(c, pub); err != nil {
		return fmt.Errorf("ControllerPublishVolume %s to %s: %w", v.VolumeId, nodeID, err)
	}
	if _, err := w.ctl.ControllerPublishVolume(c, pub); err != nil {
		return fmt.Errorf("publishing a volume already published to the same node must succeed: %w", err)
	}
	_, err = w.ctl.ControllerPublishVolume(c, &csipb.ControllerPublishVolumeRequest{VolumeId: v.VolumeId, NodeId: "byok8s-elsewhere", VolumeCapability: mountCap("")})
	if err := wantCode(err, codes.NotFound, "ControllerPublishVolume to a node the driver does not know"); err != nil {
		return err
	}
	_, err = w.ctl.ControllerPublishVolume(c, &csipb.ControllerPublishVolumeRequest{VolumeId: "no-such-volume", NodeId: nodeID, VolumeCapability: mountCap("")})
	if err := wantCode(err, codes.NotFound, "ControllerPublishVolume of a volume the driver never made"); err != nil {
		return err
	}
	_, err = w.ctl.ControllerPublishVolume(c, &csipb.ControllerPublishVolumeRequest{VolumeId: v.VolumeId, NodeId: nodeID})
	if err := wantCode(err, codes.InvalidArgument, "ControllerPublishVolume with no volume_capability"); err != nil {
		return err
	}
	_, err = w.ctl.DeleteVolume(c, &csipb.DeleteVolumeRequest{VolumeId: v.VolumeId})
	if err := wantCode(err, codes.FailedPrecondition, "DeleteVolume of a volume still published to a node"); err != nil {
		return err
	}
	if err := w.restart(); err != nil {
		return err
	}
	c, cancel = w.call()
	defer cancel()
	_, err = w.ctl.DeleteVolume(c, &csipb.DeleteVolumeRequest{VolumeId: v.VolumeId})
	if err := wantCode(err, codes.FailedPrecondition, "after a restart, DeleteVolume of a volume still published"); err != nil {
		return err
	}
	unpub := &csipb.ControllerUnpublishVolumeRequest{VolumeId: v.VolumeId, NodeId: nodeID}
	if _, err := w.ctl.ControllerUnpublishVolume(c, unpub); err != nil {
		return fmt.Errorf("ControllerUnpublishVolume: %w", err)
	}
	if _, err := w.ctl.ControllerUnpublishVolume(c, unpub); err != nil {
		return fmt.Errorf("unpublishing a volume that is not published must succeed: %w", err)
	}
	return w.delete(v.VolumeId)
}

func stageSnapshotCreate(ctx context.Context, env *kube.Env, bin string) error {
	w, err := running(ctx, env, "snapshot-create", bin)
	if err != nil {
		return err
	}
	defer w.close()

	for _, c := range []csipb.ControllerServiceCapability_RPC_Type{csipb.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT, csipb.ControllerServiceCapability_RPC_LIST_SNAPSHOTS} {
		if err := w.wantCtlCap(c); err != nil {
			return err
		}
	}
	v, staging, target, err := w.up("pvc-src", mi)
	if err != nil {
		return err
	}
	if err := w.write(target+"/f", "v1"); err != nil {
		return err
	}
	other, err := w.create(createReq("pvc-other", mi))
	if err != nil {
		return err
	}

	c, cancel := w.call()
	defer cancel()
	r, err := w.ctl.CreateSnapshot(c, &csipb.CreateSnapshotRequest{Name: "snap-1", SourceVolumeId: v.VolumeId})
	if err != nil {
		return fmt.Errorf("CreateSnapshot snap-1 of %s: %w", v.VolumeId, err)
	}
	s := r.GetSnapshot()
	switch {
	case s.GetSnapshotId() == "":
		return fmt.Errorf("CreateSnapshot answered without a snapshot_id")
	case s.SourceVolumeId != v.VolumeId:
		return fmt.Errorf("the snapshot's source_volume_id is %q, want %q", s.SourceVolumeId, v.VolumeId)
	case !s.ReadyToUse:
		return fmt.Errorf("the snapshot is not ready_to_use; a copy taken in the call is ready when it returns")
	case s.CreationTime == nil:
		return fmt.Errorf("the snapshot has no creation_time")
	}
	again, err := w.ctl.CreateSnapshot(c, &csipb.CreateSnapshotRequest{Name: "snap-1", SourceVolumeId: v.VolumeId})
	if err != nil {
		return fmt.Errorf("the same CreateSnapshot twice must succeed: %w", err)
	}
	if again.GetSnapshot().GetSnapshotId() != s.SnapshotId {
		return fmt.Errorf("snap-1 taken twice gave snapshot ids %q and %q", s.SnapshotId, again.GetSnapshot().GetSnapshotId())
	}
	_, err = w.ctl.CreateSnapshot(c, &csipb.CreateSnapshotRequest{Name: "snap-1", SourceVolumeId: other.VolumeId})
	if err := wantCode(err, codes.AlreadyExists, "snap-1 again from a different source volume"); err != nil {
		return err
	}
	_, err = w.ctl.CreateSnapshot(c, &csipb.CreateSnapshotRequest{Name: "snap-x", SourceVolumeId: "no-such-volume"})
	if err := wantCode(err, codes.NotFound, "CreateSnapshot of a volume the driver never made"); err != nil {
		return err
	}
	_, err = w.ctl.CreateSnapshot(c, &csipb.CreateSnapshotRequest{SourceVolumeId: v.VolumeId})
	if err := wantCode(err, codes.InvalidArgument, "CreateSnapshot with no name"); err != nil {
		return err
	}
	s2, err := w.ctl.CreateSnapshot(c, &csipb.CreateSnapshotRequest{Name: "snap-2", SourceVolumeId: other.VolumeId})
	if err != nil {
		return err
	}

	list := func(req *csipb.ListSnapshotsRequest) ([]string, error) {
		r, err := w.ctl.ListSnapshots(c, req)
		if err != nil {
			return nil, fmt.Errorf("ListSnapshots: %w", err)
		}
		var ids []string
		for _, e := range r.Entries {
			ids = append(ids, e.GetSnapshot().GetSnapshotId())
		}
		slices.Sort(ids)
		return ids, nil
	}
	all, err := list(&csipb.ListSnapshotsRequest{})
	if err != nil {
		return err
	}
	if !slices.Contains(all, s.SnapshotId) || !slices.Contains(all, s2.Snapshot.SnapshotId) {
		return fmt.Errorf("ListSnapshots reports %q; both snapshots should be in it", all)
	}
	one, err := list(&csipb.ListSnapshotsRequest{SnapshotId: s.SnapshotId})
	if err != nil {
		return err
	}
	if !slices.Equal(one, []string{s.SnapshotId}) {
		return fmt.Errorf("ListSnapshots with snapshot_id %s reports %q, want just that one", s.SnapshotId, one)
	}
	bySource, err := list(&csipb.ListSnapshotsRequest{SourceVolumeId: other.VolumeId})
	if err != nil {
		return err
	}
	if !slices.Equal(bySource, []string{s2.Snapshot.SnapshotId}) {
		return fmt.Errorf("ListSnapshots with source_volume_id %s reports %q, want just %s", other.VolumeId, bySource, s2.Snapshot.SnapshotId)
	}
	none, err := list(&csipb.ListSnapshotsRequest{SnapshotId: "no-such-snapshot"})
	if err != nil {
		return fmt.Errorf("listing a snapshot id that does not exist should answer an empty list: %w", err)
	}
	if len(none) != 0 {
		return fmt.Errorf("ListSnapshots for an id that does not exist reports %q", none)
	}

	if _, err := w.ctl.DeleteSnapshot(c, &csipb.DeleteSnapshotRequest{SnapshotId: s.SnapshotId}); err != nil {
		return fmt.Errorf("DeleteSnapshot: %w", err)
	}
	if _, err := w.ctl.DeleteSnapshot(c, &csipb.DeleteSnapshotRequest{SnapshotId: s.SnapshotId}); err != nil {
		return fmt.Errorf("deleting a snapshot that is already gone must succeed: %w", err)
	}
	if all, err = list(&csipb.ListSnapshotsRequest{}); err != nil {
		return err
	}
	if slices.Contains(all, s.SnapshotId) {
		return fmt.Errorf("ListSnapshots still reports %s after DeleteSnapshot", s.SnapshotId)
	}
	return w.down(v.VolumeId, staging, target)
}

func stageSnapshotRestore(ctx context.Context, env *kube.Env, bin string) error {
	w, err := running(ctx, env, "snapshot-restore", bin)
	if err != nil {
		return err
	}
	defer w.close()

	v, staging, target, err := w.up("pvc-src", mi)
	if err != nil {
		return err
	}
	if err := w.write(target+"/f", "v1"); err != nil {
		return err
	}
	c, cancel := w.call()
	defer cancel()
	sr, err := w.ctl.CreateSnapshot(c, &csipb.CreateSnapshotRequest{Name: "snap-1", SourceVolumeId: v.VolumeId})
	if err != nil {
		return fmt.Errorf("CreateSnapshot: %w", err)
	}
	snap := sr.Snapshot.SnapshotId
	if err := w.write(target+"/f", "v2"); err != nil {
		return err
	}
	if err := w.write(target+"/later", "x"); err != nil {
		return err
	}
	if err := w.down(v.VolumeId, staging, target); err != nil {
		return err
	}
	if err := w.delete(v.VolumeId); err != nil {
		return fmt.Errorf("deleting a volume that has a snapshot: %w", err)
	}

	from := &csipb.VolumeContentSource{Type: &csipb.VolumeContentSource_Snapshot{Snapshot: &csipb.VolumeContentSource_SnapshotSource{SnapshotId: snap}}}
	req := createReq("pvc-restored", mi)
	req.VolumeContentSource = from
	nv, err := w.create(req)
	if err != nil {
		return fmt.Errorf("restoring a snapshot whose source volume was deleted: %w", err)
	}
	if nv.GetContentSource().GetSnapshot().GetSnapshotId() != snap {
		return fmt.Errorf("the restored volume's content_source is %v; it should name snapshot %s", nv.GetContentSource(), snap)
	}
	if staging, err = w.stage(nv.VolumeId, mountCap("")); err != nil {
		return err
	}
	if target, err = w.publish(nv.VolumeId, staging, "restored", mountCap(""), false); err != nil {
		return err
	}
	if got, err := w.read(target + "/f"); err != nil || got != "v1" {
		return fmt.Errorf("the volume restored from the snapshot reads %q (%v); the snapshot was taken when the file said v1", got, err)
	}
	if w.try("test -e " + target + "/later") {
		return fmt.Errorf("the restored volume holds a file written after the snapshot was taken")
	}

	bad := createReq("pvc-bad", mi)
	bad.VolumeContentSource = &csipb.VolumeContentSource{Type: &csipb.VolumeContentSource_Snapshot{Snapshot: &csipb.VolumeContentSource_SnapshotSource{SnapshotId: "no-such-snapshot"}}}
	_, err = w.ctl.CreateVolume(c, bad)
	if err := wantCode(err, codes.NotFound, "CreateVolume from a snapshot that does not exist"); err != nil {
		return err
	}
	return w.down(nv.VolumeId, staging, target)
}

func stageClone(ctx context.Context, env *kube.Env, bin string) error {
	w, err := running(ctx, env, "clone", bin)
	if err != nil {
		return err
	}
	defer w.close()

	if err := w.wantCtlCap(csipb.ControllerServiceCapability_RPC_CLONE_VOLUME); err != nil {
		return err
	}
	src, staging, target, err := w.up("pvc-src", 2*mi)
	if err != nil {
		return err
	}
	if err := w.write(target+"/f", "v1"); err != nil {
		return err
	}
	from := &csipb.VolumeContentSource{Type: &csipb.VolumeContentSource_Volume{Volume: &csipb.VolumeContentSource_VolumeSource{VolumeId: src.VolumeId}}}
	req := createReq("pvc-clone", 2*mi)
	req.VolumeContentSource = from
	cl, err := w.create(req)
	if err != nil {
		return fmt.Errorf("cloning %s: %w", src.VolumeId, err)
	}
	if cl.VolumeId == src.VolumeId {
		return fmt.Errorf("the clone has the source's volume id")
	}
	if cl.GetContentSource().GetVolume().GetVolumeId() != src.VolumeId {
		return fmt.Errorf("the clone's content_source is %v; it should name volume %s", cl.GetContentSource(), src.VolumeId)
	}
	if err := w.write(target+"/f", "v2"); err != nil {
		return err
	}
	cs, err := w.stage(cl.VolumeId, mountCap(""))
	if err != nil {
		return err
	}
	ct, err := w.publish(cl.VolumeId, cs, "clone", mountCap(""), false)
	if err != nil {
		return err
	}
	if got, err := w.read(ct + "/f"); err != nil || got != "v1" {
		return fmt.Errorf("the clone reads %q (%v); it was cloned when the source said v1, and is its own copy from then on", got, err)
	}
	if err := w.write(ct+"/f", "clone"); err != nil {
		return err
	}
	if got, _ := w.read(target + "/f"); got != "v2" {
		return fmt.Errorf("writing to the clone changed the source (%q)", got)
	}

	c, cancel := w.call()
	defer cancel()
	small := createReq("pvc-small", mi)
	small.VolumeContentSource = from
	_, err = w.ctl.CreateVolume(c, small)
	if err := wantCodeIn(err, []codes.Code{codes.OutOfRange, codes.InvalidArgument}, "cloning a 2Mi volume into 1Mi"); err != nil {
		return err
	}
	bad := createReq("pvc-bad", 2*mi)
	bad.VolumeContentSource = &csipb.VolumeContentSource{Type: &csipb.VolumeContentSource_Volume{Volume: &csipb.VolumeContentSource_VolumeSource{VolumeId: "no-such-volume"}}}
	_, err = w.ctl.CreateVolume(c, bad)
	if err := wantCode(err, codes.NotFound, "cloning a volume that does not exist"); err != nil {
		return err
	}
	if err := w.down(cl.VolumeId, cs, ct); err != nil {
		return err
	}
	return w.down(src.VolumeId, staging, target)
}

func stageCapacity(ctx context.Context, env *kube.Env, bin string) error {
	const total = 100 * mi
	w, err := running(ctx, env, "capacity", bin, "--capacity", strconv.FormatInt(total, 10))
	if err != nil {
		return err
	}
	defer w.close()

	if err := w.wantCtlCap(csipb.ControllerServiceCapability_RPC_GET_CAPACITY); err != nil {
		return err
	}
	avail := func(when string) (int64, error) {
		c, cancel := w.call()
		defer cancel()
		r, err := w.ctl.GetCapacity(c, &csipb.GetCapacityRequest{VolumeCapabilities: []*csipb.VolumeCapability{mountCap("")}})
		if err != nil {
			return 0, fmt.Errorf("GetCapacity %s: %w", when, err)
		}
		return r.AvailableCapacity, nil
	}
	want := func(when string, n int64) error {
		got, err := avail(when)
		if err != nil {
			return err
		}
		if got != n {
			return fmt.Errorf("GetCapacity %s reports %d bytes available, want %d (--capacity %d less the capacity_bytes of every volume)", when, got, n, total)
		}
		return nil
	}
	if err := want("with no volumes", total); err != nil {
		return err
	}
	a, err := w.create(createReq("pvc-a", 30*mi))
	if err != nil {
		return err
	}
	if err := want("after a 30Mi volume", total-a.CapacityBytes); err != nil {
		return err
	}
	c, cancel := w.call()
	defer cancel()
	_, err = w.ctl.CreateVolume(c, createReq("pvc-big", 80*mi))
	if err := wantCode(err, codes.ResourceExhausted, "an 80Mi volume with 70Mi left"); err != nil {
		return err
	}
	if err := want("after a CreateVolume that did not fit", total-a.CapacityBytes); err != nil {
		return err
	}
	b, err := w.create(createReq("pvc-b", 50*mi))
	if err != nil {
		return err
	}
	if err := w.restart(); err != nil {
		return err
	}
	if err := want("after a restart", total-a.CapacityBytes-b.CapacityBytes); err != nil {
		return err
	}
	if err := w.delete(a.VolumeId); err != nil {
		return err
	}
	return want("after deleting the 30Mi volume", total-b.CapacityBytes)
}

func stageIdempotency(ctx context.Context, env *kube.Env, bin string) error {
	w, err := running(ctx, env, "idempotency", bin)
	if err != nil {
		return err
	}
	defer w.close()

	const n = 8
	before, err := w.volumeIDs()
	if err != nil {
		return err
	}
	// An ext4 volume, so each stage runs blkid, mkfs and mount: long enough
	// for unserialised calls to overlap.
	race := createReq("pvc-race", 64*mi)
	race.VolumeCapabilities = []*csipb.VolumeCapability{mountCap("ext4")}
	ids := make(chan string, n)
	errs := concurrently(n, func() error {
		v, err := w.create(race)
		if err == nil {
			ids <- v.VolumeId
		}
		return err
	})
	close(ids)
	if err := allOK(errs, "CreateVolume pvc-race sent 8 times at once"); err != nil {
		return err
	}
	var id string
	for got := range ids {
		if id != "" && got != id {
			return fmt.Errorf("CreateVolume pvc-race sent 8 times at once answered different volume ids, %s and %s", id, got)
		}
		id = got
	}
	after, err := w.volumeIDs()
	if err != nil {
		return err
	}
	if len(after) != len(before)+1 {
		return fmt.Errorf("CreateVolume pvc-race sent 8 times at once left %d volumes where there were %d; want exactly one more", len(after), len(before))
	}

	staging := w.co + "/stage/race"
	if _, err := w.sh("mkdir -p " + staging); err != nil {
		return err
	}
	errs = concurrently(n, func() error {
		c, cancel := w.call()
		defer cancel()
		_, err := w.nd.NodeStageVolume(c, &csipb.NodeStageVolumeRequest{VolumeId: id, StagingTargetPath: staging, VolumeCapability: mountCap("ext4")})
		return err
	})
	if err := allOK(errs, "NodeStageVolume sent 8 times at once"); err != nil {
		return err
	}
	if err := w.wantMounted(staging, "after 8 NodeStageVolume calls at once"); err != nil {
		return err
	}
	target := w.co + "/pods/race/mount"
	if _, err := w.sh("mkdir -p " + w.co + "/pods/race"); err != nil {
		return err
	}
	errs = concurrently(n, func() error {
		c, cancel := w.call()
		defer cancel()
		_, err := w.nd.NodePublishVolume(c, &csipb.NodePublishVolumeRequest{VolumeId: id, StagingTargetPath: staging, TargetPath: target, VolumeCapability: mountCap("ext4")})
		return err
	})
	if err := allOK(errs, "NodePublishVolume sent 8 times at once"); err != nil {
		return err
	}
	if err := w.wantMounted(target, "after 8 NodePublishVolume calls at once"); err != nil {
		return err
	}
	errs = concurrently(n, func() error { return w.unpublish(id, target) })
	if err := allOK(errs, "NodeUnpublishVolume sent 8 times at once"); err != nil {
		return err
	}
	errs = concurrently(n, func() error { return w.unstage(id, staging) })
	if err := allOK(errs, "NodeUnstageVolume sent 8 times at once"); err != nil {
		return err
	}
	if m, err := w.mountsUnder(w.co); err != nil {
		return err
	} else if len(m) > 0 {
		return fmt.Errorf("after the unpublish and unstage calls, mounts remain: %v", m)
	}
	errs = concurrently(n, func() error { return w.delete(id) })
	if err := allOK(errs, "DeleteVolume sent 8 times at once"); err != nil {
		return err
	}
	if after, err = w.volumeIDs(); err != nil {
		return err
	}
	if slices.Contains(after, id) {
		return fmt.Errorf("after DeleteVolume, ListVolumes still reports %s", id)
	}

	// Different names at once must not lose each other's records.
	names := make(chan string, n)
	for i := range n {
		names <- fmt.Sprintf("pvc-many-%d", i)
	}
	close(names)
	errs = concurrently(n, func() error {
		_, err := w.create(createReq(<-names, mi))
		return err
	})
	if err := allOK(errs, "8 different CreateVolume calls at once"); err != nil {
		return err
	}
	if err := w.restart(); err != nil {
		return err
	}
	if after, err = w.volumeIDs(); err != nil {
		return err
	}
	if len(after) != len(before)+n {
		return fmt.Errorf("after 8 different volumes were created at once and the driver restarted, ListVolumes reports %d volumes, want %d; concurrent writes to the driver's records lost some", len(after), len(before)+n)
	}
	return nil
}

// allOK accepts success, or Aborted: the spec lets a driver refuse a call
// while the same operation on the same volume is in flight.
func allOK(errs []error, what string) error {
	ok := 0
	for _, err := range errs {
		switch status.Code(err) {
		case codes.OK:
			ok++
		case codes.Aborted:
		default:
			return fmt.Errorf("%s: %v", what, err)
		}
	}
	if ok == 0 {
		return fmt.Errorf("%s: every call was Aborted; one of them should have done the work", what)
	}
	return nil
}
