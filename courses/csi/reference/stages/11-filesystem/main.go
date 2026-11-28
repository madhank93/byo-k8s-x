// Your CSI driver.
//
// This one file grows for the whole course. It is built for Linux and run as
// root inside a kind worker, so set GOOS=linux in your editor.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	registration "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

const version = "0.1.0"

// topologyKey names the one place a volume can be used from: the node whose
// disk holds it.
const topologyKey = "topology.byok8s.dev/node"

type driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer

	name     string
	nodeID   string
	root     string
	endpoint string // the CSI socket's path
}

func main() {
	endpoint := flag.String("endpoint", "", "the socket to serve CSI on, unix:///path")
	d := &driver{}
	flag.StringVar(&d.name, "name", "", "the driver name")
	flag.StringVar(&d.nodeID, "node-id", "", "the node this driver runs on")
	flag.StringVar(&d.root, "root", "", "where the driver keeps its volumes")
	regSocket := flag.String("registration-socket", "", "where to serve the kubelet's plugin registration API, if anywhere")
	flag.Parse()
	if err := d.serve(*endpoint, *regSocket); err != nil {
		log.Fatal(err)
	}
}

// serve listens on the endpoint, and on the registration socket if there is
// one, until SIGTERM; then it stops and removes both sockets.
func (d *driver) serve(endpoint, regSocket string) error {
	path, ok := strings.CutPrefix(endpoint, "unix://")
	if !ok {
		return fmt.Errorf("--endpoint %q is not unix://", endpoint)
	}
	d.endpoint = path
	lis, err := listen(path)
	if err != nil {
		return err
	}
	defer os.Remove(path)
	srv := grpc.NewServer()
	csi.RegisterIdentityServer(srv, d)
	csi.RegisterControllerServer(srv, d)
	csi.RegisterNodeServer(srv, d)
	servers := []*grpc.Server{srv}

	// The kubelet watches its plugins_registry directory: a socket appearing
	// there is a plugin asking to be registered, and its going is the plugin
	// leaving.
	if regSocket != "" {
		rl, err := listen(regSocket)
		if err != nil {
			return err
		}
		defer os.Remove(regSocket)
		reg := grpc.NewServer()
		registration.RegisterRegistrationServer(reg, &registrar{d: d})
		servers = append(servers, reg)
		go reg.Serve(rl)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() {
		<-ctx.Done()
		for _, s := range servers {
			s.GracefulStop()
		}
	}()
	log.Printf("%s serving on %s", d.name, path)
	return srv.Serve(lis)
}

// listen opens a unix socket at path, replacing whatever a crashed run left
// there.
func listen(path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return net.Listen("unix", path)
}

// registrar answers the kubelet's plugin watcher: which kind of plugin this
// is, its name, and where its CSI endpoint is.
type registrar struct {
	registration.UnimplementedRegistrationServer
	d *driver
}

func (r *registrar) GetInfo(context.Context, *registration.InfoRequest) (*registration.PluginInfo, error) {
	return &registration.PluginInfo{
		Type:              registration.CSIPlugin,
		Name:              r.d.name,
		Endpoint:          r.d.endpoint,
		SupportedVersions: []string{"1.0.0"},
	}, nil
}

func (r *registrar) NotifyRegistrationStatus(_ context.Context, s *registration.RegistrationStatus) (*registration.RegistrationStatusResponse, error) {
	if !s.PluginRegistered {
		log.Printf("the kubelet refused the registration: %s", s.Error)
	}
	return &registration.RegistrationStatusResponse{}, nil
}

func (d *driver) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: d.name, VendorVersion: version}, nil
}

func (d *driver) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{Capabilities: []*csi.PluginCapability{
		{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_CONTROLLER_SERVICE}}},
		{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS}}},
	}}, nil
}

func (d *driver) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

// controllerCaps is what this driver's controller service implements, and so
// all it advertises.
var controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
	csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
}

func (d *driver) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	var caps []*csi.ControllerServiceCapability
	for _, c := range controllerCaps {
		caps = append(caps, &csi.ControllerServiceCapability{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: c}}})
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

// defaultCapacity is a volume's size when the request names none.
const defaultCapacity = 1 << 30

// volume is what the driver remembers about one volume, kept as JSON under
// --root so it outlives the process.
type volume struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Capacity int64  `json:"capacity"`
	// FsType is "" for a directory volume, or the filesystem made on an
	// image file that is attached as a loop device.
	FsType string `json:"fsType,omitempty"`
}

func (d *driver) CreateVolume(_ context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume_capabilities are required")
	}
	fsType := ""
	for _, c := range req.VolumeCapabilities {
		if c.GetMount() == nil {
			return nil, status.Error(codes.InvalidArgument, "only filesystem volumes are supported")
		}
		switch t := c.GetMount().FsType; t {
		case "":
		case "ext4":
			fsType = t
		default:
			return nil, status.Errorf(codes.InvalidArgument, "fs_type %q is not supported; ext4 is", t)
		}
	}
	size, err := capacity(req.CapacityRange)
	if err != nil {
		return nil, err
	}
	if !d.reachable(req.AccessibilityRequirements) {
		return nil, status.Errorf(codes.ResourceExhausted, "volumes live on node %s, which no requisite topology includes", d.nodeID)
	}

	id := volumeID(req.Name)
	if v, err := d.load(id); err == nil {
		if v.Capacity < size || (req.CapacityRange.GetLimitBytes() > 0 && v.Capacity > req.CapacityRange.GetLimitBytes()) {
			return nil, status.Errorf(codes.AlreadyExists, "volume %s exists with capacity %d", req.Name, v.Capacity)
		}
		return &csi.CreateVolumeResponse{Volume: d.csiVolume(v)}, nil
	} else if !os.IsNotExist(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}

	v := &volume{ID: id, Name: req.Name, Capacity: size, FsType: fsType}
	if err := d.makeStorage(v); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := d.save(v); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.CreateVolumeResponse{Volume: d.csiVolume(v)}, nil
}

// reachable is whether a volume made on this node satisfies the CO's
// requisite topologies; with none, any node will do.
func (d *driver) reachable(req *csi.TopologyRequirement) bool {
	if len(req.GetRequisite()) == 0 {
		return true
	}
	for _, t := range req.Requisite {
		if t.Segments[topologyKey] == d.nodeID {
			return true
		}
	}
	return false
}

// capacity picks a volume's size from the range a CO asked for.
func capacity(r *csi.CapacityRange) (int64, error) {
	req, limit := r.GetRequiredBytes(), r.GetLimitBytes()
	if req < 0 || limit < 0 || (limit > 0 && req > limit) {
		return 0, status.Errorf(codes.OutOfRange, "capacity range %d..%d is not satisfiable", req, limit)
	}
	if req > 0 {
		return req, nil
	}
	if limit > 0 && limit < defaultCapacity {
		return limit, nil
	}
	return defaultCapacity, nil
}

// volumeID derives the id from the name, so a retried CreateVolume finds the
// volume it already made.
func volumeID(name string) string {
	sum := sha256.Sum256([]byte(name))
	return "vol-" + hex.EncodeToString(sum[:8])
}

// csiVolume is a volume as the CO sees it: it is reachable only from the node
// whose disk holds it.
func (d *driver) csiVolume(v *volume) *csi.Volume {
	return &csi.Volume{
		VolumeId:           v.ID,
		CapacityBytes:      v.Capacity,
		AccessibleTopology: []*csi.Topology{{Segments: map[string]string{topologyKey: d.nodeID}}},
	}
}

func (d *driver) metaPath(id string) string  { return filepath.Join(d.root, "volumes", id+".json") }
func (d *driver) dataPath(id string) string  { return filepath.Join(d.root, "data", id) }
func (d *driver) imagePath(id string) string { return filepath.Join(d.root, "data", id+".img") }

// makeStorage makes what holds a volume's data: a directory, or a sparse
// image file of the volume's size.
func (d *driver) makeStorage(v *volume) error {
	if v.FsType == "" {
		return os.MkdirAll(d.dataPath(v.ID), 0o755)
	}
	if err := os.MkdirAll(filepath.Dir(d.imagePath(v.ID)), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(d.imagePath(v.ID), os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(v.Capacity)
}

func (d *driver) load(id string) (*volume, error) {
	b, err := os.ReadFile(d.metaPath(id))
	if err != nil {
		return nil, err
	}
	v := &volume{}
	return v, json.Unmarshal(b, v)
}

// save writes a record whole or not at all: a crash mid-write must not leave
// a half-written file behind.
func (d *driver) save(v *volume) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	path := d.metaPath(v.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (d *driver) DeleteVolume(_ context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if err := os.RemoveAll(d.dataPath(req.VolumeId)); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := os.Remove(d.imagePath(req.VolumeId)); err != nil && !os.IsNotExist(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := os.Remove(d.metaPath(req.VolumeId)); err != nil && !os.IsNotExist(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// ListVolumes answers every volume in one page.
func (d *driver) ListVolumes(context.Context, *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	vols, err := d.all()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &csi.ListVolumesResponse{}
	for _, v := range vols {
		resp.Entries = append(resp.Entries, &csi.ListVolumesResponse_Entry{Volume: d.csiVolume(v)})
	}
	return resp, nil
}

func (d *driver) all() ([]*volume, error) {
	files, err := filepath.Glob(filepath.Join(d.root, "volumes", "*.json"))
	if err != nil {
		return nil, err
	}
	var vols []*volume
	for _, f := range files {
		v, err := d.load(strings.TrimSuffix(filepath.Base(f), ".json"))
		if err != nil {
			return nil, err
		}
		vols = append(vols, v)
	}
	return vols, nil
}

// nodeCaps is what this driver's node service implements.
var nodeCaps = []csi.NodeServiceCapability_RPC_Type{
	csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
}

func (d *driver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	var caps []*csi.NodeServiceCapability
	for _, c := range nodeCaps {
		caps = append(caps, &csi.NodeServiceCapability{Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: c}}})
	}
	return &csi.NodeGetCapabilitiesResponse{Capabilities: caps}, nil
}

func (d *driver) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId:             d.nodeID,
		AccessibleTopology: &csi.Topology{Segments: map[string]string{topologyKey: d.nodeID}},
	}, nil
}

// NodeStageVolume mounts the volume's storage at the staging path, once per
// node; every pod that uses the volume is then a bind mount of that.
func (d *driver) NodeStageVolume(_ context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.VolumeId == "" || req.StagingTargetPath == "" || req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume_id, staging_target_path and volume_capability are required")
	}
	v, err := d.load(req.VolumeId)
	if os.IsNotExist(err) {
		return nil, status.Errorf(codes.NotFound, "no volume %s", req.VolumeId)
	} else if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if v.FsType == "" {
		err = bindOnce(d.dataPath(v.ID), req.StagingTargetPath, false)
	} else {
		err = d.mountImage(v, req.StagingTargetPath)
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

// mountImage attaches the volume's image as a loop device and mounts its
// filesystem at path, making the filesystem first if the image has none.
// mount's loop option sets autoclear, so the last unmount detaches the device.
func (d *driver) mountImage(v *volume, path string) error {
	if ok, err := mounted(path); err != nil || ok {
		return err
	}
	img := d.imagePath(v.ID)
	// blkid exits 2 when it finds no filesystem; any other failure must not
	// lead to formatting over data.
	var exit *exec.ExitError
	if err := exec.Command("blkid", "-p", img).Run(); errors.As(err, &exit) && exit.ExitCode() == 2 {
		if out, err := exec.Command("mkfs."+v.FsType, "-q", "-F", img).CombinedOutput(); err != nil {
			return fmt.Errorf("mkfs.%s: %w: %s", v.FsType, err, out)
		}
	} else if err != nil {
		return fmt.Errorf("blkid %s: %w", img, err)
	}
	if out, err := exec.Command("mount", "-t", v.FsType, "-o", "loop", img, path).CombinedOutput(); err != nil {
		return fmt.Errorf("mount %s: %w: %s", img, err, out)
	}
	return nil
}

// bindOnce bind-mounts src at dst unless something is mounted there already,
// so a retried call does not stack a second mount.
func bindOnce(src, dst string, readonly bool) error {
	if ok, err := mounted(dst); err != nil || ok {
		return err
	}
	if err := syscall.Mount(src, dst, "", syscall.MS_BIND, ""); err != nil {
		return fmt.Errorf("bind %s at %s: %w", src, dst, err)
	}
	if readonly {
		// A bind mount takes its source's flags; read-only needs a remount.
		if err := syscall.Mount("", dst, "", syscall.MS_BIND|syscall.MS_REMOUNT|syscall.MS_RDONLY, ""); err != nil {
			syscall.Unmount(dst, 0)
			return fmt.Errorf("remount %s read-only: %w", dst, err)
		}
	}
	return nil
}

// mounted is whether anything is mounted exactly on path.
func mounted(path string) (bool, error) {
	b, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return false, err
	}
	for _, l := range strings.Split(string(b), "\n") {
		// Field 5 is the mount point, with spaces and the like octal-escaped.
		if f := strings.Fields(l); len(f) > 4 && f[4] == filepath.Clean(path) {
			return true, nil
		}
	}
	return false, nil
}

// NodePublishVolume makes the staged volume appear at a pod's target path,
// read-only if the pod asked.
func (d *driver) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.VolumeId == "" || req.TargetPath == "" || req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume_id, target_path and volume_capability are required")
	}
	if _, err := d.load(req.VolumeId); os.IsNotExist(err) {
		return nil, status.Errorf(codes.NotFound, "no volume %s", req.VolumeId)
	} else if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.FailedPrecondition, "staging_target_path is required; the volume must be staged first")
	}
	// The CO makes the target's parent; the target itself is the driver's.
	if err := os.MkdirAll(req.TargetPath, 0o755); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := bindOnce(req.StagingTargetPath, req.TargetPath, req.Readonly); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the target and removes it; a target that is
// already gone is success.
func (d *driver) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.VolumeId == "" || req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and target_path are required")
	}
	if err := unmount(req.TargetPath); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := os.Remove(req.TargetPath); err != nil && !os.IsNotExist(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeUnstageVolume unmounts the staging path. The directory is the CO's, so
// it stays.
func (d *driver) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.VolumeId == "" || req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and staging_target_path are required")
	}
	if err := unmount(req.StagingTargetPath); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// unmount unmounts path if anything is mounted there.
func unmount(path string) error {
	ok, err := mounted(path)
	if err != nil || !ok {
		return err
	}
	if err := syscall.Unmount(path, 0); err != nil {
		return fmt.Errorf("unmount %s: %w", path, err)
	}
	return nil
}
