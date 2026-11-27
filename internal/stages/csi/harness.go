// Package csi holds the assertions for the "Build your own CSI driver" course
// — one function per stage, registered by slug.
//
// The learner's driver is built for Linux and runs inside a kind worker, as
// root, through runner.Node, serving CSI on a unix socket under the kubelet's
// plugin directory. The grader plays the container orchestrator: it calls the
// driver over that socket, then checks what the calls did to the node's mount
// table and files. The later stages hand the driver to the real kubelet and
// the real external-provisioner, and grade the pods they run.
package csi

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/madhank93/byo-k8s-x/internal/cluster"
	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// Stage is one gradable step, in the shape cmd/tester dispatches on.
type Stage = stages.Stage

var stageRegistry = map[string]Stage{}

func register(s Stage) { stageRegistry[s.Slug] = s }

// Lookup returns the stage with this slug.
func Lookup(slug string) (Stage, bool) {
	s, ok := stageRegistry[slug]
	return s, ok
}

const (
	// nodeID is the worker the driver runs on, and the id it reports.
	nodeID = cluster.Name + "-worker"
	node   = runner.Node(nodeID)

	// topologyKey is the one segment the driver's volumes are constrained by.
	topologyKey = "topology.byok8s.dev/node"

	// baseDir holds each stage's --root and the directories the grader stages
	// and publishes into.
	baseDir = "/var/lib/byok8s-csi"

	bridgePath = "/usr/local/bin/byok8s-csi-bridge"

	mi = int64(1 << 20)
)

// work is one stage's driver: its name, its paths, the running process and a
// client connected to it.
type work struct {
	ctx  context.Context
	env  *kube.Env
	name string // the driver name, unique per run
	root string // --root
	co   string // where the grader, as the CO, stages and publishes
	sock string // the CSI endpoint
	reg  string // the kubelet registration socket
	args []string

	proc *runner.Process
	conn *grpc.ClientConn
	id   csipb.IdentityClient
	ctl  csipb.ControllerClient
	nd   csipb.NodeClient
}

func newWork(ctx context.Context, env *kube.Env, slug, bin string) (*work, error) {
	if err := node.Install(ctx, bin); err != nil {
		return nil, err
	}
	br, err := bridgeBinary(ctx)
	if err != nil {
		return nil, err
	}
	if err := node.WriteFile(ctx, bridgePath, br, 0o755); err != nil {
		return nil, err
	}
	name := "csi-" + randHex(4) + ".byok8s.dev"
	w := &work{
		ctx:  ctx,
		env:  env,
		name: name,
		root: baseDir + "/" + slug,
		co:   baseDir + "/" + slug + "-co",
		sock: "/var/lib/kubelet/plugins/" + name + "/csi.sock",
		reg:  "/var/lib/kubelet/plugins_registry/" + name + "-reg.sock",
	}
	if err := w.clean(); err != nil {
		return nil, err
	}
	// A loop device the kernel hands out has no node in a kind node's /dev,
	// which has no udev to make one.
	if _, err := w.sh(`for i in $(seq 0 255); do [ -e /dev/loop$i ] || mknod /dev/loop$i b 7 $i; done`); err != nil {
		return nil, err
	}
	if _, err := w.sh("mkdir -p " + w.co); err != nil {
		return nil, err
	}
	return w, nil
}

// close stops the driver and removes everything the stage made in the node.
func (w *work) close() {
	w.stop(5 * time.Second)
	w.clean()
}

// clean kills anything a previous run left, unmounts and detaches whatever
// lives under this stage's directories, and removes them and every
// byok8s.dev driver's sockets, so the kubelet deregisters drivers no one runs.
func (w *work) clean() error {
	if err := node.Sweep(w.ctx); err != nil {
		return err
	}
	script := fmt.Sprintf(`
for m in $(awk '{print $5}' /proc/self/mountinfo | grep -E '^(%[1]s|%[2]s)(/|$)' | sort -r); do umount -l "$m"; done
losetup -a | grep -F '(%[1]s/' | cut -d: -f1 | xargs -r -n1 losetup -d
rm -rf %[1]s %[2]s /var/lib/kubelet/plugins/csi-*.byok8s.dev /var/lib/kubelet/plugins_registry/csi-*.byok8s.dev-reg.sock
true`, w.root, w.co)
	_, err := w.sh(script)
	return err
}

// start runs the driver with the stage's arguments plus extra, and waits for
// it to answer Probe.
func (w *work) start(extra ...string) error {
	args := append([]string{"--endpoint", "unix://" + w.sock, "--name", w.name, "--node-id", nodeID, "--root", w.root}, w.args...)
	args = append(args, extra...)
	p, err := node.Start(w.ctx, nil, args...)
	if err != nil {
		return err
	}
	w.proc = p
	w.conn, err = grpc.NewClient("passthrough:///csi",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialNode(w.sock)),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.5, MaxDelay: time.Second},
			MinConnectTimeout: 3 * time.Second,
		}))
	if err != nil {
		return err
	}
	w.id = csipb.NewIdentityClient(w.conn)
	w.ctl = csipb.NewControllerClient(w.conn)
	w.nd = csipb.NewNodeClient(w.conn)

	ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()
	var last error
	err = kube.WaitFor(ctx, "the driver to answer Probe on "+w.sock, func(ctx context.Context) (bool, error) {
		if done, res := p.Exited(); done {
			return false, fmt.Errorf("the driver exited with %d before it served:\n%s", res.ExitCode, tail(res.Stderr+res.Stdout))
		}
		c, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		r, err := w.id.Probe(c, &csipb.ProbeRequest{})
		last = err
		return err == nil && (r.GetReady() == nil || r.GetReady().GetValue()), nil
	})
	if err != nil && last != nil {
		return fmt.Errorf("%w (last answer: %v)\n%s", err, last, tail(w.proc.Stderr()))
	}
	return err
}

// stop sends SIGTERM and returns how the driver exited.
func (w *work) stop(grace time.Duration) *runner.Result {
	if w.conn != nil {
		w.conn.Close()
		w.conn = nil
	}
	if w.proc == nil {
		return nil
	}
	res := w.proc.Stop(grace)
	w.proc = nil
	return res
}

func (w *work) restart() error {
	if res := w.stop(10 * time.Second); res != nil && res.ExitCode != 0 {
		return fmt.Errorf("on SIGTERM the driver should exit 0, it exited %d:\n%s", res.ExitCode, tail(res.Stderr))
	}
	return w.start()
}

// dialNode connects to a unix socket inside the node through the bridge.
func dialNode(sock string) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, _ string) (net.Conn, error) {
		cmd := exec.Command("docker", "exec", "-i", string(node), bridgePath, sock)
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return nil, err
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		ours, theirs := net.Pipe()
		go func() {
			io.Copy(stdin, theirs)
			stdin.Close()
		}()
		go func() {
			io.Copy(theirs, stdout)
			theirs.Close()
			cmd.Wait()
		}()
		return ours, nil
	}
}

//go:embed bridge/main.go
var bridgeSource []byte

var (
	bridgeOnce sync.Once
	bridgeBin  []byte
	bridgeErr  error
)

// bridgeBinary builds the bridge for the node's architecture, once per run.
func bridgeBinary(ctx context.Context) ([]byte, error) {
	bridgeOnce.Do(func() {
		dir, err := os.MkdirTemp("", "byok8s-bridge-")
		if err != nil {
			bridgeErr = err
			return
		}
		defer os.RemoveAll(dir)
		os.WriteFile(filepath.Join(dir, "main.go"), bridgeSource, 0o644)
		os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module bridge\n\ngo 1.25\n"), 0o644)
		arch, err := runner.NodeArch(ctx)
		if err != nil {
			bridgeErr = err
			return
		}
		bin, err := runner.BuildFor(ctx, dir, "linux", arch)
		if err != nil {
			bridgeErr = fmt.Errorf("build the socket bridge: %w", err)
			return
		}
		defer os.RemoveAll(filepath.Dir(bin))
		bridgeBin, bridgeErr = os.ReadFile(bin)
	})
	return bridgeBin, bridgeErr
}

// sh runs a harness script in the node and returns its stdout, failing on a
// non-zero exit.
func (w *work) sh(script string) (string, error) {
	res, err := node.Exec(w.ctx, 2*time.Minute, "sh", "-c", script)
	if err != nil {
		return "", err
	}
	if res.ExitCode != 0 {
		return "", fmt.Errorf("in node %s, %q failed: %s", node, script, strings.TrimSpace(res.Stderr))
	}
	return res.Stdout, nil
}

// try runs a script in the node and reports only whether it succeeded.
func (w *work) try(script string) bool {
	res, err := node.Exec(w.ctx, time.Minute, "sh", "-c", script)
	return err == nil && res.ExitCode == 0
}

// mounts is how many mounts sit exactly on path.
func (w *work) mounts(path string) (int, error) {
	out, err := w.sh(fmt.Sprintf(`awk -v p=%q '$5==p' /proc/self/mountinfo | wc -l`, path))
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(out))
}

// wantMounted fails unless exactly one mount sits on path.
func (w *work) wantMounted(path, what string) error {
	n, err := w.mounts(path)
	if err != nil {
		return err
	}
	switch n {
	case 1:
		return nil
	case 0:
		return fmt.Errorf("%s: nothing is mounted on %s", what, path)
	default:
		return fmt.Errorf("%s: %d mounts are stacked on %s; a repeated call must not mount again", what, n, path)
	}
}

func (w *work) wantUnmounted(path, what string) error {
	n, err := w.mounts(path)
	if err != nil {
		return err
	}
	if n != 0 {
		return fmt.Errorf("%s: %s is still mounted", what, path)
	}
	return nil
}

// mountsUnder lists the mounts below dir.
func (w *work) mountsUnder(dir string) ([]string, error) {
	out, err := w.sh(fmt.Sprintf(`awk '{print $5}' /proc/self/mountinfo | grep -E '^%s(/|$)' || true`, dir))
	if err != nil {
		return nil, err
	}
	return lines(out), nil
}

// loopsUnder lists the loop devices whose backing file is below dir.
func (w *work) loopsUnder(dir string) ([]string, error) {
	out, err := w.sh(fmt.Sprintf(`losetup -a | grep -F '(%s/' || true`, dir))
	if err != nil {
		return nil, err
	}
	return lines(out), nil
}

func (w *work) write(path, data string) error {
	_, err := w.sh(fmt.Sprintf(`printf %%s %q > %q`, data, path))
	return err
}

func (w *work) read(path string) (string, error) {
	return w.sh(fmt.Sprintf(`cat %q`, path))
}

func (w *work) call() (context.Context, context.CancelFunc) {
	return context.WithTimeout(w.ctx, 30*time.Second)
}

// mountCap is a filesystem volume written by one node; fsType "" means the
// driver's default.
func mountCap(fsType string) *csipb.VolumeCapability {
	return &csipb.VolumeCapability{
		AccessType: &csipb.VolumeCapability_Mount{Mount: &csipb.VolumeCapability_MountVolume{FsType: fsType}},
		AccessMode: &csipb.VolumeCapability_AccessMode{Mode: csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}
}

func createReq(name string, bytes int64) *csipb.CreateVolumeRequest {
	return &csipb.CreateVolumeRequest{
		Name:               name,
		CapacityRange:      &csipb.CapacityRange{RequiredBytes: bytes},
		VolumeCapabilities: []*csipb.VolumeCapability{mountCap("")},
	}
}

func (w *work) create(req *csipb.CreateVolumeRequest) (*csipb.Volume, error) {
	ctx, cancel := w.call()
	defer cancel()
	r, err := w.ctl.CreateVolume(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("CreateVolume %q: %w", req.Name, err)
	}
	if r.GetVolume().GetVolumeId() == "" {
		return nil, fmt.Errorf("CreateVolume %q answered without a volume_id", req.Name)
	}
	return r.Volume, nil
}

// stage stages vol at <co>/stage/<id> with cap, creating the directory the way
// a CO does.
func (w *work) stage(vol string, cp *csipb.VolumeCapability) (string, error) {
	path := w.co + "/stage/" + safe(vol)
	if _, err := w.sh("mkdir -p " + path); err != nil {
		return "", err
	}
	ctx, cancel := w.call()
	defer cancel()
	if _, err := w.nd.NodeStageVolume(ctx, &csipb.NodeStageVolumeRequest{VolumeId: vol, StagingTargetPath: path, VolumeCapability: cp}); err != nil {
		return "", fmt.Errorf("NodeStageVolume %s at %s: %w", vol, path, err)
	}
	return path, nil
}

// publish publishes a staged volume at <co>/pods/<pod>/mount, which it leaves
// to the driver to create.
func (w *work) publish(vol, staging, pod string, cp *csipb.VolumeCapability, ro bool) (string, error) {
	path := w.co + "/pods/" + pod + "/mount"
	if _, err := w.sh("mkdir -p " + filepath.Dir(path)); err != nil {
		return "", err
	}
	ctx, cancel := w.call()
	defer cancel()
	if _, err := w.nd.NodePublishVolume(ctx, &csipb.NodePublishVolumeRequest{
		VolumeId: vol, StagingTargetPath: staging, TargetPath: path, VolumeCapability: cp, Readonly: ro,
	}); err != nil {
		return "", fmt.Errorf("NodePublishVolume %s at %s: %w", vol, path, err)
	}
	return path, nil
}

func (w *work) unpublish(vol, target string) error {
	ctx, cancel := w.call()
	defer cancel()
	if _, err := w.nd.NodeUnpublishVolume(ctx, &csipb.NodeUnpublishVolumeRequest{VolumeId: vol, TargetPath: target}); err != nil {
		return fmt.Errorf("NodeUnpublishVolume %s at %s: %w", vol, target, err)
	}
	return nil
}

func (w *work) unstage(vol, staging string) error {
	ctx, cancel := w.call()
	defer cancel()
	if _, err := w.nd.NodeUnstageVolume(ctx, &csipb.NodeUnstageVolumeRequest{VolumeId: vol, StagingTargetPath: staging}); err != nil {
		return fmt.Errorf("NodeUnstageVolume %s at %s: %w", vol, staging, err)
	}
	return nil
}

func (w *work) delete(vol string) error {
	ctx, cancel := w.call()
	defer cancel()
	if _, err := w.ctl.DeleteVolume(ctx, &csipb.DeleteVolumeRequest{VolumeId: vol}); err != nil {
		return fmt.Errorf("DeleteVolume %s: %w", vol, err)
	}
	return nil
}

// volumeIDs is every volume ListVolumes reports, following its pages.
func (w *work) volumeIDs() ([]string, error) {
	var ids []string
	token := ""
	for range 100 {
		ctx, cancel := w.call()
		r, err := w.ctl.ListVolumes(ctx, &csipb.ListVolumesRequest{StartingToken: token})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("ListVolumes: %w", err)
		}
		for _, e := range r.Entries {
			ids = append(ids, e.GetVolume().GetVolumeId())
		}
		if token = r.NextToken; token == "" {
			return ids, nil
		}
	}
	return nil, fmt.Errorf("ListVolumes kept returning a next_token")
}

// up creates a directory volume and stages and publishes it, the path every
// data-carrying stage starts from.
func (w *work) up(name string, bytes int64) (vol *csipb.Volume, staging, target string, err error) {
	if vol, err = w.create(createReq(name, bytes)); err != nil {
		return
	}
	if staging, err = w.stage(vol.VolumeId, mountCap("")); err != nil {
		return
	}
	target, err = w.publish(vol.VolumeId, staging, name, mountCap(""), false)
	return
}

// down unpublishes and unstages what up made.
func (w *work) down(vol, staging, target string) error {
	if err := w.unpublish(vol, target); err != nil {
		return err
	}
	return w.unstage(vol, staging)
}

// wantCode fails unless err carries the gRPC code want.
func wantCode(err error, want codes.Code, what string) error {
	got := status.Code(err)
	if got == want {
		return nil
	}
	if err == nil {
		return fmt.Errorf("%s: want %s, the call succeeded", what, want)
	}
	return fmt.Errorf("%s: want %s, got %s (%s)", what, want, got, status.Convert(err).Message())
}

// wantCodeIn is wantCode where the spec allows more than one answer.
func wantCodeIn(err error, want []codes.Code, what string) error {
	if slices.Contains(want, status.Code(err)) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("%s: want one of %v, the call succeeded", what, want)
	}
	return fmt.Errorf("%s: want one of %v, got %s (%s)", what, want, status.Code(err), status.Convert(err).Message())
}

func (w *work) pluginCaps() (svc []csipb.PluginCapability_Service_Type, exp []csipb.PluginCapability_VolumeExpansion_Type, err error) {
	ctx, cancel := w.call()
	defer cancel()
	r, err := w.id.GetPluginCapabilities(ctx, &csipb.GetPluginCapabilitiesRequest{})
	if err != nil {
		return nil, nil, fmt.Errorf("GetPluginCapabilities: %w", err)
	}
	for _, c := range r.Capabilities {
		if s := c.GetService(); s != nil {
			svc = append(svc, s.Type)
		}
		if e := c.GetVolumeExpansion(); e != nil {
			exp = append(exp, e.Type)
		}
	}
	return svc, exp, nil
}

// ctlCaps is what ControllerGetCapabilities advertises; a driver with no
// controller service advertises nothing.
func (w *work) ctlCaps() ([]csipb.ControllerServiceCapability_RPC_Type, error) {
	ctx, cancel := w.call()
	defer cancel()
	r, err := w.ctl.ControllerGetCapabilities(ctx, &csipb.ControllerGetCapabilitiesRequest{})
	if status.Code(err) == codes.Unimplemented {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ControllerGetCapabilities: %w", err)
	}
	var out []csipb.ControllerServiceCapability_RPC_Type
	for _, c := range r.Capabilities {
		out = append(out, c.GetRpc().GetType())
	}
	return out, nil
}

func (w *work) nodeCaps() ([]csipb.NodeServiceCapability_RPC_Type, error) {
	ctx, cancel := w.call()
	defer cancel()
	r, err := w.nd.NodeGetCapabilities(ctx, &csipb.NodeGetCapabilitiesRequest{})
	if status.Code(err) == codes.Unimplemented {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("NodeGetCapabilities: %w", err)
	}
	var out []csipb.NodeServiceCapability_RPC_Type
	for _, c := range r.Capabilities {
		out = append(out, c.GetRpc().GetType())
	}
	return out, nil
}

func (w *work) wantCtlCap(c csipb.ControllerServiceCapability_RPC_Type) error {
	caps, err := w.ctlCaps()
	if err != nil {
		return err
	}
	if !slices.Contains(caps, c) {
		return fmt.Errorf("ControllerGetCapabilities advertises %v; it should include %s", caps, c)
	}
	return nil
}

func (w *work) wantNodeCap(c csipb.NodeServiceCapability_RPC_Type) error {
	caps, err := w.nodeCaps()
	if err != nil {
		return err
	}
	if !slices.Contains(caps, c) {
		return fmt.Errorf("NodeGetCapabilities advertises %v; it should include %s", caps, c)
	}
	return nil
}

// concurrently runs f n times at once and returns each call's error.
func concurrently(n int, f func() error) []error {
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { errs[i] = f() })
	}
	wg.Wait()
	return errs
}

func safe(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '/' || r == ' ' {
			return '_'
		}
		return r
	}, s)
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func lines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}

func tail(s string) string {
	ls := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(ls) > 30 {
		ls = append([]string{"…"}, ls[len(ls)-30:]...)
	}
	return "  " + strings.Join(ls, "\n  ")
}
