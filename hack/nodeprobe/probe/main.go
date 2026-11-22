//go:build linux

// Command probe runs inside a kind node and reports which Linux primitives
// the node-plane courses could grade there. Every check cleans up behind
// itself: namespaces die with the child that made them, and files, mounts,
// cgroups and processes live under names starting byok8s-probe.
package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

const work = "/var/lib/byok8s-probe"

type check struct {
	name string
	run  func() (string, error)
}

func main() {
	if len(os.Args) > 2 && os.Args[1] == "child" {
		fn, ok := children[os.Args[2]]
		if !ok {
			fmt.Fprintln(os.Stderr, "no child", os.Args[2])
			os.Exit(2)
		}
		detail, err := fn()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Print(detail)
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "sleep" {
		fmt.Println("sleeping", os.Getpid())
		time.Sleep(time.Hour)
		return
	}

	checks := []check{
		{"mount-ns", inChild("mount-ns", syscall.CLONE_NEWNS)},
		{"pid-ns", inChild("pid-ns", syscall.CLONE_NEWNS|syscall.CLONE_NEWPID)},
		{"uts-ipc-ns", inChild("uts-ipc-ns", syscall.CLONE_NEWUTS|syscall.CLONE_NEWIPC)},
		{"net-ns", inChild("net-ns", syscall.CLONE_NEWNET)},
		{"user-ns", userNS},
		{"pivot-root", inChild("pivot-root", syscall.CLONE_NEWNS|syscall.CLONE_NEWPID)},
		{"overlayfs", inChild("overlayfs", syscall.CLONE_NEWNS)},
		{"cgroup-v2-limits", cgroupLimits},
		{"capabilities", inChild("capabilities", syscall.CLONE_NEWNS)},
		{"seccomp-bpf", inChild("seccomp", 0)},
		{"landlock", inChild("landlock", 0)},
		{"kvm", kvm},
		{"veth-bridge-routes", inChild("veth", syscall.CLONE_NEWNET)},
		{"vxlan", inChild("vxlan", syscall.CLONE_NEWNET)},
		{"nftables", inChild("nftables", syscall.CLONE_NEWNET)},
		{"loop-mkfs-mount", loopMount},
		{"second-cri-endpoint", secondContainerd},
	}
	failed := 0
	for _, c := range checks {
		os.RemoveAll(work)
		if err := os.MkdirAll(work, 0o755); err != nil {
			fmt.Println("setup", err)
			os.Exit(1)
		}
		detail, err := c.run()
		verdict := "ok"
		if err != nil {
			verdict, detail = "FAIL", err.Error()
			failed++
		}
		fmt.Printf("%-20s %-4s %s\n", c.name, verdict, strings.Join(strings.Fields(detail), " "))
	}
	os.RemoveAll(work)
	fmt.Printf("%d of %d checks failed\n", failed, len(checks))
}

// inChild re-executes this binary in new namespaces and runs the named check
// there, which is how a runtime itself gets code running inside them.
func inChild(name string, flags uintptr) func() (string, error) {
	return func() (string, error) {
		return runChild(name, &syscall.SysProcAttr{Cloneflags: flags})
	}
}

func runChild(name string, attr *syscall.SysProcAttr) (string, error) {
	cmd := exec.Command("/proc/self/exe", "child", name)
	cmd.SysProcAttr = attr
	var out, errb strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%v: %s", err, errb.String())
	}
	return out.String(), nil
}

var children = map[string]func() (string, error){
	"mount-ns":     childMountNS,
	"pid-ns":       childPidNS,
	"uts-ipc-ns":   childUTS,
	"net-ns":       childNetNS,
	"user-ns":      childUserNS,
	"pivot-root":   childPivotRoot,
	"overlayfs":    childOverlay,
	"capabilities": childCaps,
	"seccomp":      childSeccomp,
	"landlock":     childLandlock,
	"veth":         childVeth,
	"vxlan":        childVxlan,
	"nftables":     childNft,
	"loop":         childLoop,
}

func private() error {
	return syscall.Mount("", "/", "", syscall.MS_REC|syscall.MS_PRIVATE, "")
}

func childMountNS() (string, error) {
	if err := private(); err != nil {
		return "", fmt.Errorf("make / private: %w", err)
	}
	dir := filepath.Join(work, "mnt")
	os.MkdirAll(dir, 0o755)
	if err := syscall.Mount("tmpfs", dir, "tmpfs", 0, "size=1m"); err != nil {
		return "", fmt.Errorf("mount tmpfs: %w", err)
	}
	host, _ := os.ReadFile("/proc/1/mountinfo")
	if strings.Contains(string(host), dir) {
		return "", errors.New("the mount leaked into the node's mount namespace")
	}
	return "tmpfs mounted here, absent from the node's mountinfo", nil
}

func childPidNS() (string, error) {
	if os.Getpid() != 1 {
		return "", fmt.Errorf("pid %d, want 1", os.Getpid())
	}
	if err := private(); err != nil {
		return "", err
	}
	if err := syscall.Mount("proc", "/proc", "proc", 0, ""); err != nil {
		return "", fmt.Errorf("mount proc: %w", err)
	}
	ents, _ := os.ReadDir("/proc")
	n := 0
	for _, e := range ents {
		if _, err := strconv.Atoi(e.Name()); err == nil {
			n++
		}
	}
	return fmt.Sprintf("pid 1, a fresh /proc shows %d process(es)", n), nil
}

func childUTS() (string, error) {
	if err := syscall.Sethostname([]byte("byok8s-probe")); err != nil {
		return "", err
	}
	h, _ := os.Hostname()
	mine, _ := os.Readlink("/proc/self/ns/ipc")
	node, _ := os.Readlink("/proc/1/ns/ipc")
	if mine == node {
		return "", errors.New("ipc namespace is the node's")
	}
	return "hostname " + h + ", own ipc namespace", nil
}

func childNetNS() (string, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	var names []string
	for _, i := range ifs {
		names = append(names, i.Name)
	}
	if len(names) != 1 || names[0] != "lo" {
		return "", fmt.Errorf("interfaces %v, want only lo", names)
	}
	return "only lo", nil
}

func userNS() (string, error) {
	// A runtime maps container root to an unprivileged host range, then
	// becomes root inside, which the namespace's creator is allowed to do.
	out, err := runChild("user-ns", &syscall.SysProcAttr{
		Cloneflags:  syscall.CLONE_NEWUSER,
		Credential:  &syscall.Credential{Uid: 0, Gid: 0},
		UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: 100000, Size: 65536}},
		GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: 100000, Size: 65536}},
	})
	if err != nil {
		return "", err
	}
	rootless := "; an unprivileged user can create one too"
	if id, err := sh("setpriv --reuid 65534 --regid 65534 --clear-groups unshare --user --map-root-user id -u"); err != nil || strings.TrimSpace(id) != "0" {
		rootless = fmt.Sprintf("; an unprivileged user cannot (%q %v)", id, err)
	}
	return out + rootless, nil
}

func childUserNS() (string, error) {
	m, _ := os.ReadFile("/proc/self/uid_map")
	if os.Getuid() != 0 {
		return "", fmt.Errorf("uid %d inside, want 0", os.Getuid())
	}
	return "uid 0 inside, uid_map " + strings.Join(strings.Fields(string(m)), " "), nil
}

func childPivotRoot() (string, error) {
	if err := private(); err != nil {
		return "", err
	}
	root := filepath.Join(work, "root")
	os.MkdirAll(filepath.Join(root, "old"), 0o755)
	os.WriteFile(filepath.Join(root, "marker"), []byte("inside"), 0o644)
	if err := syscall.Mount(root, root, "", syscall.MS_BIND|syscall.MS_REC, ""); err != nil {
		return "", fmt.Errorf("bind rootfs: %w", err)
	}
	if err := syscall.PivotRoot(root, filepath.Join(root, "old")); err != nil {
		return "", fmt.Errorf("pivot_root: %w", err)
	}
	if err := os.Chdir("/"); err != nil {
		return "", err
	}
	if err := syscall.Unmount("/old", syscall.MNT_DETACH); err != nil {
		return "", fmt.Errorf("detach old root: %w", err)
	}
	if b, err := os.ReadFile("/marker"); err != nil || string(b) != "inside" {
		return "", fmt.Errorf("/marker not the new root's: %v", err)
	}
	if _, err := os.Stat("/usr/bin"); err == nil {
		return "", errors.New("the node's /usr/bin is still reachable")
	}
	return "new root in place, old root detached", nil
}

func childOverlay() (string, error) {
	if err := private(); err != nil {
		return "", err
	}
	d := filepath.Join(work, "ov")
	for _, s := range []string{"lower", "upper", "work", "merged"} {
		os.MkdirAll(filepath.Join(d, s), 0o755)
	}
	os.WriteFile(filepath.Join(d, "lower", "a"), []byte("a"), 0o644)
	opts := fmt.Sprintf("lowerdir=%s/lower,upperdir=%s/upper,workdir=%s/work", d, d, d)
	if err := syscall.Mount("overlay", filepath.Join(d, "merged"), "overlay", 0, opts); err != nil {
		return "", fmt.Errorf("mount overlay on /var (%s): %w", fstype("/var"), err)
	}
	os.WriteFile(filepath.Join(d, "merged", "b"), []byte("b"), 0o644)
	os.Remove(filepath.Join(d, "merged", "a"))
	var st syscall.Stat_t
	if err := syscall.Stat(filepath.Join(d, "upper", "a"), &st); err != nil || st.Mode&syscall.S_IFMT != syscall.S_IFCHR {
		return "", errors.New("deleting a lower file left no whiteout in upper")
	}
	if _, err := os.Stat(filepath.Join(d, "upper", "b")); err != nil {
		return "", errors.New("a write did not land in upper")
	}
	syscall.Unmount(filepath.Join(d, "merged"), 0)
	return "overlay on " + fstype("/var") + ", copy-up and whiteouts work", nil
}

func fstype(path string) string {
	b, _ := os.ReadFile("/proc/self/mounts")
	best, typ := "", "?"
	for _, l := range strings.Split(string(b), "\n") {
		f := strings.Fields(l)
		if len(f) > 2 && strings.HasPrefix(path, f[1]) && len(f[1]) >= len(best) {
			best, typ = f[1], f[2]
		}
	}
	return typ
}

// limited makes a cgroup with the given limits and returns an fd that starts
// a process inside it, and a cleanup that kills whatever is left and removes it.
func limited(name string, limits map[string]string) (int, func(), error) {
	cg := "/sys/fs/cgroup/byok8s-probe-" + name
	if err := os.Mkdir(cg, 0o755); err != nil {
		return 0, nil, err
	}
	fd, err := syscall.Open(cg, syscall.O_DIRECTORY|syscall.O_RDONLY, 0)
	cleanup := func() {
		syscall.Close(fd)
		os.WriteFile(cg+"/cgroup.kill", []byte("1"), 0)
		for i := 0; i < 50 && os.Remove(cg) != nil; i++ {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if err != nil {
		cleanup()
		return 0, nil, err
	}
	for f, v := range limits {
		if err := os.WriteFile(filepath.Join(cg, f), []byte(v), 0); err != nil {
			cleanup()
			return 0, nil, fmt.Errorf("write %s: %w", f, err)
		}
	}
	return fd, cleanup, nil
}

func cgroupLimits() (string, error) {
	// pids.max: a shell that keeps forking sleeps must hit the ceiling.
	fd, cleanup, err := limited("pids", map[string]string{"pids.max": "12"})
	if err != nil {
		return "", err
	}
	fork := exec.Command("sh", "-c", "i=0; while [ $i -lt 40 ]; do sleep 5 & i=$((i+1)); done; wait")
	fork.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: fd}
	forkOut, _ := fork.CombinedOutput()
	ev, _ := os.ReadFile("/sys/fs/cgroup/byok8s-probe-pids/pids.events")
	cleanup()
	if !strings.Contains(string(ev), "max") || strings.Contains(string(ev), "max 0") {
		return "", fmt.Errorf("pids.max not enforced: pids.events %q, shell said %q", ev, forkOut)
	}

	// memory.max: allocating past it gets the process OOM-killed.
	fd, cleanup, err = limited("mem", map[string]string{"memory.max": "33554432", "memory.swap.max": "0"})
	if err != nil {
		return "", err
	}
	defer cleanup()
	mem := exec.Command("sh", "-c", "x=$(head -c 67108864 /dev/zero | tr '\\0' a); echo ${#x}")
	mem.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: fd}
	merr := mem.Run()
	mev, _ := os.ReadFile("/sys/fs/cgroup/byok8s-probe-mem/memory.events")
	if strings.Contains(string(mev), "oom_kill 0") {
		return "", fmt.Errorf("memory.max not enforced (%v): %s", merr, mev)
	}
	return "pids.max refused forks, memory.max OOM-killed at 32Mi", nil
}

// capget/capset header, version 3.
type capHeader struct {
	version uint32
	pid     int32
}
type capData struct{ effective, permitted, inheritable uint32 }

func childCaps() (string, error) {
	if err := private(); err != nil {
		return "", err
	}
	runtime.LockOSThread()
	hdr := capHeader{version: 0x20080522}
	var data [2]capData
	if _, _, e := syscall.RawSyscall(syscall.SYS_CAPGET, uintptr(unsafe.Pointer(&hdr)), uintptr(unsafe.Pointer(&data[0])), 0); e != 0 {
		return "", e
	}
	const capSysAdmin = 21
	data[0].effective &^= 1 << capSysAdmin
	if _, _, e := syscall.RawSyscall(syscall.SYS_CAPSET, uintptr(unsafe.Pointer(&hdr)), uintptr(unsafe.Pointer(&data[0])), 0); e != 0 {
		return "", fmt.Errorf("capset: %w", e)
	}
	dir := filepath.Join(work, "caps")
	os.MkdirAll(dir, 0o755)
	err := syscall.Mount("tmpfs", dir, "tmpfs", 0, "")
	if err == nil {
		return "", errors.New("mount still allowed without CAP_SYS_ADMIN")
	}
	return "without CAP_SYS_ADMIN, mount fails: " + err.Error(), nil
}

type sockFilter struct {
	code uint16
	jt   uint8
	jf   uint8
	k    uint32
}
type sockFprog struct {
	len    uint16
	filter *sockFilter
}

func childSeccomp() (string, error) {
	runtime.LockOSThread()
	prog := []sockFilter{
		{0x20, 0, 0, 0}, // ld [nr]
		{0x15, 0, 1, uint32(syscall.SYS_GETPPID)},        // jeq getppid
		{0x06, 0, 0, 0x00050000 | uint32(syscall.EPERM)}, // ret errno(EPERM)
		{0x06, 0, 0, 0x7fff0000},                         // ret allow
	}
	fp := sockFprog{len: uint16(len(prog)), filter: &prog[0]}
	if err := prctl(syscall.PR_SET_NO_NEW_PRIVS, 1); err != nil {
		return "", err
	}
	const seccompSetModeFilter, tsync = 1, 1
	if _, _, e := syscall.RawSyscall(syscall.SYS_SECCOMP, seccompSetModeFilter, tsync, uintptr(unsafe.Pointer(&fp))); e != 0 {
		return "", fmt.Errorf("seccomp: %w", e)
	}
	_, _, e := syscall.RawSyscall(syscall.SYS_GETPPID, 0, 0, 0)
	if e != syscall.EPERM {
		return "", fmt.Errorf("getppid returned errno %v under a filter denying it", e)
	}
	return "a filter denying getppid made it return EPERM", nil
}

func prctl(op, arg uintptr) error {
	if _, _, e := syscall.RawSyscall(syscall.SYS_PRCTL, op, arg, 0); e != 0 {
		return e
	}
	return nil
}

func childLandlock() (string, error) {
	const create, restrict = 444, 446
	abi, _, e := syscall.RawSyscall(create, 0, 0, 1)
	if e != 0 {
		return "", fmt.Errorf("landlock_create_ruleset: %w", e)
	}
	runtime.LockOSThread()
	attr := struct{ handledFS uint64 }{1 << 8} // LANDLOCK_ACCESS_FS_MAKE_REG
	fd, _, e := syscall.RawSyscall(create, uintptr(unsafe.Pointer(&attr)), unsafe.Sizeof(attr), 0)
	if e != 0 {
		return "", fmt.Errorf("create ruleset: %w", e)
	}
	if err := prctl(syscall.PR_SET_NO_NEW_PRIVS, 1); err != nil {
		return "", err
	}
	if _, _, e := syscall.RawSyscall(restrict, fd, 0, 0); e != 0 {
		return "", fmt.Errorf("restrict self: %w", e)
	}
	f, err := os.Create(filepath.Join(work, "landlocked"))
	if err == nil {
		f.Close()
		return "", errors.New("file creation still allowed")
	}
	return fmt.Sprintf("ABI v%d, file creation denied: %v", abi, err), nil
}

func kvm() (string, error) {
	if _, err := os.Stat("/dev/kvm"); err != nil {
		return "", err
	}
	return "/dev/kvm present", nil
}

func sh(script string) (string, error) {
	out, err := exec.Command("sh", "-ec", script).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%v: %s", err, out)
	}
	return string(out), nil
}

func childVeth() (string, error) {
	_, err := sh(`
ip link set lo up
ip link add bko-a type veth peer name bko-b
ip link add bko-br type bridge
ip link set bko-b master bko-br
ip addr add 10.244.250.1/24 dev bko-a
ip link set bko-a up; ip link set bko-b up; ip link set bko-br up
ip route add 10.245.0.0/16 via 10.244.250.2 dev bko-a onlink
ip route get 10.245.1.1 | grep -q bko-a
`)
	if err != nil {
		return "", err
	}
	return "veth pair, bridge, address and route in a fresh netns", nil
}

func childVxlan() (string, error) {
	_, err := sh(`
ip link add bko-dummy type dummy; ip addr add 10.99.0.1/24 dev bko-dummy; ip link set bko-dummy up
ip link add bko-vx type vxlan id 42 dstport 4789 local 10.99.0.1 dev bko-dummy
ip link set bko-vx up
bridge fdb append 00:00:00:00:00:00 dev bko-vx dst 10.99.0.2
`)
	if err != nil {
		return "", err
	}
	return "vxlan device with an fdb entry", nil
}

func childNft() (string, error) {
	_, err := sh(`
nft add table inet bko
nft add chain inet bko pre '{ type nat hook prerouting priority -100; }'
nft add map inet bko svc '{ type ipv4_addr . inet_service : verdict; }'
nft add chain inet bko ep
nft add rule inet bko ep 'numgen random mod 2 vmap { 0 : accept, 1 : accept }'
nft add rule inet bko pre ip daddr . tcp dport vmap @svc
nft add rule inet bko pre tcp dport 80 dnat ip to 10.0.0.2:8080
nft list table inet bko >/dev/null
`)
	if err != nil {
		return "", err
	}
	v, _ := sh("nft --version")
	return strings.TrimSpace(v) + ": nat chain, maps, numgen, dnat", nil
}

func loopMount() (string, error) {
	img := filepath.Join(work, "disk.img")
	if err := os.WriteFile(img, nil, 0o644); err != nil {
		return "", err
	}
	if err := os.Truncate(img, 64<<20); err != nil {
		return "", err
	}
	return runChild("loop", &syscall.SysProcAttr{Cloneflags: syscall.CLONE_NEWNS})
}

func childLoop() (string, error) {
	if err := private(); err != nil {
		return "", err
	}
	dev, err := sh("losetup --find --show " + filepath.Join(work, "disk.img"))
	if err != nil {
		return "", err
	}
	dev = strings.TrimSpace(dev)
	defer exec.Command("losetup", "-d", dev).Run()
	if _, err := sh("mkfs.ext4 -q " + dev); err != nil {
		return "", err
	}
	dir := filepath.Join(work, "vol")
	os.MkdirAll(dir, 0o755)
	if err := syscall.Mount(dev, dir, "ext4", 0, ""); err != nil {
		return "", fmt.Errorf("mount %s: %w", dev, err)
	}
	defer syscall.Unmount(dir, 0)
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644); err != nil {
		return "", err
	}
	return dev + " formatted ext4, mounted, written", nil
}

// secondContainerd starts a containerd of its own beside the node's, on its
// own socket, root and state, and runs a pod sandbox and a container through
// its CRI — what a learner's kubelet would drive without fighting the node's.
func secondContainerd() (string, error) {
	sock := "/run/byok8s-probe/containerd.sock"
	state := "/run/byok8s-probe/state"
	defer os.RemoveAll("/run/byok8s-probe")
	cfg := fmt.Sprintf(`version = 2
root = %q
state = %q
[grpc]
  address = %q
[ttrpc]
  address = %q
[plugins."io.containerd.grpc.v1.cri"]
  sandbox_image = "registry.k8s.io/pause:3.10"
  stream_server_port = "0"
[plugins."io.containerd.grpc.v1.cri".containerd]
  snapshotter = "overlayfs"
`, work+"/ctrd", state, sock, sock+".ttrpc")
	os.WriteFile(work+"/config.toml", []byte(cfg), 0o644)

	ctrd := exec.Command("containerd", "--config", work+"/config.toml")
	ctrd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var log strings.Builder
	ctrd.Stdout, ctrd.Stderr = &log, &log
	if err := ctrd.Start(); err != nil {
		return "", err
	}
	defer func() {
		syscall.Kill(-ctrd.Process.Pid, syscall.SIGKILL)
		ctrd.Wait()
		// Shims and their mounts outlive containerd; clear both.
		exec.Command("sh", "-c", `pkill -9 -f "containerd-shim.*byok8s-probe" ; `+
			`for m in $(awk '$2 ~ /byok8s-probe/ {print $2}' /proc/self/mounts | sort -r); do umount -l "$m"; done`).Run()
	}()
	crictl := "crictl --runtime-endpoint unix://" + sock + " --image-endpoint unix://" + sock + " "
	for i := 0; ; i++ {
		if _, err := sh(crictl + "version"); err == nil {
			break
		} else if i > 50 {
			return "", fmt.Errorf("never answered: %v\n%s", err, log.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
	// The pause image is preloaded in the node's containerd; the second one
	// gets a copy rather than reaching for the internet.
	if _, err := sh(`ctr -n k8s.io images export ` + work + `/pause.tar registry.k8s.io/pause:3.10 &&
ctr --address ` + sock + ` -n k8s.io images import ` + work + `/pause.tar`); err != nil {
		return "", fmt.Errorf("copy pause image: %w", err)
	}
	os.WriteFile(work+"/pod.json", []byte(`{"metadata":{"name":"probe","namespace":"byok8s","uid":"probe-1"},
"linux":{"cgroup_parent":"/byok8s-probe-pods","security_context":{"namespace_options":{"network":2}}},"log_directory":"`+work+`/logs"}`), 0o644)
	os.WriteFile(work+"/ctr.json", []byte(`{"metadata":{"name":"c"},"image":{"image":"registry.k8s.io/pause:3.10"},"log_path":"c.log"}`), 0o644)
	defer os.Remove("/sys/fs/cgroup/byok8s-probe-pods")
	pod, err := sh(crictl + "runp " + work + "/pod.json")
	if err != nil {
		return "", fmt.Errorf("runp: %w", err)
	}
	pod = strings.TrimSpace(pod)
	defer exec.Command("sh", "-c", crictl+"stopp "+pod+"; "+crictl+"rmp -f "+pod).Run()
	c, err := sh(crictl + "create " + pod + " " + work + "/ctr.json " + work + "/pod.json")
	if err != nil {
		return "", fmt.Errorf("create: %w", err)
	}
	c = strings.TrimSpace(c)
	if _, err := sh(crictl + "start " + c); err != nil {
		return "", fmt.Errorf("start: %w", err)
	}
	st, err := sh(crictl + "inspect -o go-template --template '{{.status.state}}' " + c)
	if err != nil || !strings.Contains(st, "RUNNING") {
		return "", fmt.Errorf("container state %q: %v", st, err)
	}
	v, _ := sh(crictl + "version | awk '/RuntimeVersion/{print $2}'")
	return "containerd " + strings.TrimSpace(v) + " on its own socket ran a sandbox and a container", nil
}
