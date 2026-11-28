// Package cni holds the assertions for the "Build your own CNI plugin"
// course — one function per stage, registered by slug.
//
// The learner's program is a CNI plugin, built for Linux and installed in the
// kind workers. The harness plays the container runtime: it makes network
// namespaces, invokes the plugin with the CNI environment and a config on
// stdin, and then judges what the plugin did by looking at the node — links,
// addresses, routes, nftables — and by sending traffic through it.
package cni

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/madhank93/byo-k8s-x/internal/cluster"
	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// Stage is one gradable step, in the shape cmd/tester dispatches on.
type Stage = stages.Stage

var (
	stageRegistry = map[string]Stage{}
	stageNumber   = map[string]int{}
)

func register(n int, slug string, run func(*work) error) {
	stageNumber[slug] = n
	stageRegistry[slug] = Stage{Slug: slug, Run: func(ctx context.Context, env *kube.Env, bin string) error {
		w, err := newWork(ctx, env, slug, bin)
		if err != nil {
			return err
		}
		defer w.close()
		return run(w)
	}}
}

// Lookup returns the stage with this slug.
func Lookup(slug string) (Stage, bool) {
	s, ok := stageRegistry[slug]
	return s, ok
}

// The plugin runs in the first two workers: one node for everything that
// stays on a node, both for traffic between nodes.
const (
	node1 = runner.Node(cluster.Name + "-worker")
	node2 = runner.Node(cluster.Name + "-worker2")
)

// baseDir holds one directory per stage on each node, for IPAM state and
// fixtures.
const baseDir = "/var/lib/byok8s-cni"

// Every netns and link the harness creates, and every link name it hands the
// plugin, starts with prefix, so a sweep can find what a killed run left.
const prefix = "bk8s"

// The course's addresses: 10.217.N.0/24 for stage N. Outside kind's pod
// range, so nothing here collides with a real pod.
const podRange = "10.217.0.0/16"

// harnessTables keeps kind from masquerading traffic between the course's
// addresses, which it does to anything leaving for outside its own pod
// range; containers would see every peer as the gateway. A no-op SNAT in an
// earlier nat chain claims the connection's binding first, and conntrack
// stays on so forwarded ports can still be un-NATed on the way back.
var harnessTables = `table ip byok8s-cni-harness {
  chain post {
    type nat hook postrouting priority srcnat - 10;
    ip saddr ` + podRange + ` ip daddr ` + podRange + ` snat to ip saddr
  }
}
`

// serve accepts one connection, sends a token, and prints the peer address.
const serveScript = `import socket,sys
s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind(("0.0.0.0",int(sys.argv[1]))); s.listen(8); s.settimeout(float(sys.argv[2]))
c,a=s.accept(); c.sendall(sys.argv[3].encode()); print(a[0]); c.close()
`

// dial connects until a deadline and prints what the server sent.
const dialScript = `import socket,sys,time
end=time.time()+float(sys.argv[3]); err=None
while time.time()<end:
  try:
    c=socket.create_connection((sys.argv[1],int(sys.argv[2])),timeout=1)
    print(c.recv(100).decode()); sys.exit(0)
  except OSError as e:
    err=e; time.sleep(0.1)
print(err, file=sys.stderr); sys.exit(1)
`

type work struct {
	ctx   context.Context
	env   *kube.Env
	slug  string
	n     int
	dir   string
	token string
}

func newWork(ctx context.Context, env *kube.Env, slug, bin string) (*work, error) {
	w := &work{ctx: ctx, env: env, slug: slug, n: stageNumber[slug], dir: baseDir + "/" + slug, token: randHex(4)}
	for _, n := range []runner.Node{node1, node2} {
		if err := n.Install(ctx, bin); err != nil {
			return nil, err
		}
	}
	if err := w.clean(); err != nil {
		return nil, err
	}
	// One exec per node: under load each docker exec costs seconds.
	setup := fmt.Sprintf("mkdir -p %[1]s/harness %[2]s\n", baseDir, w.dir)
	for path, body := range map[string]string{
		baseDir + "/harness/serve.py":   serveScript,
		baseDir + "/harness/dial.py":    dialScript,
		baseDir + "/harness/tables.nft": harnessTables,
	} {
		setup += fmt.Sprintf("cat > %s <<'BK8S_EOF'\n%sBK8S_EOF\n", path, body)
	}
	setup += "nft -f " + baseDir + "/harness/tables.nft\n"
	for _, n := range []runner.Node{node1, node2} {
		if _, err := w.sh(n, setup); err != nil {
			return nil, err
		}
	}
	return w, nil
}

func (w *work) close() { w.clean() }

// clean removes everything a run leaves in both nodes: processes in the
// course's namespaces, the namespaces, links, routes into the course's
// range, nftables tables named byok8s-cni*, the stage's directory, and a plugin
// left installed for the kubelet.
func (w *work) clean() error {
	script := fmt.Sprintf(`
rm -f /etc/cni/net.d/00-byok8s.conflist /opt/cni/bin/byok8s
for ns in $(ip netns list | awk '{print $1}' | grep '^%[1]s'); do
  ip netns pids "$ns" | xargs -r kill -9
  ip netns del "$ns"
done
sleep 0.2
for l in $(ip -o link show | awk -F': ' '{print $2}' | cut -d@ -f1 | grep '^%[1]s'); do ip link del "$l" 2>/dev/null; done
ip route show | grep '^10\.217\.' | while read -r r; do ip route del $r 2>/dev/null; done
for t in $(nft list tables | awk '$3 ~ /^byok8s-cni/ {print $2 "/" $3}'); do nft delete table "${t%%/*}" "${t#*/}"; done
rm -rf %[2]s
true`, prefix, w.dir)
	for _, n := range []runner.Node{node1, node2} {
		if err := n.Sweep(w.ctx); err != nil {
			return err
		}
		if _, err := w.sh(n, script); err != nil {
			return err
		}
	}
	return nil
}

// sh runs a harness script in a node and returns its stdout, failing on a
// non-zero exit.
func (w *work) sh(n runner.Node, script string) (string, error) {
	res, err := n.Exec(w.ctx, time.Minute, "sh", "-c", script)
	if err != nil {
		return "", err
	}
	if res.ExitCode != 0 {
		return "", fmt.Errorf("in node %s, %q failed: %s", n, script, res.Stderr)
	}
	return res.Stdout, nil
}

// subnet is this stage's /24, and ip the address host h in it.
func (w *work) subnet() string        { return fmt.Sprintf("10.217.%d.0/24", w.n) }
func (w *work) ip(h int) string       { return fmt.Sprintf("10.217.%d.%d", w.n, h) }
func (w *work) bridge() string        { return fmt.Sprintf("%sbr%d", prefix, w.n) }
func (w *work) ipamDir() string       { return w.dir + "/ipam" }
func (w *work) id(name string) string { return prefix + "-" + w.token + "-" + name }

// netns makes a network namespace in a node and returns its name and path.
func (w *work) netns(n runner.Node, name string) (string, string, error) {
	ns := fmt.Sprintf("%s-%d-%s", prefix, w.n, name)
	if _, err := w.sh(n, "ip netns add "+ns); err != nil {
		return "", "", err
	}
	return ns, "/run/netns/" + ns, nil
}

// conf is a plugin config for this stage with fields added.
func (w *work) conf(fields map[string]any) map[string]any {
	c := map[string]any{"cniVersion": "1.0.0", "name": "byok8s-" + w.slug, "type": "byok8s"}
	for k, v := range fields {
		c[k] = v
	}
	return c
}

// pool is an ipam section allocating from cidr, with state in the stage's
// directory and a default route.
func (w *work) pool(cidr string) map[string]any {
	return map[string]any{
		"type":    "pool",
		"subnet":  cidr,
		"dataDir": w.ipamDir(),
		"routes":  []any{map[string]any{"dst": "0.0.0.0/0"}},
	}
}

// call is one invocation of the plugin, as a runtime would make it.
type call struct {
	node    runner.Node
	command string
	id      string
	netns   string
	ifname  string
	args    string
	path    string
}

func (c call) environ() []string {
	var env []string
	for k, v := range map[string]string{
		"CNI_COMMAND": c.command, "CNI_CONTAINERID": c.id, "CNI_NETNS": c.netns,
		"CNI_IFNAME": c.ifname, "CNI_ARGS": c.args, "CNI_PATH": c.path,
	} {
		if v != "" {
			env = append(env, k+"="+v)
		}
	}
	if c.path == "" {
		env = append(env, "CNI_PATH=/opt/cni/bin")
	}
	return env
}

// invoke runs the plugin with conf on stdin; conf may be raw text.
func (w *work) invoke(c call, conf any, args ...string) (*runner.Result, error) {
	n := c.node
	if n == "" {
		n = node1
	}
	stdin, ok := conf.(string)
	if !ok {
		b, err := json.Marshal(conf)
		if err != nil {
			return nil, err
		}
		stdin = string(b)
	}
	return n.InvokeStdin(w.ctx, c.environ(), stdin, time.Minute, args...)
}

// result is the part of a CNI result the stages read.
type result struct {
	CNIVersion string `json:"cniVersion"`
	Interfaces []struct {
		Name    string `json:"name"`
		Mac     string `json:"mac"`
		Mtu     int    `json:"mtu"`
		Sandbox string `json:"sandbox"`
	} `json:"interfaces"`
	IPs []struct {
		Address   string `json:"address"`
		Gateway   string `json:"gateway"`
		Interface *int   `json:"interface"`
	} `json:"ips"`
	Routes []struct {
		Dst string `json:"dst"`
		GW  string `json:"gw"`
	} `json:"routes"`
	DNS map[string]any `json:"dns"`
	raw string
}

// host is the name of the result's host-side interface: the one with no
// sandbox.
func (r *result) host() string {
	for _, i := range r.Interfaces {
		if i.Sandbox == "" {
			return i.Name
		}
	}
	return ""
}

// addr is the first address in the result, without its prefix length.
func (r *result) addr() string {
	if len(r.IPs) == 0 {
		return ""
	}
	return strings.SplitN(r.IPs[0].Address, "/", 2)[0]
}

// ok runs a call that must succeed and parses the result it prints.
func (w *work) ok(c call, conf any, what string) (*result, error) {
	res, err := w.invoke(c, conf)
	if err != nil {
		return nil, err
	}
	if err := wantExit(res, 0); err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	if c.command == "DEL" || c.command == "CHECK" {
		return nil, nil
	}
	return parseResult(res.Stdout, what)
}

func parseResult(out, what string) (*result, error) {
	r := &result{raw: strings.TrimSpace(out)}
	if err := json.Unmarshal([]byte(r.raw), r); err != nil {
		return nil, fmt.Errorf("%s: stdout must be one JSON result, got %q: %v", what, out, err)
	}
	return r, nil
}

// add is ADD of container name, in a new netns on node n, with ifname eth0.
func (w *work) add(n runner.Node, name string, conf any) (*result, string, error) {
	ns, path, err := w.netns(n, name)
	if err != nil {
		return nil, "", err
	}
	r, err := w.ok(call{node: n, command: "ADD", id: w.id(name), netns: path, ifname: "eth0"}, conf, "ADD of "+name)
	return r, ns, err
}

// cniErr is the error a plugin prints on stdout when it fails.
type cniErr struct {
	CNIVersion string `json:"cniVersion"`
	Code       int    `json:"code"`
	Msg        string `json:"msg"`
}

// wantCNIError is a failure reported the CNI way: a non-zero exit and an
// error object on stdout. code 0 accepts any code.
func wantCNIError(res *runner.Result, code int, why string) error {
	var e cniErr
	if res.ExitCode == 0 || json.Unmarshal([]byte(strings.TrimSpace(res.Stdout)), &e) != nil || e.Code == 0 || e.Msg == "" {
		return fmt.Errorf("%s, so expected a non-zero exit and a CNI error on stdout ({\"cniVersion\":…,\"code\":N,\"msg\":…}), got exit %d\nstdout:\n%s\nstderr:\n%s",
			why, res.ExitCode, tail(res.Stdout), tail(res.Stderr))
	}
	if code != 0 && e.Code != code {
		return fmt.Errorf("%s, so expected error code %d, got %d (%q)", why, code, e.Code, e.Msg)
	}
	return nil
}

func wantExit(res *runner.Result, code int) error {
	if res.ExitCode != code {
		return fmt.Errorf("expected exit %d, got %d\nstdout:\n%s\nstderr:\n%s", code, res.ExitCode, tail(res.Stdout), tail(res.Stderr))
	}
	return nil
}

func tail(s string) string {
	ls := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(ls) > 40 {
		ls = append([]string{"…"}, ls[len(ls)-40:]...)
	}
	return "  " + strings.Join(ls, "\n  ")
}

// in runs a command inside netns ns on node n.
func (w *work) in(n runner.Node, ns, script string) (string, error) {
	return w.sh(n, fmt.Sprintf("ip netns exec %s sh -c %q", ns, script))
}

// end is one side of a connection: a netns on a node, or the node itself
// when ns is empty.
type end struct {
	node runner.Node
	ns   string
}

func (e end) argv(a ...string) []string {
	if e.ns == "" {
		return a
	}
	return append([]string{"ip", "netns", "exec", e.ns}, a...)
}

func (e end) String() string {
	if e.ns == "" {
		return "node " + string(e.node)
	}
	return "netns " + e.ns + " on " + string(e.node)
}

// dial connects from one end to ip:port, where a server listens in the other,
// and returns the source address the server saw.
func (w *work) dial(from, to end, ip string, port int) (string, error) {
	return w.dialTo(from, to, ip, port, port)
}

// dialTo is dial with the server on its own port, for a connection that is
// forwarded from port to srvPort on the way.
func (w *work) dialTo(from, to end, ip string, port, srvPort int) (string, error) {
	tok := "tok-" + randHex(3)
	var (
		srv    *runner.Result
		srvErr error
		wg     sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv, srvErr = to.node.Exec(w.ctx, 20*time.Second, to.argv("python3", baseDir+"/harness/serve.py", fmt.Sprint(srvPort), "8", tok)...)
	}()
	time.Sleep(300 * time.Millisecond)
	cli, err := from.node.Exec(w.ctx, 20*time.Second, from.argv("python3", baseDir+"/harness/dial.py", ip, fmt.Sprint(port), "5")...)
	wg.Wait()
	if err != nil {
		return "", err
	}
	if srvErr != nil {
		return "", srvErr
	}
	if cli.ExitCode != 0 || strings.TrimSpace(cli.Stdout) != tok {
		return "", fmt.Errorf("from %s, a TCP connection to %s:%d (a server listening in %s) did not get through: %s",
			from, ip, port, to, strings.TrimSpace(cli.Stderr))
	}
	return strings.TrimSpace(srv.Stdout), nil
}

// blocked is dial that must not get through.
func (w *work) blocked(from, to end, ip string, port int, why string) error {
	if _, err := w.dial(from, to, ip, port); err == nil {
		return fmt.Errorf("from %s, a TCP connection to %s:%d got through; %s", from, ip, port, why)
	}
	return nil
}

func port() int {
	n, _ := rand.Int(rand.Reader, big.NewInt(10000))
	return 40000 + int(n.Int64())
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// nodeIP is a node's address on the kind network.
func (w *work) nodeIP(n runner.Node) (string, error) {
	out, err := w.sh(n, `ip -4 -o addr show dev eth0 | awk '{print $4}' | cut -d/ -f1`)
	return strings.TrimSpace(out), err
}

// link is one interface as `ip -j -d link show` reports it.
type link struct {
	Name      string   `json:"ifname"`
	Mac       string   `json:"address"`
	Mtu       int      `json:"mtu"`
	Flags     []string `json:"flags"`
	Master    string   `json:"master"`
	OperState string   `json:"operstate"`
	Info      struct {
		Kind string         `json:"info_kind"`
		Data map[string]any `json:"info_data"`
	} `json:"linkinfo"`
	Stats struct {
		TX struct {
			Packets int `json:"packets"`
		} `json:"tx"`
	} `json:"stats64"`
}

func (l link) up() bool {
	for _, f := range l.Flags {
		if f == "UP" {
			return true
		}
	}
	return false
}

// links lists the interfaces of netns ns (the node's own when empty).
func (w *work) links(n runner.Node, ns string) (map[string]link, error) {
	cmd := "ip -j -d -s link show"
	if ns != "" {
		cmd = "ip -n " + ns + " -j -d -s link show"
	}
	out, err := w.sh(n, cmd)
	if err != nil {
		return nil, err
	}
	var ls []link
	if err := json.Unmarshal([]byte(out), &ls); err != nil {
		return nil, fmt.Errorf("parse ip link output: %w", err)
	}
	m := map[string]link{}
	for _, l := range ls {
		m[l.Name] = l
	}
	return m, nil
}

// addrs lists the addresses on dev in netns ns, as address/prefix.
func (w *work) addrs(n runner.Node, ns, dev string) ([]string, error) {
	cmd := "ip -4 -o addr show dev " + dev
	if ns != "" {
		cmd = "ip -n " + ns + " -4 -o addr show dev " + dev
	}
	out, err := w.sh(n, cmd+" | awk '{print $4}'")
	if err != nil {
		return nil, err
	}
	return strings.Fields(out), nil
}

// routes lists netns ns's main routing table, one route per line.
func (w *work) routes(n runner.Node, ns string) (string, error) {
	return w.sh(n, "ip -n "+ns+" -4 route show")
}
