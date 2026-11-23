//go:build linux

// A container runtime: pull an image from a registry, turn its layers into a
// root filesystem, and run its process in namespaces of its own.
//
//	app auth REF
//	app manifest REF
//	app pull [--root DIR] REF
//	app unpack [--root DIR] REF DEST
//	app mount [--root DIR] REF ID
//	app umount [--root DIR] ID
//	app config [--root DIR] REF [ARG…]
//	app run [--root DIR] REF [ARG…]
package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"syscall"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: app auth|manifest|pull …")
		os.Exit(2)
	}
	var err error
	switch cmd, args := os.Args[1], os.Args[2:]; cmd {
	case "auth":
		err = cmdAuth(args)
	case "manifest":
		err = cmdManifest(args)
	case "pull":
		err = cmdPull(args)
	case "unpack":
		err = cmdUnpack(args)
	case "mount":
		err = cmdMount(args)
	case "umount":
		err = cmdUmount(args)
	case "config":
		err = cmdConfig(args)
	case "run":
		err = cmdRun(args)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func cmdAuth(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: auth REF")
	}
	ref, err := parseRef(args[0])
	if err != nil {
		return err
	}
	c, err := connect(ref)
	if err != nil {
		return err
	}
	fmt.Println(c.token)
	return nil
}

func cmdManifest(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: manifest REF")
	}
	ref, err := parseRef(args[0])
	if err != nil {
		return err
	}
	c, err := connect(ref)
	if err != nil {
		return err
	}
	m, digest, err := c.resolve()
	if err != nil {
		return err
	}
	fmt.Println("manifest", digest)
	fmt.Println("config", m.Config.Digest)
	for _, l := range m.Layers {
		fmt.Println("layer", l.Digest)
	}
	return nil
}

// defaultRoot is where the runtime keeps images and containers.
const defaultRoot = "/var/lib/byok8s-runtime"

func cmdPull(args []string) error {
	fs := flag.NewFlagSet("pull", flag.ContinueOnError)
	root := fs.String("root", defaultRoot, "where images and containers live")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: pull [--root DIR] REF")
	}
	_, err := pull(*root, fs.Arg(0))
	return err
}

// pull makes sure the image's config and layers are in the store, and
// returns its manifest.
func pull(root, name string) (manifest, error) {
	ref, err := parseRef(name)
	if err != nil {
		return manifest{}, err
	}
	c, err := connect(ref)
	if err != nil {
		return manifest{}, err
	}
	m, _, err := c.resolve()
	if err != nil {
		return m, err
	}
	for _, d := range append([]descriptor{m.Config}, m.Layers...) {
		if err := c.fetchBlob(root, d.Digest); err != nil {
			return m, err
		}
	}
	return m, nil
}

func cmdUnpack(args []string) error {
	fs := flag.NewFlagSet("unpack", flag.ContinueOnError)
	root := fs.String("root", defaultRoot, "where images and containers live")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: unpack [--root DIR] REF DEST")
	}
	m, err := pull(*root, fs.Arg(0))
	if err != nil {
		return err
	}
	dest := fs.Arg(1)
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}
	for _, l := range m.Layers {
		if err := applyLayer(blobPath(*root, l.Digest), dest, false); err != nil {
			return fmt.Errorf("layer %s: %w", l.Digest, err)
		}
	}
	return nil
}

func cmdMount(args []string) error {
	fs := flag.NewFlagSet("mount", flag.ContinueOnError)
	root := fs.String("root", defaultRoot, "where images and containers live")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: mount [--root DIR] REF ID")
	}
	m, err := pull(*root, fs.Arg(0))
	if err != nil {
		return err
	}
	rootfs, err := mountRootfs(*root, m, fs.Arg(1))
	if err != nil {
		return err
	}
	fmt.Println(rootfs)
	return nil
}

func cmdUmount(args []string) error {
	fs := flag.NewFlagSet("umount", flag.ContinueOnError)
	root := fs.String("root", defaultRoot, "where images and containers live")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: umount [--root DIR] ID")
	}
	return removeContainer(*root, fs.Arg(0))
}

func cmdConfig(args []string) error {
	fs := flag.NewFlagSet("config", flag.ContinueOnError)
	root := fs.String("root", defaultRoot, "where images and containers live")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return errors.New("usage: config [--root DIR] REF [ARG…]")
	}
	_, p, err := imageProcess(*root, fs.Arg(0), fs.Args()[1:])
	if err != nil {
		return err
	}
	a, _ := json.Marshal(p.args)
	fmt.Println("args", string(a))
	fmt.Println("cwd", p.cwd)
	for _, e := range p.env {
		fmt.Println("env", e)
	}
	return nil
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	root := fs.String("root", defaultRoot, "where images and containers live")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return errors.New("usage: run [--root DIR] REF [ARG…]")
	}
	m, p, err := imageProcess(*root, fs.Arg(0), fs.Args()[1:])
	if err != nil {
		return err
	}
	id := make([]byte, 6)
	if _, err := rand.Read(id); err != nil {
		return err
	}
	code, err := runContainer(*root, m, hex.EncodeToString(id), p)
	if err != nil {
		return err
	}
	os.Exit(code)
	return nil
}

// runContainer runs the process in a fresh rootfs for the image, waits for it,
// removes the container, and returns the process's exit code.
func runContainer(root string, m manifest, id string, p process) (int, error) {
	defer removeContainer(root, id)
	rootfs, err := mountRootfs(root, m, id)
	if err != nil {
		return 0, err
	}
	// Path, Dir and every lookup the process makes resolve inside the chroot.
	cmd := &exec.Cmd{
		Path:        p.args[0],
		Args:        p.args,
		Env:         p.env,
		Dir:         p.cwd,
		Stdin:       os.Stdin,
		Stdout:      os.Stdout,
		Stderr:      os.Stderr,
		SysProcAttr: &syscall.SysProcAttr{Chroot: rootfs},
	}
	err = cmd.Run()
	if exit, ok := err.(*exec.ExitError); ok {
		return exit.ExitCode(), nil
	}
	return 0, err
}

// process is what a container runs: its argv, working directory and
// environment.
type process struct {
	args []string
	cwd  string
	env  []string
}

// defaultPath is the PATH a container gets when its image sets none, the
// same default Docker and containerd use.
const defaultPath = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

// imageProcess pulls the image and reads from its config the process it
// means. Arguments given on the command line replace the image's Cmd and
// keep its Entrypoint, as `docker run IMAGE ARG…` does.
func imageProcess(root, ref string, args []string) (manifest, process, error) {
	m, err := pull(root, ref)
	if err != nil {
		return m, process{}, err
	}
	data, err := os.ReadFile(blobPath(root, m.Config.Digest))
	if err != nil {
		return m, process{}, err
	}
	var img struct {
		Config struct {
			Entrypoint []string `json:"Entrypoint"`
			Cmd        []string `json:"Cmd"`
			Env        []string `json:"Env"`
			WorkingDir string   `json:"WorkingDir"`
		} `json:"config"`
	}
	if err := json.Unmarshal(data, &img); err != nil {
		return m, process{}, fmt.Errorf("image config: %w", err)
	}
	c := img.Config
	cmd := c.Cmd
	if len(args) > 0 {
		cmd = args
	}
	p := process{args: append(slices.Clone(c.Entrypoint), cmd...), cwd: c.WorkingDir, env: c.Env}
	if len(p.args) == 0 {
		return m, p, errors.New("the image names no command and none was given")
	}
	if p.cwd == "" {
		p.cwd = "/"
	}
	if !slices.ContainsFunc(p.env, func(e string) bool { return strings.HasPrefix(e, "PATH=") }) {
		p.env = append(p.env, defaultPath)
	}
	return m, p, nil
}

// containerDir holds one container's writable layer, overlay workdir and the
// mount point of its root filesystem.
func containerDir(root, id string) string {
	return filepath.Join(root, "containers", filepath.Base(id))
}

// mountRootfs makes sure each of the image's layers is extracted in its own
// directory, and mounts an overlay of them under the container's directory
// with a writable layer of its own on top. It returns the mount point.
func mountRootfs(root string, m manifest, id string) (string, error) {
	// overlayfs lists lowerdirs top first; the manifest lists layers bottom first.
	lower := make([]string, len(m.Layers))
	for i, l := range m.Layers {
		dir, err := layerDir(root, l.Digest)
		if err != nil {
			return "", fmt.Errorf("layer %s: %w", l.Digest, err)
		}
		lower[len(m.Layers)-1-i] = dir
	}
	c := containerDir(root, id)
	upper, work, rootfs := filepath.Join(c, "upper"), filepath.Join(c, "work"), filepath.Join(c, "rootfs")
	for _, d := range []string{upper, work, rootfs} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return "", err
		}
	}
	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", strings.Join(lower, ":"), upper, work)
	if err := syscall.Mount("overlay", rootfs, "overlay", 0, opts); err != nil {
		return "", fmt.Errorf("mount overlay at %s: %w", rootfs, err)
	}
	return rootfs, nil
}

// removeContainer unmounts a container's rootfs and deletes its directory,
// writable layer included. A container that is not mounted is still removed.
func removeContainer(root, id string) error {
	c := containerDir(root, id)
	if err := syscall.Unmount(filepath.Join(c, "rootfs"), syscall.MNT_DETACH); err != nil && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, syscall.ENOENT) {
		return err
	}
	return os.RemoveAll(c)
}

// layerDir extracts a layer into a directory of its own, once: overlay
// mounts of every container from the image share it read-only. It is built
// beside its final name and renamed into place, so a half-extracted layer is
// never mistaken for a finished one.
func layerDir(root, digest string) (string, error) {
	algo, hex, _ := strings.Cut(digest, ":")
	dir := filepath.Join(root, "layers", algo, hex)
	if _, err := os.Stat(dir); err == nil {
		return dir, nil
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return "", err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(dir), ".partial-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmp)
	if err := applyLayer(blobPath(root, digest), tmp, true); err != nil {
		return "", err
	}
	if err := os.Chmod(tmp, 0o755); err != nil {
		return "", err
	}
	return dir, os.Rename(tmp, dir)
}

// blobPath is where the store keeps a blob: named by its digest, so a blob
// already there is one that was verified when it was written.
func blobPath(root, digest string) string {
	algo, hex, _ := strings.Cut(digest, ":")
	return filepath.Join(root, "blobs", algo, hex)
}

// fetchBlob downloads a blob it does not already have, hashing as it writes,
// and gives it its final name only once the hash matches.
func (c *client) fetchBlob(root, digest string) error {
	path := blobPath(root, digest)
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	if !strings.HasPrefix(digest, "sha256:") {
		return fmt.Errorf("unsupported digest %s", digest)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	resp, err := c.get("/v2/"+c.ref.repo+"/blobs/"+digest, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	tmp, err := os.CreateTemp(filepath.Dir(path), ".partial-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	h := sha256.New()
	_, err = io.Copy(io.MultiWriter(tmp, h), resp.Body)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("blob %s: %w", digest, err)
	}
	if got := "sha256:" + hex.EncodeToString(h.Sum(nil)); got != digest {
		return fmt.Errorf("blob %s: the registry sent content with digest %s", digest, got)
	}
	return os.Rename(tmp.Name(), path)
}

// reference is an image name: registry host, repository, and a tag or digest.
type reference struct {
	registry, repo, tag, digest string
}

func parseRef(s string) (reference, error) {
	host, rest, ok := strings.Cut(s, "/")
	if !ok || rest == "" {
		return reference{}, fmt.Errorf("%q: want HOST[:PORT]/REPO[:TAG|@DIGEST]", s)
	}
	r := reference{registry: host, tag: "latest"}
	if name, digest, ok := strings.Cut(rest, "@"); ok {
		r.repo, r.digest, r.tag = name, digest, ""
	} else if i := strings.LastIndex(rest, ":"); i >= 0 {
		r.repo, r.tag = rest[:i], rest[i+1:]
	} else {
		r.repo = rest
	}
	return r, nil
}

// client talks to one repository of one registry, holding a pull token.
type client struct {
	ref   reference
	base  string
	token string
}

// connect asks the registry what authentication it wants and gets a token
// for pulling ref's repository.
func connect(ref reference) (*client, error) {
	c := &client{ref: ref, base: "http://" + ref.registry}
	resp, err := http.Get(c.base + "/v2/")
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return c, nil
	case http.StatusUnauthorized:
	default:
		return nil, fmt.Errorf("GET /v2/: %s", resp.Status)
	}
	ch := challenge(resp.Header.Get("WWW-Authenticate"))
	if ch["realm"] == "" {
		return nil, fmt.Errorf("the registry wants authentication but names no realm: %q", resp.Header.Get("WWW-Authenticate"))
	}
	q := url.Values{"service": {ch["service"]}, "scope": {"repository:" + ref.repo + ":pull"}}
	req, err := http.NewRequest(http.MethodGet, ch["realm"]+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	user, pass, err := credentials(ref.registry)
	if err != nil {
		return nil, err
	}
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("token for %s: %s %s", ref.repo, resp.Status, body)
	}
	var tok struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		return nil, fmt.Errorf("token response: %w", err)
	}
	c.token = tok.Token
	if c.token == "" {
		c.token = tok.AccessToken
	}
	return c, nil
}

var challengeParam = regexp.MustCompile(`(\w+)="([^"]*)"`)

// challenge reads the parameters of a Bearer WWW-Authenticate header.
func challenge(h string) map[string]string {
	out := map[string]string{}
	if !strings.HasPrefix(h, "Bearer ") {
		return out
	}
	for _, m := range challengeParam.FindAllStringSubmatch(h, -1) {
		out[m[1]] = m[2]
	}
	return out
}

// credentials finds the user and password for host in config.json, under
// $DOCKER_CONFIG or ~/.docker. No file, or no entry, means anonymous.
func credentials(host string) (string, string, error) {
	dir := os.Getenv("DOCKER_CONFIG")
	if dir == "" {
		home, _ := os.UserHomeDir()
		dir = filepath.Join(home, ".docker")
	}
	data, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if errors.Is(err, fs.ErrNotExist) {
		return "", "", nil
	} else if err != nil {
		return "", "", err
	}
	var cfg struct {
		Auths map[string]struct {
			Auth string `json:"auth"`
		} `json:"auths"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return "", "", fmt.Errorf("%s/config.json: %w", dir, err)
	}
	entry, ok := cfg.Auths[host]
	if !ok {
		return "", "", nil
	}
	raw, err := base64.StdEncoding.DecodeString(entry.Auth)
	if err != nil {
		return "", "", fmt.Errorf("auth for %s: %w", host, err)
	}
	user, pass, _ := strings.Cut(string(raw), ":")
	return user, pass, nil
}

const (
	mediaOCIIndex       = "application/vnd.oci.image.index.v1+json"
	mediaOCIManifest    = "application/vnd.oci.image.manifest.v1+json"
	mediaDockerList     = "application/vnd.docker.distribution.manifest.list.v2+json"
	mediaDockerManifest = "application/vnd.docker.distribution.manifest.v2+json"
)

// descriptor points at content by digest: a manifest, a config, a layer.
type descriptor struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
	Platform  *struct {
		Architecture string `json:"architecture"`
		OS           string `json:"os"`
	} `json:"platform,omitempty"`
}

// manifest is an image manifest or an index of them; which one is in
// MediaType, and only the matching fields are set.
type manifest struct {
	MediaType string       `json:"mediaType"`
	Config    descriptor   `json:"config"`
	Layers    []descriptor `json:"layers"`
	Manifests []descriptor `json:"manifests"`
}

// get fetches a registry path with the client's token.
func (c *client) get(path, accept string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, c.base+path, nil)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("GET %s: %s %s", path, resp.Status, body)
	}
	return resp, nil
}

// resolve returns the image manifest the reference means on this platform,
// and its digest, following an index to the entry for linux/GOARCH.
func (c *client) resolve() (manifest, string, error) {
	ref := c.ref.digest
	if ref == "" {
		ref = c.ref.tag
	}
	m, digest, err := c.fetchManifest(ref)
	if err != nil {
		return m, "", err
	}
	if m.MediaType != mediaOCIIndex && m.MediaType != mediaDockerList {
		return m, digest, nil
	}
	for _, d := range m.Manifests {
		if d.Platform != nil && d.Platform.OS == "linux" && d.Platform.Architecture == runtime.GOARCH {
			return c.fetchManifest(d.Digest)
		}
	}
	return m, "", fmt.Errorf("%s has no image for linux/%s", c.ref.repo, runtime.GOARCH)
}

// fetchManifest gets a manifest by tag or digest. A manifest asked for by
// digest must hash to it, or it is not the manifest that was asked for.
func (c *client) fetchManifest(ref string) (manifest, string, error) {
	var m manifest
	accept := strings.Join([]string{mediaOCIIndex, mediaOCIManifest, mediaDockerList, mediaDockerManifest}, ", ")
	resp, err := c.get("/v2/"+c.ref.repo+"/manifests/"+ref, accept)
	if err != nil {
		return m, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return m, "", err
	}
	digest := digestOf(body)
	if strings.HasPrefix(ref, "sha256:") && digest != ref {
		return m, "", fmt.Errorf("manifest %s: the registry sent content with digest %s", ref, digest)
	}
	if err := json.Unmarshal(body, &m); err != nil {
		return m, "", fmt.Errorf("manifest %s: %w", ref, err)
	}
	if m.MediaType == "" {
		m.MediaType = resp.Header.Get("Content-Type")
	}
	return m, digest, nil
}

func digestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// The two whiteout forms a layer tarball uses: .wh.NAME deletes NAME from the
// layers below, and .wh..wh..opq hides everything they put in its directory.
const (
	whiteoutPrefix = ".wh."
	whiteoutOpaque = ".wh..wh..opq"
)

// applyLayer extracts a gzipped layer tarball onto dest. Names are confined to
// dest: each is cleaned against "/" before it is joined, and no entry is
// written through a symlink an earlier entry planted.
//
// Applied onto the layers below it, a whiteout deletes; extracted alone for
// overlayfs, it is recorded the way overlayfs reads one instead.
func applyLayer(blob, dest string, forOverlay bool) error {
	f, err := os.Open(blob)
	if err != nil {
		return err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		name := filepath.Clean("/" + hdr.Name)
		if name == "/" {
			continue
		}
		if err := noSymlinkParents(dest, filepath.Dir(name)); err != nil {
			return err
		}
		target := filepath.Join(dest, name)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		base := filepath.Base(name)
		if base == whiteoutOpaque {
			hide := opaque
			if forOverlay {
				hide = overlayOpaque
			}
			if err := hide(filepath.Dir(target)); err != nil {
				return err
			}
			continue
		}
		if hidden, ok := strings.CutPrefix(base, whiteoutPrefix); ok {
			hide := whiteout
			if forOverlay {
				hide = overlayWhiteout
			}
			if err := hide(filepath.Join(filepath.Dir(target), hidden)); err != nil {
				return err
			}
			continue
		}
		if err := extract(tr, hdr, dest, target); err != nil {
			return fmt.Errorf("%s: %w", hdr.Name, err)
		}
	}
}

// noSymlinkParents refuses a path whose directories inside dest include a
// symlink, which could point anywhere on the node.
func noSymlinkParents(dest, dir string) error {
	p := dest
	for _, part := range strings.Split(strings.Trim(dir, "/"), "/") {
		if part == "" {
			continue
		}
		p = filepath.Join(p, part)
		fi, err := os.Lstat(p)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		} else if err != nil {
			return err
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%s: a layer entry would be written through a symlink", p)
		}
	}
	return nil
}

func opaque(dir string) error {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

func whiteout(path string) error {
	return os.RemoveAll(path)
}

// overlayWhiteout is overlayfs's form of a deleted file: a character device
// numbered 0/0 in its place.
func overlayWhiteout(path string) error {
	return syscall.Mknod(path, syscall.S_IFCHR, 0)
}

// overlayOpaque marks a directory whose lower-layer contents overlayfs hides.
func overlayOpaque(dir string) error {
	return syscall.Setxattr(dir, "trusted.overlay.opaque", []byte("y"), 0)
}

// extract writes one tar entry at target, replacing whatever a lower layer
// left there, then sets its owner and mode as the tarball records them.
func extract(tr *tar.Reader, hdr *tar.Header, dest, target string) error {
	if fi, err := os.Lstat(target); err == nil && !(fi.IsDir() && hdr.Typeflag == tar.TypeDir) {
		if err := os.RemoveAll(target); err != nil {
			return err
		}
	}
	switch hdr.Typeflag {
	case tar.TypeDir:
		if err := os.MkdirAll(target, 0o755); err != nil {
			return err
		}
	case tar.TypeReg:
		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		_, err = io.Copy(f, tr)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
	case tar.TypeSymlink:
		return os.Symlink(hdr.Linkname, target)
	case tar.TypeLink:
		return os.Link(filepath.Join(dest, filepath.Clean("/"+hdr.Linkname)), target)
	default:
		return nil
	}
	if err := os.Lchown(target, hdr.Uid, hdr.Gid); err != nil {
		return err
	}
	return syscall.Chmod(target, uint32(hdr.Mode&0o7777))
}
