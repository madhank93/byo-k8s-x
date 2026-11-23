// A container runtime: pull an image from a registry, turn its layers into a
// root filesystem, and run its process in namespaces of its own.
//
//	app auth REF
//	app manifest REF
//	app pull [--root DIR] REF
package main

import (
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
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
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
