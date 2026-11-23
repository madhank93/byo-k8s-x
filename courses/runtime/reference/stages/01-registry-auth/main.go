//go:build linux

// A container runtime: pull an image from a registry, turn its layers into a
// root filesystem, and run its process in namespaces of its own.
//
//	app auth REF
package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: app auth REF")
		os.Exit(2)
	}
	var err error
	switch cmd, args := os.Args[1], os.Args[2:]; cmd {
	case "auth":
		err = cmdAuth(args)
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
