package apiserver

import (
	"bufio"
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/madhank93/byo-k8s-x/internal/kube"
)

func init() {
	register(Stage{Slug: "kubectl-drives-it", Run: stageKubectlDrivesIt})
}

// stageKubectlDrivesIt hands the real kubectl a kubeconfig pointing at the
// program and grades what kubectl makes of it: get prints the server's own
// columns, apply works client- and server-side, edit and scale land, a watch
// prints each change as a line, and a misspelt field in a manifest is refused
// because kubectl left checking it to the server.
func stageKubectlDrivesIt(ctx context.Context, _ *kube.Env, bin string) error {
	if _, err := exec.LookPath("kubectl"); err != nil {
		return fmt.Errorf("this stage runs the real kubectl against your server, and there is no kubectl on PATH: install one (mise install provisions the version the course is graded with)")
	}
	dir, err := os.MkdirTemp("", "byok8s-apiserver-*")
	if err != nil {
		return fmt.Errorf("make a directory for the kubeconfig: %w", err)
	}
	defer os.RemoveAll(dir)
	pki, err := certMakePKI(dir)
	if err != nil {
		return err
	}
	srv, cleanup, err := serveTLS(ctx, bin, pki, "-tls-cert-file", pki.certFile, "-tls-private-key-file", pki.keyFile, "-client-ca-file", pki.caFile)
	if err != nil {
		return err
	}
	defer cleanup()
	k, err := newKubectl(dir, srv, pki)
	if err != nil {
		return err
	}
	alice := pki.client(&pki.alice)
	get := func(path string) (map[string]any, error) {
		res, body, err := certSend(ctx, srv, alice, http.MethodGet, path, "", nil)
		if err != nil {
			return nil, err
		}
		if res.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("GET %s answered %d after kubectl said it was written\nthe body was:\n%s", path, res.StatusCode, tail(string(body)))
		}
		return decode(body)
	}
	const settingsPath = "/api/v1/namespaces/default/configmaps/settings"

	// Client-side apply: kubectl computes the patch itself, from the
	// annotation it left on the object last time.
	settings := func(finalizers, data string) string {
		return "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\n  labels: {app: web}\n  finalizers: [" + finalizers + "]\ndata:\n" + data
	}
	out, err := k.run(ctx, settings("example.com/keep", "  color: blue\n  size: large\n"), "apply", "-f", "-")
	if err != nil {
		if strings.Contains(out, "error validating data") {
			return fmt.Errorf("kubectl apply -f failed:\n%s\nkubectl leaves checking field names to the server only once /openapi/v3 shows a write of that kind taking a fieldValidation query parameter: each operation names the kind it serves in x-kubernetes-group-version-kind, and lists its parameters. Without that it looks for /openapi/v2 to check the manifest itself", tail(out))
		}
		return fmt.Errorf("kubectl apply -f of a ConfigMap failed:\n%s", tail(out))
	}
	if !strings.Contains(out, "configmap/settings created") {
		return fmt.Errorf("kubectl apply -f of a new ConfigMap printed:\n%s\nand it says \"configmap/settings created\" when the POST answers 201 with the object", tail(out))
	}
	if out, err := k.run(ctx, settings("example.com/keep", "  color: blue\n  size: large\n"), "apply", "-f", "-"); err != nil || !strings.Contains(out, "unchanged") {
		return fmt.Errorf("kubectl apply -f of the same manifest again printed:\n%s\nand it says \"unchanged\": it reads the object back, finds the manifest it applied last in the kubectl.kubernetes.io/last-applied-configuration annotation, and has nothing to send", tail(out))
	}
	if out, err := k.run(ctx, settings("example.com/keep, example.com/audit", "  color: green\n"), "apply", "-f", "-"); err != nil || !strings.Contains(out, "configured") {
		return fmt.Errorf("kubectl apply -f of the manifest with color changed, size dropped and a second finalizer printed:\n%s\nand it says \"configmap/settings configured\": kubectl sends a strategic merge patch, ?fieldValidation=Strict, and keys starting with $ — here metadata.$setElementOrder/finalizers — are the patch's directives, not fields of the object", tail(out))
	}
	obj, err := get(settingsPath)
	if err != nil {
		return err
	}
	if finalizers, _ := obj["metadata"].(map[string]any)["finalizers"].([]any); len(finalizers) != 2 {
		return fmt.Errorf("after applying a manifest with finalizers [example.com/keep, example.com/audit] over one with [example.com/keep], the ConfigMap's finalizers are %v: finalizers merge, and the patch lists only the one added", finalizers)
	}
	data, _ := obj["data"].(map[string]any)
	if data["color"] != "green" || data["size"] != nil {
		return fmt.Errorf("after applying a manifest with color: green and no size, the ConfigMap's data is %v: kubectl patches color and sends size: null, because size was in the manifest it applied last and is not in this one — a merge patch's null deletes", obj["data"])
	}

	// get prints what the server says to print.
	out, err = k.run(ctx, "", "get", "configmaps")
	if err != nil {
		return fmt.Errorf("kubectl get configmaps failed:\n%s", tail(out))
	}
	if err := kubectlTable("kubectl get configmaps", out, []string{"NAME", "DATA", "AGE"}, "settings", "1"); err != nil {
		return err
	}
	out, err = k.run(ctx, "", "get", "configmap", "settings")
	if err != nil {
		return fmt.Errorf("kubectl get configmap settings failed:\n%s", tail(out))
	}
	if err := kubectlTable("kubectl get configmap settings", out, []string{"NAME", "DATA", "AGE"}, "settings", "1"); err != nil {
		return fmt.Errorf("%w\n\nreading one object by name asks for a Table too, and it is a Table of one row", err)
	}
	out, err = k.run(ctx, "", "get", "configmaps", "-A", "--show-labels")
	if err != nil {
		return fmt.Errorf("kubectl get configmaps -A --show-labels failed:\n%s", tail(out))
	}
	if err := kubectlTable("kubectl get configmaps -A --show-labels", out, []string{"NAMESPACE", "NAME", "DATA", "AGE", "LABELS"}, "default", "settings", "1", "", "app=web"); err != nil {
		return fmt.Errorf("%w\n\nthe NAMESPACE and LABELS columns are kubectl's, filled from the metadata each row carries in row.object: a PartialObjectMetadata with the object's metadata", err)
	}

	// The same misspelt field, through a create and through a patch.
	typo := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: typo\ndta:\n  color: blue\n"
	out, err = k.run(ctx, typo, "apply", "-f", "-")
	if err == nil || !strings.Contains(out, `unknown field "dta"`) {
		return fmt.Errorf("kubectl apply -f of a ConfigMap with a top-level field \"dta\" printed:\n%s\nand it is refused, naming the field: kubectl sent ?fieldValidation=Strict because the schema said the server checks field names, so the server is the only thing that does — answer 400 with `unknown field \"dta\"` in the message", tail(out))
	}
	if res, _, err := certSend(ctx, srv, alice, http.MethodGet, "/api/v1/namespaces/default/configmaps/typo", "", nil); err != nil || res.StatusCode != http.StatusNotFound {
		return fmt.Errorf("the ConfigMap refused for an unknown field was stored anyway: a write that fails validation stores nothing")
	}
	out, err = k.run(ctx, settings("example.com/keep, example.com/audit", "  color: green\n")+"dta: {}\n", "apply", "-f", "-")
	if err == nil || !strings.Contains(out, `unknown field "dta"`) {
		return fmt.Errorf("kubectl apply -f of the existing ConfigMap settings, now with a field \"dta\", printed:\n%s\nand it is refused like the create was: this one arrives as a PATCH, still with ?fieldValidation=Strict, and the patch's field names are checked against the schema the same way", tail(out))
	}
	out, err = k.run(ctx, typo, "apply", "--validate=warn", "-f", "-")
	if err != nil || !strings.Contains(out, `unknown field "dta"`) || !strings.Contains(out, "configmap/typo created") {
		return fmt.Errorf("kubectl apply --validate=warn of a ConfigMap with a field \"dta\" printed:\n%s\nand it is created with a warning: ?fieldValidation=Warn stores the object without the field and says so in a Warning: 299 - \"unknown field \\\"dta\\\"\" header, which kubectl prints", tail(out))
	}
	if obj, err := get("/api/v1/namespaces/default/configmaps/typo"); err != nil {
		return err
	} else if _, ok := obj["dta"]; ok {
		return fmt.Errorf("the ConfigMap applied with --validate=warn was stored with its unknown field \"dta\": the warning is that the field was dropped — the real server decodes into a type that has no such field, so it never reaches storage")
	}
	out, err = k.run(ctx, strings.Replace(typo, "name: typo", "name: quiet", 1), "apply", "--validate=false", "-f", "-")
	if err != nil || strings.Contains(out, "unknown field") || !strings.Contains(out, "created") {
		return fmt.Errorf("kubectl apply --validate=false of a ConfigMap with a field \"dta\" printed:\n%s\nand it is created without a word: ?fieldValidation=Ignore drops the field and says nothing", tail(out))
	}

	// edit: kubectl fetches the object, runs the editor on it and sends the
	// difference as a patch.
	editor := filepath.Join(dir, "editor.sh")
	script := "#!/bin/sh\nsed 's/color: green/color: purple/' \"$1\" > \"$1.new\" && mv \"$1.new\" \"$1\"\n"
	if err := os.WriteFile(editor, []byte(script), 0o700); err != nil {
		return fmt.Errorf("write the editor script: %w", err)
	}
	out, err = k.runEnv(ctx, []string{"KUBE_EDITOR=" + editor}, "", "edit", "configmap", "settings")
	if err != nil || !strings.Contains(out, "edited") {
		return fmt.Errorf("kubectl edit configmap settings, with an editor that changes color: green to color: purple, printed:\n%s\nand it says \"configmap/settings edited\"", tail(out))
	}
	if obj, err = get(settingsPath); err != nil {
		return err
	}
	if data, _ := obj["data"].(map[string]any); data["color"] != "purple" {
		return fmt.Errorf("after kubectl edit changed color to purple, the ConfigMap's data is %v", obj["data"])
	}

	// Server-side apply: the server merges, and owns the conflicts.
	flags := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: flags\ndata:\n  mode: "
	if out, err := k.run(ctx, flags+"fast\n", "apply", "--server-side", "-f", "-"); err != nil || !strings.Contains(out, "serverside-applied") {
		return fmt.Errorf("kubectl apply --server-side of a new ConfigMap printed:\n%s\nand it says \"configmap/flags serverside-applied\"", tail(out))
	}
	out, err = k.run(ctx, flags+"slow\n", "apply", "--server-side", "--field-manager=other", "-f", "-")
	if err == nil || !strings.Contains(out, "conflict") {
		return fmt.Errorf("kubectl apply --server-side --field-manager=other, setting data.mode that the manager kubectl owns, printed:\n%s\nand it fails with the conflict: the 409 carries the field and its owner, and kubectl prints them", tail(out))
	}
	if out, err := k.run(ctx, flags+"slow\n", "apply", "--server-side", "--field-manager=other", "--force-conflicts", "-f", "-"); err != nil {
		return fmt.Errorf("kubectl apply --server-side --force-conflicts failed:\n%s\nforce=true takes the field over instead of refusing", tail(out))
	}

	// scale, read back as the server's own columns.
	rc := "apiVersion: v1\nkind: ReplicationController\nmetadata:\n  name: web\nspec:\n  replicas: 2\n  selector: {app: web}\n  template:\n    metadata: {labels: {app: web}}\n    spec: {containers: [{name: web, image: nginx}]}\n"
	if out, err := k.run(ctx, rc, "apply", "-f", "-"); err != nil {
		return fmt.Errorf("kubectl apply -f of a ReplicationController failed:\n%s", tail(out))
	}
	if out, err := k.run(ctx, "", "scale", "replicationcontroller", "web", "--replicas=5"); err != nil || !strings.Contains(out, "scaled") {
		return fmt.Errorf("kubectl scale replicationcontroller web --replicas=5 printed:\n%s\nand it says \"replicationcontroller/web scaled\": it patches the scale subresource", tail(out))
	}
	out, err = k.run(ctx, "", "get", "replicationcontrollers")
	if err != nil {
		return fmt.Errorf("kubectl get replicationcontrollers failed:\n%s", tail(out))
	}
	if err := kubectlTable("kubectl get replicationcontrollers after scaling web to 5", out, []string{"NAME", "DESIRED", "CURRENT", "READY", "AGE"}, "web", "5", "0", "0"); err != nil {
		return fmt.Errorf("%w\n\nDESIRED is spec.replicas, CURRENT status.replicas and READY status.readyReplicas, 0 while nothing has reported them", err)
	}

	return kubectlWatch(ctx, k)
}

// kubectlWatch runs kubectl get --watch and insists a change made while it
// runs is printed as a row, promptly.
func kubectlWatch(ctx context.Context, k *kubectl) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", append(k.base, "get", "replicationcontrollers", "--watch")...)
	cmd.Env = k.env
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start kubectl get --watch: %w", err)
	}
	defer func() { cancel(); _ = cmd.Wait() }()
	lines := make(chan string)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(pipe)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	var seen []string
	next := func(want ...string) error {
		timeout := time.After(10 * time.Second)
		for {
			select {
			case line, ok := <-lines:
				if !ok {
					return fmt.Errorf("kubectl get replicationcontrollers --watch exited having printed:\n%s\nstderr:\n%s", strings.Join(seen, "\n"), tail(stderr.String()))
				}
				seen = append(seen, line)
				if fields := strings.Fields(line); len(fields) >= len(want) && slices.Equal(fields[:len(want)], want) {
					return nil
				}
			case <-timeout:
				return fmt.Errorf("kubectl get replicationcontrollers --watch printed no row starting %q within 10s; it printed:\n%s\nstderr:\n%s", strings.Join(want, " "), strings.Join(seen, "\n"), tail(stderr.String()))
			}
		}
	}
	if err := next("web", "5"); err != nil {
		return fmt.Errorf("%w\n\nkubectl lists first, as a Table, and prints that before it watches", err)
	}
	if out, err := k.run(ctx, "", "scale", "replicationcontroller", "web", "--replicas=3"); err != nil {
		return fmt.Errorf("kubectl scale while a watch was open failed:\n%s", tail(out))
	}
	if err := next("web", "3"); err != nil {
		return fmt.Errorf("%w\n\nthe watch kubectl opens asks for a Table too: each event's object is a Table of one row, the changed object's, so it prints as a line under the others", err)
	}
	return nil
}

// kubectlTable insists the output of a kubectl get is a header with exactly
// these columns and a row whose leading cells are these.
func kubectlTable(what, out string, header []string, row ...string) error {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	got := strings.Fields(lines[0])
	if !slices.Equal(got, header) {
		if slices.Equal(got, []string{"NAME", "AGE"}) || slices.Equal(got, []string{"NAMESPACE", "NAME", "AGE"}) {
			return fmt.Errorf("%s printed:\n%s\nwith only NAME and AGE, which is what kubectl falls back to when the server answers with the objects: it asks for a Table in Accept (application/json;as=Table;v=v1;g=meta.k8s.io), and the server decides the columns — here %s", what, tail(out), strings.Join(header, ", "))
		}
		return fmt.Errorf("%s printed:\n%s\nand its columns are %s", what, tail(out), strings.Join(header, ", "))
	}
	// Columns are padded with spaces and a cell may be empty, so the row is
	// cut at the offsets the header's columns start at. An empty want matches
	// any cell.
	var starts []int
	for i := range lines[0] {
		if lines[0][i] != ' ' && (i == 0 || lines[0][i-1] == ' ') {
			starts = append(starts, i)
		}
	}
	starts = append(starts, 1<<30)
	for _, line := range lines[1:] {
		matched := true
		for i, want := range row {
			cell := line[min(starts[i], len(line)):min(starts[i+1], len(line))]
			if want != "" && strings.TrimSpace(cell) != want {
				matched = false
			}
		}
		if matched {
			return nil
		}
	}
	return fmt.Errorf("%s printed:\n%s\nand it has a row %q", what, tail(out), strings.Join(row, "  "))
}

// kubectl runs the real kubectl as alice, against one server, with a
// kubeconfig and discovery cache of its own.
type kubectl struct {
	base []string
	env  []string
}

func newKubectl(dir string, srv *server, pki *certPKI) (*kubectl, error) {
	key, err := x509.MarshalPKCS8PrivateKey(pki.alice.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("encode alice's key: %w", err)
	}
	certFile, keyFile := filepath.Join(dir, "alice.crt"), filepath.Join(dir, "alice.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pki.alice.Certificate[0]}), 0o600); err != nil {
		return nil, err
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0o600); err != nil {
		return nil, err
	}
	config := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: byok8s
  cluster: {server: %q, certificate-authority: %q}
users:
- name: alice
  user: {client-certificate: %q, client-key: %q}
contexts:
- name: byok8s
  context: {cluster: byok8s, user: alice, namespace: default}
current-context: byok8s
`, srv.url, pki.caFile, certFile, keyFile)
	kubeconfig := filepath.Join(dir, "kubeconfig")
	if err := os.WriteFile(kubeconfig, []byte(config), 0o600); err != nil {
		return nil, err
	}
	return &kubectl{
		base: []string{"--kubeconfig", kubeconfig, "--cache-dir", filepath.Join(dir, "cache"), "--request-timeout", "15s"},
		env:  append(os.Environ(), "KUBECONFIG="+kubeconfig),
	}, nil
}

// run runs kubectl with stdin, returning stdout and stderr together.
func (k *kubectl) run(ctx context.Context, stdin string, args ...string) (string, error) {
	return k.runEnv(ctx, nil, stdin, args...)
}

func (k *kubectl) runEnv(ctx context.Context, env []string, stdin string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", append(k.base, args...)...)
	cmd.Env = append(slices.Clone(k.env), env...)
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return string(out), fmt.Errorf("kubectl %s did not finish within 30s", strings.Join(args, " "))
	}
	return string(out), err
}
