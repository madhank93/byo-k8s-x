// A kustomize: `build DIR` reads DIR's kustomization and prints the resources
// it produces as one YAML stream.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"
)

// Kustomization is the kustomization file. Decoding is strict, so a field
// missing here is a field kustomize users would see rejected.
type Kustomization struct {
	APIVersion string   `json:"apiVersion,omitempty"`
	Kind       string   `json:"kind,omitempty"`
	Resources  []string `json:"resources,omitempty"`
	Namespace  string   `json:"namespace,omitempty"`
	NamePrefix string   `json:"namePrefix,omitempty"`
	NameSuffix string   `json:"nameSuffix,omitempty"`
}

// obj is one resource, as decoded YAML.
type obj = map[string]any

func main() {
	if len(os.Args) != 3 || os.Args[1] != "build" {
		fmt.Fprintln(os.Stderr, "usage: app build DIR")
		os.Exit(2)
	}
	objs, err := build(os.Args[2], map[string]bool{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
	sortLegacy(objs)
	out, err := render(objs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
	fmt.Print(out)
}

func render(objs []obj) (string, error) {
	var docs []string
	for _, o := range objs {
		b, err := yaml.Marshal(o)
		if err != nil {
			return "", err
		}
		docs = append(docs, string(b))
	}
	return strings.Join(docs, "---\n"), nil
}

// kustomizationNames are the file names kustomize looks for, in its order.
var kustomizationNames = []string{"kustomization.yaml", "kustomization.yml", "Kustomization"}

func load(dir string) (*Kustomization, error) {
	var found []string
	for _, n := range kustomizationNames {
		if _, err := os.Stat(filepath.Join(dir, n)); err == nil {
			found = append(found, n)
		}
	}
	switch len(found) {
	case 0:
		return nil, fmt.Errorf("unable to find one of %s in directory %q", strings.Join(kustomizationNames, ", "), dir)
	case 1:
	default:
		return nil, fmt.Errorf("found multiple kustomization files under %s", dir)
	}
	data, err := os.ReadFile(filepath.Join(dir, found[0]))
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(data)) == "" {
		return nil, fmt.Errorf("%s is empty", found[0])
	}
	var k Kustomization
	if err := yaml.Unmarshal(data, &k, func(d *json.Decoder) *json.Decoder {
		d.DisallowUnknownFields()
		return d
	}); err != nil {
		return nil, fmt.Errorf("invalid Kustomization: %w", err)
	}
	if k.Kind != "" && k.Kind != "Kustomization" {
		return nil, errors.New("kind should be Kustomization")
	}
	if k.APIVersion != "" && k.APIVersion != "kustomize.config.k8s.io/v1beta1" {
		return nil, errors.New("apiVersion for Kustomization should be kustomize.config.k8s.io/v1beta1")
	}
	return &k, nil
}

// build produces a kustomization directory's resources. visiting holds the
// directories being built further up, so a cycle is caught, not recursed.
func build(dir string, visiting map[string]bool) ([]obj, error) {
	abs, err := filepath.Abs(dir)
	if err == nil {
		abs, err = filepath.EvalSymlinks(abs)
	}
	if err != nil {
		return nil, err
	}
	if visiting[abs] {
		return nil, fmt.Errorf("cycle detected: %s includes itself", abs)
	}
	visiting[abs] = true
	defer delete(visiting, abs)

	k, err := load(abs)
	if err != nil {
		return nil, err
	}
	var out []obj
	for _, r := range k.Resources {
		objs, err := accumulate(abs, r, visiting)
		if err != nil {
			return nil, fmt.Errorf("accumulating resources from %q: %w", r, err)
		}
		out = append(out, objs...)
		if err := unique(out); err != nil {
			return nil, fmt.Errorf("may not add resource with an already registered id: %w", err)
		}
	}
	if k.Namespace != "" {
		setNamespace(out, k.Namespace)
		if err := unique(out); err != nil {
			return nil, fmt.Errorf("namespace transformation produces ID conflict: %w", err)
		}
	}
	for _, o := range out {
		if !fixedName[str(o, "kind")] {
			meta(o)["name"] = k.NamePrefix + str(o, "metadata", "name") + k.NameSuffix
		}
	}
	return out, nil
}

// fixedName are the kinds whose names the API server derives from their
// content, so a prefix would break them.
var fixedName = map[string]bool{"Namespace": true, "CustomResourceDefinition": true, "APIService": true}

// unique refuses two resources with one identity.
func unique(objs []obj) error {
	seen := map[string]bool{}
	for _, o := range objs {
		if seen[id(o)] {
			return errors.New(id(o))
		}
		seen[id(o)] = true
	}
	return nil
}

// clusterKinds are the built-in kinds with no namespace, by group/kind.
var clusterKinds = map[string]bool{
	"/Namespace": true, "/Node": true, "/PersistentVolume": true, "/ComponentStatus": true,
	"rbac.authorization.k8s.io/ClusterRole": true, "rbac.authorization.k8s.io/ClusterRoleBinding": true,
	"apiextensions.k8s.io/CustomResourceDefinition": true, "apiregistration.k8s.io/APIService": true,
	"storage.k8s.io/StorageClass": true, "storage.k8s.io/CSIDriver": true, "storage.k8s.io/CSINode": true,
	"storage.k8s.io/VolumeAttachment": true, "scheduling.k8s.io/PriorityClass": true,
	"node.k8s.io/RuntimeClass": true, "networking.k8s.io/IngressClass": true,
	"admissionregistration.k8s.io/MutatingWebhookConfiguration":     true,
	"admissionregistration.k8s.io/ValidatingWebhookConfiguration":   true,
	"admissionregistration.k8s.io/ValidatingAdmissionPolicy":        true,
	"admissionregistration.k8s.io/ValidatingAdmissionPolicyBinding": true,
	"certificates.k8s.io/CertificateSigningRequest":                 true,
	"flowcontrol.apiserver.k8s.io/FlowSchema":                       true,
	"flowcontrol.apiserver.k8s.io/PriorityLevelConfiguration":       true,
}

func group(o obj) string {
	g, _, ok := strings.Cut(str(o, "apiVersion"), "/")
	if !ok {
		return ""
	}
	return g
}

func clusterScoped(o obj) bool { return clusterKinds[group(o)+"/"+str(o, "kind")] }

// meta is the object's metadata, created if it is missing.
func meta(o obj) obj {
	m, ok := o["metadata"].(obj)
	if !ok {
		m = obj{}
		o["metadata"] = m
	}
	return m
}

// setNamespace moves every namespaced resource into ns. A Namespace object is
// renamed to match, and a binding's subjects named default follow it too.
func setNamespace(objs []obj, ns string) {
	for _, o := range objs {
		if group(o) == "rbac.authorization.k8s.io" && strings.HasSuffix(str(o, "kind"), "RoleBinding") {
			subjects, _ := o["subjects"].([]any)
			for _, sub := range subjects {
				if m, ok := sub.(obj); ok && m["name"] == "default" {
					m["namespace"] = ns
				}
			}
		}
		switch {
		case group(o) == "" && str(o, "kind") == "Namespace":
			meta(o)["name"] = ns
		case !clusterScoped(o):
			meta(o)["namespace"] = ns
		}
	}
}

// accumulate reads one entry of resources: a directory is another
// kustomization, and a file must sit inside the kustomization's own root.
func accumulate(root, entry string, visiting map[string]bool) ([]obj, error) {
	path := filepath.Join(root, entry)
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return build(path, visiting)
	}
	if rel, err := filepath.Rel(root, path); err != nil || strings.HasPrefix(rel, "..") {
		return nil, fmt.Errorf("security; file %q is not in or below %q", path, root)
	}
	return readResources(path)
}

// id is what makes a resource unique: group, version, kind, namespace, name.
func id(o obj) string {
	return str(o, "apiVersion") + "|" + str(o, "kind") + "|" + str(o, "metadata", "namespace") + "|" + str(o, "metadata", "name")
}

// Kinds kustomize prints first, in this order, and last; everything else
// sits between them.
var (
	orderFirst = []string{"Namespace", "ResourceQuota", "StorageClass", "CustomResourceDefinition",
		"ServiceAccount", "PodSecurityPolicy", "Role", "ClusterRole", "RoleBinding", "ClusterRoleBinding",
		"ConfigMap", "Secret", "Endpoints", "Service", "LimitRange", "PriorityClass", "PersistentVolume",
		"PersistentVolumeClaim", "Deployment", "StatefulSet", "CronJob", "PodDisruptionBudget"}
	orderLast = []string{"MutatingWebhookConfiguration", "ValidatingWebhookConfiguration"}
)

func kindRank(kind string) int {
	for i, k := range orderFirst {
		if k == kind {
			return i - len(orderFirst)
		}
	}
	for i, k := range orderLast {
		if k == kind {
			return i + 1
		}
	}
	return 0
}

// sortKey orders by group, version and kind, then by name and namespace,
// with kustomize's placeholders for the empty ones ('~' sorts after letters).
func sortKey(o obj) string {
	group, version, ok := strings.Cut(str(o, "apiVersion"), "/")
	if !ok {
		group, version = "", group
	}
	ns := str(o, "metadata", "namespace")
	if group == "" {
		group = "~G"
	}
	if version == "" {
		version = "~V"
	}
	if ns == "" {
		ns = "~X"
	}
	return group + "_" + version + "_" + str(o, "kind") + "|" + str(o, "metadata", "name") + "_" + ns
}

func sortLegacy(objs []obj) {
	sort.SliceStable(objs, func(i, j int) bool {
		ri, rj := kindRank(str(objs[i], "kind")), kindRank(str(objs[j], "kind"))
		if ri != rj {
			return ri < rj
		}
		return sortKey(objs[i]) < sortKey(objs[j])
	})
}

var docSep = regexp.MustCompile(`(?m)^---[ \t]*$`)

// readResources decodes every document of a YAML file.
func readResources(path string) ([]obj, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []obj
	for _, doc := range docSep.Split(string(data), -1) {
		var o obj
		if err := yaml.Unmarshal([]byte(doc), &o); err != nil {
			return nil, err
		}
		if o == nil {
			continue
		}
		if items, ok := o["items"].([]any); ok && strings.HasSuffix(str(o, "kind"), "List") {
			for _, it := range items {
				m, ok := it.(obj)
				if !ok {
					return nil, errors.New("a List item is not an object")
				}
				out = append(out, m)
			}
			continue
		}
		if str(o, "kind") == "" {
			return nil, errors.New("missing kind in object")
		}
		if str(o, "metadata", "name") == "" {
			return nil, errors.New("missing metadata.name in object")
		}
		out = append(out, o)
	}
	return out, nil
}

// str reads the string at a path of map keys, or "".
func str(o obj, path ...string) string {
	var v any = o
	for _, p := range path {
		m, ok := v.(obj)
		if !ok {
			return ""
		}
		v = m[p]
	}
	s, _ := v.(string)
	return s
}
