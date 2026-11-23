// A kustomize: `build DIR` reads DIR's kustomization and prints the resources
// it produces as one YAML stream.
package main

import (
	"bytes"
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

	CommonLabels map[string]string `json:"commonLabels,omitempty"`
	Labels       []Label           `json:"labels,omitempty"`

	CommonAnnotations map[string]string `json:"commonAnnotations,omitempty"`
}

// Label is one entry of labels: pairs, and how far into the object they go.
type Label struct {
	Pairs            map[string]string `json:"pairs,omitempty"`
	IncludeSelectors bool              `json:"includeSelectors,omitempty"`
	IncludeTemplates bool              `json:"includeTemplates,omitempty"`
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
	// YAMLToJSON first: yaml.Unmarshal into a struct would quietly turn a
	// number into the string a field wants, which kustomize refuses.
	js, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, err
	}
	var k Kustomization
	dec := json.NewDecoder(bytes.NewReader(js))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&k); err != nil {
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
	if len(k.CommonLabels) > 0 {
		apply(out, labelSpecs, setPairs(k.CommonLabels))
	}
	for _, l := range k.Labels {
		specs := labelSpecs[:1]
		switch {
		case l.IncludeSelectors:
			specs = labelSpecs
		case l.IncludeTemplates:
			specs = nil
			for _, fs := range labelSpecs {
				if strings.HasSuffix(fs.path, "metadata/labels") {
					specs = append(specs, fs)
				}
			}
		}
		apply(out, specs, setPairs(l.Pairs))
	}
	if len(k.CommonAnnotations) > 0 {
		apply(out, annotationSpecs, setPairs(k.CommonAnnotations))
	}
	return out, nil
}

// fieldSpec is a field kustomize knows to change: a slash path, in objects of
// a group and kind ("" matches any), created when missing only if create.
// A path segment that meets a list applies to every item.
type fieldSpec struct {
	group, kind, path string
	create            bool
}

// apply runs set on the field each matching spec names.
func apply(objs []obj, specs []fieldSpec, set func(parent obj, key string)) {
	for _, o := range objs {
		for _, fs := range specs {
			if (fs.group == "" || fs.group == group(o)) && (fs.kind == "" || fs.kind == str(o, "kind")) {
				walk(o, strings.Split(fs.path, "/"), fs.create, set)
			}
		}
	}
}

func walk(v any, path []string, create bool, set func(parent obj, key string)) {
	switch v := v.(type) {
	case []any:
		for _, item := range v {
			walk(item, path, create, set)
		}
	case obj:
		key := strings.TrimSuffix(path[0], "[]")
		if len(path) == 1 {
			if _, ok := v[key]; ok || create {
				set(v, key)
			}
			return
		}
		next, ok := v[key]
		if !ok || next == nil {
			if !create {
				return
			}
			next = obj{}
			v[key] = next
		}
		walk(next, path[1:], create, set)
	}
}

// setPairs merges pairs into the map at parent[key], creating it.
func setPairs(pairs map[string]string) func(obj, string) {
	return func(parent obj, key string) {
		m, ok := parent[key].(obj)
		if !ok {
			m = obj{}
			parent[key] = m
		}
		for k, v := range pairs {
			m[k] = v
		}
	}
}

// selectorTerms are the optional label selectors inside a pod template.
var selectorTerms = []string{
	"spec/template/spec/affinity/podAffinity/preferredDuringSchedulingIgnoredDuringExecution/podAffinityTerm/labelSelector/matchLabels",
	"spec/template/spec/affinity/podAffinity/requiredDuringSchedulingIgnoredDuringExecution/labelSelector/matchLabels",
	"spec/template/spec/affinity/podAntiAffinity/preferredDuringSchedulingIgnoredDuringExecution/podAffinityTerm/labelSelector/matchLabels",
	"spec/template/spec/affinity/podAntiAffinity/requiredDuringSchedulingIgnoredDuringExecution/labelSelector/matchLabels",
	"spec/template/spec/topologySpreadConstraints/labelSelector/matchLabels",
}

// labelSpecs is where commonLabels goes; metadata/labels comes first.
var labelSpecs = func() []fieldSpec {
	specs := []fieldSpec{
		{"", "", "metadata/labels", true},
		{"", "ReplicationController", "spec/selector", true},
		{"", "ReplicationController", "spec/template/metadata/labels", true},
		{"", "Service", "spec/selector", true},
		{"", "Deployment", "spec/selector/matchLabels", true},
		{"", "Deployment", "spec/template/metadata/labels", true},
		{"", "ReplicaSet", "spec/selector/matchLabels", true},
		{"", "ReplicaSet", "spec/template/metadata/labels", true},
		{"", "DaemonSet", "spec/selector/matchLabels", true},
		{"", "DaemonSet", "spec/template/metadata/labels", true},
		{"apps", "StatefulSet", "spec/selector/matchLabels", true},
		{"apps", "StatefulSet", "spec/template/metadata/labels", true},
		{"apps", "StatefulSet", "spec/volumeClaimTemplates[]/metadata/labels", true},
		{"batch", "Job", "spec/selector/matchLabels", false},
		{"batch", "Job", "spec/template/metadata/labels", true},
		{"batch", "CronJob", "spec/jobTemplate/spec/selector/matchLabels", false},
		{"batch", "CronJob", "spec/jobTemplate/metadata/labels", true},
		{"batch", "CronJob", "spec/jobTemplate/spec/template/metadata/labels", true},
		{"policy", "PodDisruptionBudget", "spec/selector/matchLabels", false},
		{"networking.k8s.io", "NetworkPolicy", "spec/podSelector/matchLabels", false},
		{"networking.k8s.io", "NetworkPolicy", "spec/ingress/from/podSelector/matchLabels", false},
		{"networking.k8s.io", "NetworkPolicy", "spec/egress/to/podSelector/matchLabels", false},
	}
	for _, kind := range []string{"Deployment", "StatefulSet"} {
		for _, p := range selectorTerms {
			specs = append(specs, fieldSpec{"apps", kind, p, false})
		}
	}
	return specs
}()

// annotationSpecs is where commonAnnotations goes: metadata and pod templates.
var annotationSpecs = []fieldSpec{
	{"", "", "metadata/annotations", true},
	{"", "ReplicationController", "spec/template/metadata/annotations", true},
	{"", "Deployment", "spec/template/metadata/annotations", true},
	{"", "ReplicaSet", "spec/template/metadata/annotations", true},
	{"", "DaemonSet", "spec/template/metadata/annotations", true},
	{"apps", "StatefulSet", "spec/template/metadata/annotations", true},
	{"batch", "Job", "spec/template/metadata/annotations", true},
	{"batch", "CronJob", "spec/jobTemplate/metadata/annotations", true},
	{"batch", "CronJob", "spec/jobTemplate/spec/template/metadata/annotations", true},
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

// sortKey orders by group, version and kind, then by namespace and name,
// with kustomize's placeholders for the empty ones ('~' sorts after letters,
// and '|' after every character a name can hold).
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
	return group + "_" + version + "_" + str(o, "kind") + "|" + ns + "|" + str(o, "metadata", "name")
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
