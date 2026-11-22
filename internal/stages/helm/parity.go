package helm

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/madhank93/byo-k8s-x/internal/kube"
)

// stageTemplateParity — template agrees with the real helm byte for byte,
// stdout, stderr and exit code, on a chart full of whitespace, typing and
// function traps, on every way a render fails, on --set's typing and syntax
// (list indexes, --set-json and --set-file too), on flags placed after the
// positional arguments, and on charts that render nothing but hooks. Nothing
// touches the cluster.
func stageTemplateParity(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := needHelm(w); err != nil {
		return err
	}
	par := w.chart("par", map[string]string{
		"Chart.yaml": `apiVersion: v2
name: par
version: 0.1.0
appVersion: "1.0"
`,
		"values.yaml": `name: demo
big: 1000000
small: 1.5
port: 8080
zero: 0
flag: false
emptyStr: ""
emptyList: []
emptyMap: {}
nested:
  b: 2
  a: 1
  c:
    z: last
    y: [1, "two", true, null]
tricky:
  - "yes"
  - "on"
  - "1.0"
  - "012"
  - "~"
  - "null"
  - "true"
  - ""
  - "a: b"
  - "#hash"
  - "multi\nline"
  - "trailing\n"
  - "  leading space"
  - "very long string that goes on and on and on and on and on and on and on and on and on and on and on and on and on"
labels:
  zeta: z
  alpha: a
  mid: m
tplString: "{{ .Values.name }}-{{ .Release.Name }}"
html: "<b>&</b>"
`,
		"files/conf.properties": `a=1
b=2
`,
		"files/lines.txt": `line1
line2

`,
		"templates/01-chomp.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: chomp
  labels:
    {{- include "par.labels" . | nindent 4 }}
data:
  a: {{ .Values.name -}}   
  {{- /* comment trimmed both sides */ -}}
  x
  b: "{{- " spaced " -}}"
  c: |
    {{- range $i, $v := list "one" "two" }}
    {{ $i }}={{ $v }}
    {{- end }}
  d: {{ "keep" }}{{ "  " }}end
  ws: |-
    {{- include "par.ws" . }}
`,
		"templates/02-yaml.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: yaml
data:
  nested: |
    {{- toYaml .Values.nested | nindent 4 }}
  indented: {{- toYaml .Values.labels | nindent 4 }}
  emptyMap: {{ toYaml .Values.emptyMap }}
  emptyList: {{ toYaml .Values.emptyList }}
  nil: {{ toYaml .Values.missing }}
  tricky: |
{{ toYaml .Values.tricky | indent 4 }}
  json: {{ toJson .Values.nested | squote }}
  html: {{ toJson .Values.html | squote }}
  rawjson: {{ toRawJson .Values.html | squote }}
  pretty: |
{{ toPrettyJson .Values.labels | indent 4 }}
`,
		"templates/03-numbers.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: numbers
data:
  big: {{ .Values.big | quote }}
  bigInt: {{ .Values.big | int | quote }}
  bigPrintf: {{ printf "%d" (.Values.big | int) | quote }}
  small: {{ .Values.small | quote }}
  port: {{ .Values.port | quote }}
  add: {{ add 1 2.7 | quote }}
  addf: {{ addf 1 2.7 | quote }}
  div: {{ div 7 2 | quote }}
  intStr: {{ "42abc" | int | quote }}
  floatStr: {{ "3.9" | int | quote }}
  list: {{ .Values.nested.c.y | quote }}
  map: {{ .Values.labels | quote }}
  printfv: {{ printf "%v" .Values.labels | quote }}
  missing: {{ .Values.nope | quote }}
  missingRaw: "{{ .Values.nope }}"
  missingDeep: "{{ .Values.nested.nope }}"
`,
		"templates/04-range.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: range
data:
  {{- range $k, $v := .Values.labels }}
  {{ $k }}: {{ $v | quote }}
  {{- end }}
  sortedKeys: {{ keys .Values.labels | sortAlpha | join "," | quote }}
  {{- range .Values.emptyList }}
  never: here
  {{- else }}
  elseBranch: "empty list"
  {{- end }}
  nestedRange: |
    {{- range $k, $v := .Values.nested }}
    {{ $k }}={{ $v }}
    {{- end }}
`,
		"templates/05-default.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: defaults
data:
  zero: {{ .Values.zero | default "dflt" | quote }}
  flag: {{ .Values.flag | default "dflt" | quote }}
  emptyStr: {{ .Values.emptyStr | default "dflt" | quote }}
  emptyList: {{ .Values.emptyList | default "dflt" | quote }}
  emptyMap: {{ .Values.emptyMap | default "dflt" | quote }}
  missing: {{ .Values.missing | default "dflt" | quote }}
  name: {{ .Values.name | default "dflt" | quote }}
  emptyZeroStr: {{ empty "0" | quote }}
  emptyZero: {{ empty 0 | quote }}
  coalesce: {{ coalesce .Values.zero .Values.emptyStr .Values.name | quote }}
  ternary: {{ ternary "yes" "no" .Values.flag | quote }}
  hasKeyFlag: {{ hasKey .Values "flag" | quote }}
  dig: {{ dig "c" "z" "none" .Values.nested | quote }}
  digMissing: {{ dig "q" "r" "none" .Values.nested | quote }}
  {{- if .Values.flag }}
  ifFalse: shown
  {{- end }}
  {{- with .Values.emptyMap }}
  withEmpty: shown
  {{- end }}
  {{- if eq (int .Values.port) 8080 }}
  eqIntFloat: "true"
  {{- end }}
`,
		"templates/06-tpl-lookup.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: tpl-lookup
data:
  tpl: {{ tpl .Values.tplString . | quote }}
  tplInline: {{ tpl "{{ upper .Values.name }}" . | quote }}
  lookup: {{ lookup "v1" "ConfigMap" "default" "kube-root-ca.crt" | quote }}
  lookupList: {{ lookup "v1" "ConfigMap" "default" "" | quote }}
  lookupLen: {{ len (lookup "v1" "Namespace" "" "") | quote }}
  {{- if not (lookup "v1" "Secret" .Release.Namespace "x") }}
  lookupFalsy: "true"
  {{- end }}
  kube: {{ .Capabilities.KubeVersion.Version | quote }}
  ns: {{ .Release.Namespace | quote }}
  isInstall: {{ .Release.IsInstall | quote }}
  service: {{ .Release.Service | quote }}
  chart: {{ .Chart.Name }}-{{ .Chart.Version }}
  templateName: {{ .Template.Name | quote }}
  basePath: {{ .Template.BasePath | quote }}
  hasApps: {{ .Capabilities.APIVersions.Has "apps/v1" | quote }}
  hasCRD: {{ .Capabilities.APIVersions.Has "example.com/v1" | quote }}
`,
		"templates/07-files.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: files
data:
  {{- (.Files.Glob "files/*.properties").AsConfig | nindent 2 }}
  lines: {{ .Files.Lines "files/lines.txt" | toJson | squote }}
  get: {{ .Files.Get "files/conf.properties" | quote }}
  missingFile: {{ .Files.Get "files/none" | quote }}
  b64: {{ "hello" | b64enc }}
  sha: {{ "hello" | sha256sum | trunc 12 }}
  regex: {{ regexReplaceAll "[aeiou]" "helm template" "_" }}
  semver: {{ semverCompare ">=1.20.0-0" .Capabilities.KubeVersion.Version | quote }}
  title: {{ "hello world" | title }}
  snake: {{ "HelloWorld" | snakecase }}
  splitList: {{ splitList "," "a,b,,c" | toJson | squote }}
---
# a document that is only a comment
---
{{- if false }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: never
{{- end }}
---
apiVersion: v1
kind: Secret
metadata:
  name: sec
stringData:
  k: {{ .Values.html }}
data:
  enc: {{ "s3cr3t" | b64enc | quote }}
`,
		"templates/_helpers.tpl": `{{- define "par.labels" -}}
app: {{ .Values.name }}
release: {{ .Release.Name }}
{{- end -}}
{{- define "par.ws" }}
  padded: true
{{ end }}
`,
	})
	perr := w.chart("perr", map[string]string{
		"Chart.yaml": `apiVersion: v2
name: perr
version: 0.1.0
`,
		"values.yaml": `case: none
port: 8080
nested:
  a: 1
`,
		"templates/cm.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: errs
data:
{{- if eq .Values.case "required" }}
  r: {{ required "A valid .Values.who entry required!" .Values.who }}
{{- else if eq .Values.case "requiredEmpty" }}
  r: {{ required "who must be non-empty" .Values.who | quote }}
{{- else if eq .Values.case "fail" }}
  {{- fail "this chart refuses to render" }}
{{- else if eq .Values.case "nilptr" }}
  n: {{ .Values.nested.missing.deeper }}
{{- else if eq .Values.case "eq" }}
  e: {{ eq .Values.port 8080 }}
{{- else if eq .Values.case "dig" }}
  d: {{ dig "nested" "a" "x" .Values }}
{{- else if eq .Values.case "include" }}
  i: {{ include "nope" . }}
{{- else if eq .Values.case "tplerr" }}
  t: {{ tpl "{{ .Values.nested.missing.deeper }}" . }}
{{- else if eq .Values.case "badyaml" }}
  b: {{ toYaml .Values.nested | indent 2 }}
{{- else if eq .Values.case "set" }}
  v: {{ .Values.v | quote }}
  kind: {{ kindOf .Values.v | quote }}
{{- else if eq .Values.case "json" }}
  j: {{ toJson .Values | squote }}
{{- end }}
`,
	})
	pbad := w.chart("pbad", map[string]string{
		"Chart.yaml":       "apiVersion: v2\nname: pbad\nversion: 0.1.0\n",
		"templates/a.yaml": "a: {{ .Values.x \n",
	})
	pfn := w.chart("pfn", map[string]string{
		"Chart.yaml":       "apiVersion: v2\nname: pfn\nversion: 0.1.0\n",
		"templates/a.yaml": "a: {{ include \"x\" . | nosuchfunc }}\n",
	})
	ptpl := w.chart("ptpl", map[string]string{
		"Chart.yaml":       "apiVersion: v2\nname: ptpl\nversion: 0.1.0\n",
		"templates/a.yaml": "a: {{ tpl \"{{ required \\\"deep inside tpl\\\" .Values.no }}\" . }}\nb: {{ tpl \"{{ .x \" . }}\n",
	})
	phook := w.chart("phook", map[string]string{
		"Chart.yaml": "apiVersion: v2\nname: phook\nversion: 0.1.0\n",
		"templates/hook.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: only-a-hook
  annotations:
    helm.sh/hook: pre-install
`,
	})
	big := w.file("big.yaml", "v: 1000000\n")
	note := w.file("note.txt", "line1\nline2\n")
	missing := filepath.Join(w.dir, "missing.txt")

	set := func(v ...string) []string {
		return append(append([]string{"--set", "case=set"}, v...), "-s", "templates/cm.yaml", "rel", perr)
	}
	cases := [][]string{
		{"rel", par},
		{"--kube-version", "1.29.3", "-s", "templates/06-tpl-lookup.yaml", "rel", par},
		{"-s", "templates/0[34]-*.yaml", "rel", par},
		{"--set", "case=required", "rel", perr},
		{"--set", "case=requiredEmpty", "--set", "who=", "rel", perr},
		{"--set", "case=fail", "rel", perr},
		{"--set", "case=nilptr", "rel", perr},
		{"--set", "case=eq", "rel", perr},
		{"--set", "case=include", "rel", perr},
		{"--set", "case=tplerr", "rel", perr},
		{"--set", "case=badyaml", "rel", perr},
		{"rel", pbad},
		{"rel", pfn},
		{"rel", ptpl},
		{"-s", "templates/nope.yaml", "rel", perr},
		set("--set", "v=1000000"),
		set("-f", big),
		set("--set", "v=1.5"),
		set("--set", "v=1e3"),
		set("--set", "v=012"),
		set("--set", "v=true"),
		set("--set", "v=null"),
		set("--set", `v="quoted"`),
		set("--set", "v={a,b}"),
		set("--set-string", "v=1000000"),
		set("--set", "v=7", "--set-string", "v=8"),
		{"--set", `case=json,a=x\,y,b.c\.d=1,e={1,two,true},g={},h=a=b`, "--set", "empty=", "rel", perr},
		{"--set", "case=json", "--set-string", "s={1,2},t=true", "rel", perr},
		{"--set", "case=json", "--set", "x={a", "rel", perr},
		{"--set", "case=json", "--set", "x", "rel", perr},
		{"rel", phook},
		{"-s", "templates/hook.yaml", "rel", phook},
		// List indexes, JSON and file values, applied in helm's order.
		{"--set", "case=json", "--set", "a[2].b=y,a[0]=x,a[2].c[1]=z", "--set", "m[1][1]=1", "--set", "m[0]={1,2}", "rel", perr},
		{"--set", "case=json", "--set", "a[1]=y", "--set", "a[0]=x", "--set", "a[3]", "--set", "b[0]=1,b[0]=2", "rel", perr},
		{"--set", "case=json", "--set-json", `j={"x":[1,2],"y":null,"z":1.5},k=[1,"a"]`, "--set-json", "l[1]=true", "--set", "j.w=2", "rel", perr},
		{"--set", "case=json,o=set,p=set", "--set-json", "o=1", "--set-string", "p=str", "--set-file", "p=" + note, "rel", perr},
		{"--set", "case=json", "--set-file", "f=" + note + ",g[1]=" + note, "rel", perr},
		{"--set", "case=json", "--set", "a[0]=x", "--set", "a.b=1", "rel", perr},
		{"--set", "case=json", "--set", "a[-1]=x", "rel", perr},
		{"--set", "case=json", "--set", "a[0]b=1", "rel", perr},
		{"--set", "case=json", "--set", "a[x]=1", "rel", perr},
		{"--set", "case=json", "--set", "a[65537]=1", "rel", perr},
		{"--set", "case=json", "--set-json", "j={x", "rel", perr},
		{"--set", "case=json", "--set-file", "f=" + missing, "rel", perr},
		// Flags anywhere among the arguments, and -- to end them.
		{"rel", "--set", "case=json", perr, "--set", "a[0]=1", "-n", "after"},
		{"--set", "case=json", "--", "rel", perr},
	}
	for _, c := range cases {
		if err := sameTemplate(w, append([]string{"template"}, c...)); err != nil {
			return err
		}
	}
	return nil
}

// sameTemplate runs one command on both and wants the same exit code, stdout
// and stderr, byte for byte, with the grader's paths written as DIR.
func sameTemplate(w *work, args []string) error {
	want, err := w.helm(args...)
	if err != nil {
		return err
	}
	got, err := w.run(args...)
	if err != nil {
		return err
	}
	shown := strings.ReplaceAll(strings.Join(args, " "), w.dir, "DIR")
	if err := wantExit(got, want.ExitCode); err != nil {
		return fmt.Errorf("%s, which exits %d for the real helm: %w", shown, want.ExitCode, err)
	}
	if err := wantExact(got.Stdout, want.Stdout, shown+" (stdout)"); err != nil {
		return err
	}
	return wantExact(goNeutral(got.Stderr), goNeutral(want.Stderr), shown+" (stderr)")
}

// goTypes is the operand-type suffix newer Go releases add to text/template's
// comparison error; whether it appears depends on the Go that built each binary.
var goTypes = regexp.MustCompile(`(incompatible types for comparison): \S+ and \S+`)

func goNeutral(s string) string { return goTypes.ReplaceAllString(s, "$1") }
