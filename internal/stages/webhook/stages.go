// Package webhook holds the assertions for the "Build your own admission
// webhook" course — one function per stage, registered by slug.
//
// An admission webhook is judged from the other side: the API server calls the
// learner's program, so almost every stage here starts the program, registers
// it (or lets it register itself), then applies an object and asks what the
// API server did with it. The verdict is the cluster's, not ours. Nothing
// inspects the learner's source — any program the API server is satisfied by
// is a correct one.
package webhook

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// StageTimeout bounds one stage.
const StageTimeout = stages.Timeout

// Stage is one gradable step, in the shape cmd/tester dispatches on.
type Stage = stages.Stage

// externalHost is the name the API server uses to reach a program running on
// this machine.
//
// The cluster is a kind node, which is a container: "localhost" there is the
// container, not the host the learner's webhook is listening on. Docker
// publishes the host under this name, and it is passed to the program rather
// than compiled into it, so the same program would work unchanged against a
// cluster that reaches it some other way.
const externalHost = "host.docker.internal"

var registry = map[string]Stage{}

func register(s Stage) { registry[s.Slug] = s }

// Lookup resolves a stage slug within this course.
func Lookup(slug string) (Stage, bool) {
	s, ok := registry[slug]
	return s, ok
}

func init() {
	register(Stage{Slug: "serve-tls", Run: stageServeTLS})
	register(Stage{Slug: "admission-review", Run: stageAdmissionReview})
}

// stageServeTLS checks the one thing every later stage rests on: that the
// program is listening, and listening with TLS.
//
// The API server will not speak plaintext to a webhook, and it will not
// negotiate: a webhook that serves HTTP is simply unreachable, and the failure
// arrives later as an admission timeout that says nothing about certificates.
func stageServeTLS(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return fmt.Errorf("the program never said it was serving: %w", err)
	}

	if err := waitFor(ctx, "the webhook to answer over TLS", 30*time.Second, func(ctx context.Context) (bool, error) {
		_, err := tlsGet(ctx, "https://"+addr+"/healthz")
		return err == nil, nil
	}); err != nil {
		return fmt.Errorf("nothing answered HTTPS on %s: %w\nthe program said:\n%s", addr, err, tail(p.Stdout()))
	}

	// A plaintext request to a TLS listener fails at the handshake. If this
	// one succeeds, the program is serving HTTP and the API server will never
	// reach it.
	if _, err := plainGet(ctx, "http://"+addr+"/healthz"); err == nil {
		return fmt.Errorf("the program answered a plain HTTP request on %s — the API server only calls webhooks over TLS", addr)
	}
	return nil
}

// stageAdmissionReview checks the request and response shape on its own,
// before the API server is involved.
//
// An AdmissionReview is a wrapper: the same kind goes in and comes back, with
// the answer in .response. Getting this wrong is the most common way a webhook
// fails silently — the API server reads no verdict and treats the reply as
// malformed.
func stageAdmissionReview(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	const uid = "3f2b1c00-0000-4000-8000-000000000001"
	review := fmt.Sprintf(`{
	  "apiVersion": "admission.k8s.io/v1",
	  "kind": "AdmissionReview",
	  "request": {
	    "uid": %q,
	    "operation": "CREATE",
	    "resource": {"group": "", "version": "v1", "resource": "pods"},
	    "namespace": %q,
	    "object": {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "asked-about"}}
	  }
	}`, uid, env.Namespace)

	var body string
	if err := waitFor(ctx, "the webhook to answer an AdmissionReview", 30*time.Second, func(ctx context.Context) (bool, error) {
		got, err := tlsPost(ctx, "https://"+addr+"/validate", review)
		if err != nil {
			return false, nil
		}
		body = got
		return true, nil
	}); err != nil {
		return fmt.Errorf("POST /validate never answered: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	switch {
	case !strings.Contains(body, `"kind":"AdmissionReview"`) && !strings.Contains(body, `"kind": "AdmissionReview"`):
		return fmt.Errorf("the reply is not an AdmissionReview — the same kind goes out as came in:\n%s", tail(body))
	case !strings.Contains(body, uid):
		return fmt.Errorf("the reply does not carry the request's uid, so the API server cannot match it to the request it made:\n%s", tail(body))
	case !strings.Contains(body, `"allowed"`):
		return fmt.Errorf("the reply has no .response.allowed, which is the whole verdict:\n%s", tail(body))
	}
	return nil
}

// --- helpers ---------------------------------------------------------------

// launch starts the learner's webhook with a kubeconfig scoped to this stage's
// namespace, the port it should listen on, and the name the API server will
// reach it by.
func launch(ctx context.Context, env *kube.Env, bin string, port int) (*runner.Process, func(), error) {
	kc, cleanupEnv, err := scoped(env)
	if err != nil {
		return nil, nil, err
	}
	args := []string{
		fmt.Sprintf("--addr=0.0.0.0:%d", port),
		fmt.Sprintf("--external-host=%s", externalHost),
	}
	p, err := runner.Start(ctx, bin, kc, args...)
	if err != nil {
		cleanupEnv()
		return nil, nil, err
	}
	return p, func() {
		p.Stop(5 * time.Second)
		cleanupEnv()
	}, nil
}

// scoped writes a kubeconfig whose context selects this stage's namespace and
// returns it as an environment entry, so the program works in its own
// namespace without needing a flag it has not learned yet.
func scoped(env *kube.Env) ([]string, func(), error) {
	dir, err := os.MkdirTemp("", "byok8s-stage-")
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { os.RemoveAll(dir) }
	path, err := env.KubeconfigScoped(dir)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return []string{"KUBECONFIG=" + path}, cleanup, nil
}

// freePort asks the kernel for a port nothing is using.
//
// The port has to be known before the program starts — it goes into the
// webhook's registration — and a fixed one would collide with whatever the
// last stage left behind.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find a free port: %w", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// insecureClient trusts whatever certificate the webhook presents.
//
// The API server is told to trust it through a caBundle; the harness has no
// such bundle and does not need one, because what is being graded is that the
// program serves TLS at all, not that this machine trusts it.
func insecureClient() *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
}

func tlsGet(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	return send(insecureClient(), req)
}

func tlsPost(ctx context.Context, url, body string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	return send(insecureClient(), req)
}

func plainGet(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	return send(&http.Client{Timeout: 10 * time.Second}, req)
}

func send(client *http.Client, req *http.Request) (string, error) {
	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	if res.StatusCode != http.StatusOK {
		return string(body), fmt.Errorf("%s: %s", req.URL, res.Status)
	}
	return string(body), nil
}

// awaitLine waits for the program to print something.
func awaitLine(ctx context.Context, p *runner.Process, want string, within time.Duration) error {
	return waitFor(ctx, fmt.Sprintf("the program to print %q", want), within, func(ctx context.Context) (bool, error) {
		if strings.Contains(p.Stdout(), want) {
			return true, nil
		}
		if done, res := p.Exited(); done {
			return false, fmt.Errorf("the program exited (%d) before printing it:\n%s", res.ExitCode, tail(res.Stderr))
		}
		return false, nil
	})
}

func waitFor(ctx context.Context, what string, within time.Duration, cond wait.ConditionWithContextFunc) error {
	ctx, cancel := context.WithTimeout(ctx, within)
	defer cancel()
	if err := kube.WaitFor(ctx, what, cond); err != nil {
		return err
	}
	return nil
}

// tail keeps an error message readable when the program has been talkative.
func tail(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > 12 {
		lines = append([]string{"  …"}, lines[len(lines)-12:]...)
	}
	for i, l := range lines {
		if !strings.HasPrefix(l, "  ") {
			lines[i] = "  " + l
		}
	}
	return strings.Join(lines, "\n")
}
