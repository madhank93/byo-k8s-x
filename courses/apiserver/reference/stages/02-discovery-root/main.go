// Your API server.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", "127.0.0.1:8080", "the address to serve the API on")
	flag.Parse()

	mux := http.NewServeMux()

	// The three endpoints that say the process is alive, reachable and willing.
	// The real server distinguishes them — livez is "restart me", readyz is
	// "send me traffic" — and answers each with a list of named checks.
	for _, path := range []string{"/healthz", "/livez", "/readyz"} {
		mux.HandleFunc("GET "+path, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			fmt.Fprintln(w, "ok")
		})
	}

	// Discovery: what this server can do, in the shape a client asks for it.
	// Every client starts here, kubectl included, and builds its map of what
	// exists from these three answers before it sends a single request.
	mux.HandleFunc("GET /api", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"kind":     "APIVersions",
			"versions": []string{"v1"},
		})
	})
	// The group-less core API is /api; everything else lives under /apis, in a
	// named group. There are none yet, and the empty list still has to be
	// there: a client that gets a 404 here decides the server is broken, not
	// that it has no groups.
	mux.HandleFunc("GET /apis", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"kind":       "APIGroupList",
			"apiVersion": "v1",
			"groups":     []any{},
		})
	})
	mux.HandleFunc("GET /api/v1", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"kind":         "APIResourceList",
			"apiVersion":   "v1",
			"groupVersion": "v1",
			"resources": []any{map[string]any{
				"name":         "configmaps",
				"singularName": "configmap",
				"namespaced":   true,
				"kind":         "ConfigMap",
				"verbs":        []string{"create", "delete", "get", "list", "update", "watch"},
				"shortNames":   []string{"cm"},
			}},
		})
	})

	// Anything this server does not serve is a 404 carrying a Status, not an
	// empty body: a client reads the reason out of it.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeStatus(w, http.StatusNotFound, "NotFound", "the server could not find the requested resource: "+r.URL.Path)
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()

	fmt.Println("serving on", *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve on %s: %w", *addr, err)
	}
	return nil
}

// writeJSON sends one object. The content type is not decoration: a client
// that asked for JSON and got text decides the server is broken.
func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		fmt.Fprintf(os.Stderr, "write response: %v\n", err)
	}
}

// writeStatus sends the object the API server answers every failure with.
//
// An error from this server is a resource like any other: kind Status, with a
// machine-readable reason and the same code as the HTTP status. It is what
// client-go turns back into a typed error, and what makes IsNotFound(err)
// possible on the other side.
func writeStatus(w http.ResponseWriter, code int, reason, message string) {
	writeJSON(w, code, map[string]any{
		"kind":       "Status",
		"apiVersion": "v1",
		"metadata":   map[string]any{},
		"status":     "Failure",
		"message":    message,
		"reason":     reason,
		"code":       code,
	})
}
