// ShopEdge API — Go HTTP server fronting the sharded + replicated OxiDB cluster.
//
// All data traffic flows through the top-level oxipool router (pool-router:4445),
// which CRC32-hashes the configured shard key (customer_id for orders/carts/events)
// onto one of three per-shard pools, each of which routes writes → master and
// reads → replica round-robin.
//
// Diagnostic endpoints (/api/raft/metrics, /api/health) bypass the router and
// connect directly to each db-XN node to observe the underlying state.

package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	listen := envOr("API_LISTEN", "0.0.0.0:8080")
	routerHost := envOr("OXIDB_ROUTER_HOST", "pool-router")
	routerPort, _ := strconv.Atoi(envOr("OXIDB_ROUTER_PORT", "4445"))
	directNodesStr := envOr("OXIDB_DIRECT_NODES",
		"db-a0:4444,db-a1:4444,db-a2:4444,db-b0:4444,db-b1:4444,db-b2:4444,db-c0:4444,db-c1:4444,db-c2:4444")

	directNodes := parseHostPorts(directNodesStr)

	srv := NewServer(routerHost, routerPort, directNodes)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	httpSrv := &http.Server{
		Addr:              listen,
		Handler:           withLogging(mux),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("ShopEdge API listening on %s", listen)
	log.Printf("  router  → %s:%d", routerHost, routerPort)
	log.Printf("  direct  → %d nodes", len(directNodes))
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseHostPorts(s string) []HostPort {
	var out []HostPort
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		host, port, ok := strings.Cut(part, ":")
		if !ok {
			continue
		}
		p, err := strconv.Atoi(port)
		if err != nil {
			continue
		}
		out = append(out, HostPort{Host: host, Port: p})
	}
	return out
}

func withLogging(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: 200}
		h.ServeHTTP(rec, r)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, rec.code, time.Since(start))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.code = code
	s.ResponseWriter.WriteHeader(code)
}
