package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

// HostPort is a "host:port" pair.
type HostPort struct {
	Host string
	Port int
}

func (h HostPort) String() string { return fmt.Sprintf("%s:%d", h.Host, h.Port) }

// Server holds shared state for the API.
type Server struct {
	RouterHost  string
	RouterPort  int
	DirectNodes []HostPort
}

func NewServer(routerHost string, routerPort int, directNodes []HostPort) *Server {
	return &Server{
		RouterHost:  routerHost,
		RouterPort:  routerPort,
		DirectNodes: directNodes,
	}
}

// connect opens a fresh client connection to the top-level router.
// Caller MUST Close.
func (s *Server) connect() (*oxidb.Client, error) {
	return oxidb.Connect(s.RouterHost, s.RouterPort, 5*time.Second)
}

// connectDirect opens a fresh connection to a specific db-XN node, bypassing
// the router. Used for diagnostic endpoints only.
func (s *Server) connectDirect(hp HostPort) (*oxidb.Client, error) {
	return oxidb.Connect(hp.Host, hp.Port, 3*time.Second)
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/health", s.handleHealth)
	mux.HandleFunc("/api/topology", s.handleTopology)
	mux.HandleFunc("/api/raft/metrics", s.handleRaftMetrics)
	mux.HandleFunc("/api/seed", s.handleSeed)

	// Catalog (unsharded — products live on shard A)
	mux.HandleFunc("/api/products", s.handleProducts)

	// Sharded reads + writes
	mux.HandleFunc("/api/cart", s.handleCart)               // POST add line
	mux.HandleFunc("/api/cart/", s.handleCartByCustomer)    // GET /api/cart/:customer_id
	mux.HandleFunc("/api/checkout", s.handleCheckout)       // POST → TX
	mux.HandleFunc("/api/orders", s.handleOrdersScatter)    // GET (scatter-gather)
	mux.HandleFunc("/api/orders/", s.handleOrdersByCustomer) // GET /api/orders/:customer_id
}

// ─── Helpers ────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"ok": false, "error": msg})
}

func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}

// pathTail returns the last path component after `/api/foo/`.
// pathTail("/api/cart/42", "/api/cart/") → "42"
func pathTail(path, prefix string) string {
	tail := strings.TrimPrefix(path, prefix)
	if i := strings.Index(tail, "/"); i >= 0 {
		tail = tail[:i]
	}
	return tail
}
