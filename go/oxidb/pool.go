package oxidb

import (
	"fmt"
	"sync"
	"time"
)

// pingTimeout bounds the liveness check on a pooled connection. Without a
// deadline, Ping on a server-reaped TCP conn that didn't get a clean FIN
// can hang for the OS keepalive interval (~2h on macOS/Linux). 2s is more
// than enough RTT for healthy local/LAN deployments and small enough that
// stale conns don't visibly stall application requests.
const pingTimeout = 2 * time.Second

// Pool is a connection pool for oxidb-server.
// Goroutines check out connections with Get() and return them with Put().
type Pool struct {
	conns   chan *Client
	host    string
	port    int
	timeout time.Duration
	size    int
	mu      sync.Mutex
	closed  bool

	// user/pass are non-empty when the pool was constructed via
	// NewPoolAuth. Every fresh connection — eager at NewPool time and
	// any later reconnect during checkout's stale-conn recovery —
	// AuthSimple's against these credentials before being used.
	user string
	pass string
}

// NewPool creates a connection pool with the given size.
// All connections are established eagerly during creation.
// Connections are anonymous — for an OxiDB with OXIDB_AUTH enabled,
// use NewPoolAuth instead.
func NewPool(host string, port int, size int, timeout time.Duration) (*Pool, error) {
	return newPool(host, port, size, timeout, "", "")
}

// NewPoolAuth is the SCRAM-style variant of NewPool. Every connection
// AuthSimple's with the given (user, pass) before being returned to
// the pool. Reconnects during stale-conn checkout re-authenticate
// transparently so callers don't need to handle auth state at the
// request layer.
//
// Empty user is invalid here — call NewPool for anonymous pools.
// Pass-through of the credentials means they live in the Pool struct
// for its lifetime; close the pool when done.
func NewPoolAuth(host string, port, size int, timeout time.Duration, user, pass string) (*Pool, error) {
	if user == "" {
		return nil, fmt.Errorf("oxidb pool: NewPoolAuth requires a non-empty user; use NewPool for anonymous")
	}
	return newPool(host, port, size, timeout, user, pass)
}

func newPool(host string, port int, size int, timeout time.Duration, user, pass string) (*Pool, error) {
	if size <= 0 {
		size = 4
	}
	p := &Pool{
		conns:   make(chan *Client, size),
		host:    host,
		port:    port,
		timeout: timeout,
		size:    size,
		user:    user,
		pass:    pass,
	}
	for i := 0; i < size; i++ {
		c, err := p.dial()
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("oxidb pool: connect %d/%d: %w", i+1, size, err)
		}
		p.conns <- c
	}
	return p, nil
}

// dial opens a fresh connection and authenticates it (if the pool
// holds credentials). Used by NewPool's eager init AND by checkout's
// stale-conn replacement path — the auth state survives both.
func (p *Pool) dial() (*Client, error) {
	c, err := Connect(p.host, p.port, p.timeout)
	if err != nil {
		return nil, err
	}
	if p.user != "" {
		if _, err := c.AuthSimple(p.user, p.pass); err != nil {
			c.Close()
			return nil, fmt.Errorf("auth: %w", err)
		}
	}
	return c, nil
}

// Get checks out a connection from the pool.
// Blocks until a connection is available.
func (p *Pool) Get() (*Client, error) {
	c, ok := <-p.conns
	if !ok {
		return nil, fmt.Errorf("oxidb pool: closed")
	}
	return p.checkout(c)
}

// GetTimeout checks out a connection with a timeout.
// Returns an error if no connection is available within the duration.
func (p *Pool) GetTimeout(d time.Duration) (*Client, error) {
	select {
	case c, ok := <-p.conns:
		if !ok {
			return nil, fmt.Errorf("oxidb pool: closed")
		}
		return p.checkout(c)
	case <-time.After(d):
		return nil, fmt.Errorf("oxidb pool: timeout waiting for connection")
	}
}

// checkout verifies the conn is alive (bounded by pingTimeout) and returns
// it, or transparently dials a fresh replacement if the conn was reaped by
// the server. The deadline is cleared on success so subsequent operations
// run without an inherited timeout.
func (p *Pool) checkout(c *Client) (*Client, error) {
	_ = c.SetDeadline(time.Now().Add(pingTimeout))
	_, err := c.Ping()
	if err == nil {
		_ = c.SetDeadline(time.Time{})
		return c, nil
	}
	c.Close()
	fresh, derr := p.dial()
	if derr != nil {
		// Return the (closed) conn to the channel so the slot is not leaked.
		// Without this, every failed checkout during an outage permanently
		// shrinks the pool: once all slots are consumed, Get() blocks forever
		// even after the backend recovers. The dead conn fails its next ping
		// instantly and triggers another dial attempt, so capacity is kept
		// and the pool self-heals as soon as the backend is reachable again.
		p.Put(c)
		return nil, fmt.Errorf("oxidb pool: reconnect: %w", derr)
	}
	return fresh, nil
}

// Put returns a connection to the pool.
// If the pool is full or closed, the connection is closed instead.
func (p *Pool) Put(c *Client) {
	if c == nil {
		return
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		c.Close()
		return
	}
	select {
	case p.conns <- c:
	default:
		c.Close()
	}
}

// WithConn checks out a connection, runs fn, and returns it to the pool.
// If fn returns an error, the connection is still returned (unless it's a network error).
func (p *Pool) WithConn(fn func(c *Client) error) error {
	c, err := p.Get()
	if err != nil {
		return err
	}
	err = fn(c)
	p.Put(c)
	return err
}

// Size returns the pool capacity.
func (p *Pool) Size() int {
	return p.size
}

// Available returns the number of idle connections in the pool.
func (p *Pool) Available() int {
	return len(p.conns)
}

// Close closes all connections and the pool.
func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()
	close(p.conns)
	for c := range p.conns {
		c.Close()
	}
}
