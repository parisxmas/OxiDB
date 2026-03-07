package oxidb

import (
	"fmt"
	"sync"
	"time"
)

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
}

// NewPool creates a connection pool with the given size.
// All connections are established eagerly during creation.
func NewPool(host string, port int, size int, timeout time.Duration) (*Pool, error) {
	if size <= 0 {
		size = 4
	}
	p := &Pool{
		conns:   make(chan *Client, size),
		host:    host,
		port:    port,
		timeout: timeout,
		size:    size,
	}
	for i := 0; i < size; i++ {
		c, err := Connect(host, port, timeout)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("oxidb pool: connect %d/%d: %w", i+1, size, err)
		}
		p.conns <- c
	}
	return p, nil
}

// Get checks out a connection from the pool.
// Blocks until a connection is available.
func (p *Pool) Get() (*Client, error) {
	c, ok := <-p.conns
	if !ok {
		return nil, fmt.Errorf("oxidb pool: closed")
	}
	// Verify the connection is alive
	if _, err := c.Ping(); err != nil {
		c.Close()
		// Create a fresh connection
		fresh, err := Connect(p.host, p.port, p.timeout)
		if err != nil {
			return nil, fmt.Errorf("oxidb pool: reconnect: %w", err)
		}
		return fresh, nil
	}
	return c, nil
}

// GetTimeout checks out a connection with a timeout.
// Returns an error if no connection is available within the duration.
func (p *Pool) GetTimeout(d time.Duration) (*Client, error) {
	select {
	case c, ok := <-p.conns:
		if !ok {
			return nil, fmt.Errorf("oxidb pool: closed")
		}
		if _, err := c.Ping(); err != nil {
			c.Close()
			fresh, err := Connect(p.host, p.port, p.timeout)
			if err != nil {
				return nil, fmt.Errorf("oxidb pool: reconnect: %w", err)
			}
			return fresh, nil
		}
		return c, nil
	case <-time.After(d):
		return nil, fmt.Errorf("oxidb pool: timeout waiting for connection")
	}
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
