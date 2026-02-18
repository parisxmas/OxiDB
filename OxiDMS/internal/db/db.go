package db

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

// Pool is a round-robin connection pool for OxiDB with auto-reconnect.
type Pool struct {
	host    string
	port    int
	clients []*oxidb.Client
	mu      []sync.Mutex
	idx     uint64
	stop    chan struct{}
}

// NewPool creates a pool of n OxiDB connections.
func NewPool(host string, port, size int) (*Pool, error) {
	p := &Pool{
		host:    host,
		port:    port,
		clients: make([]*oxidb.Client, size),
		mu:      make([]sync.Mutex, size),
		stop:    make(chan struct{}),
	}
	for i := 0; i < size; i++ {
		c, err := oxidb.Connect(host, port, 5*time.Second)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("pool: connect client %d: %w", i, err)
		}
		p.clients[i] = c
	}
	// Start keepalive pings every 10 seconds to prevent idle timeout
	go p.keepalive()
	return p, nil
}

// Get returns the next client in round-robin order, reconnecting if needed.
func (p *Pool) Get() *oxidb.Client {
	n := atomic.AddUint64(&p.idx, 1)
	i := n % uint64(len(p.clients))
	return p.clients[i]
}

// Reconnect replaces a broken client at index i.
func (p *Pool) reconnect(i int) {
	p.mu[i].Lock()
	defer p.mu[i].Unlock()
	if p.clients[i] != nil {
		p.clients[i].Close()
	}
	c, err := oxidb.Connect(p.host, p.port, 5*time.Second)
	if err != nil {
		log.Printf("pool: reconnect client %d failed: %v", i, err)
		return
	}
	p.clients[i] = c
}

func (p *Pool) keepalive() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			for i := range p.clients {
				if _, err := p.clients[i].Ping(); err != nil {
					log.Printf("pool: client %d ping failed, reconnecting: %v", i, err)
					p.reconnect(i)
				}
			}
		}
	}
}

// Close closes all connections.
func (p *Pool) Close() {
	close(p.stop)
	for _, c := range p.clients {
		if c != nil {
			c.Close()
		}
	}
}
