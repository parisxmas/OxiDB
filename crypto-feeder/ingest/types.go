package main

import (
	"sort"
	"sync"
)

type Level struct {
	Price float64 `json:"price"`
	Qty   float64 `json:"qty"`
}

type OrderBook struct {
	Exchange  string  `json:"exchange"`
	Pair      string  `json:"pair"`
	Timestamp int64   `json:"timestamp"`
	Bids      []Level `json:"bids"`
	Asks      []Level `json:"asks"`
}

// Book maintains a local order book with thread-safe updates.
type Book struct {
	mu   sync.Mutex
	bids map[float64]float64
	asks map[float64]float64
}

func NewBook() *Book {
	return &Book{
		bids: make(map[float64]float64),
		asks: make(map[float64]float64),
	}
}

func (b *Book) Reset() {
	b.mu.Lock()
	b.bids = make(map[float64]float64)
	b.asks = make(map[float64]float64)
	b.mu.Unlock()
}

func (b *Book) SetBids(levels []Level) {
	b.mu.Lock()
	b.bids = make(map[float64]float64, len(levels))
	for _, l := range levels {
		b.bids[l.Price] = l.Qty
	}
	b.mu.Unlock()
}

func (b *Book) SetAsks(levels []Level) {
	b.mu.Lock()
	b.asks = make(map[float64]float64, len(levels))
	for _, l := range levels {
		b.asks[l.Price] = l.Qty
	}
	b.mu.Unlock()
}

func (b *Book) UpdateBids(levels []Level) {
	b.mu.Lock()
	for _, l := range levels {
		if l.Qty == 0 {
			delete(b.bids, l.Price)
		} else {
			b.bids[l.Price] = l.Qty
		}
	}
	b.mu.Unlock()
}

func (b *Book) UpdateAsks(levels []Level) {
	b.mu.Lock()
	for _, l := range levels {
		if l.Qty == 0 {
			delete(b.asks, l.Price)
		} else {
			b.asks[l.Price] = l.Qty
		}
	}
	b.mu.Unlock()
}

// Snapshot returns top N bids (desc) and asks (asc).
func (b *Book) Snapshot(depth int) (bids, asks []Level) {
	b.mu.Lock()
	defer b.mu.Unlock()

	bids = make([]Level, 0, len(b.bids))
	for p, q := range b.bids {
		bids = append(bids, Level{Price: p, Qty: q})
	}
	sort.Slice(bids, func(i, j int) bool { return bids[i].Price > bids[j].Price })
	if len(bids) > depth {
		bids = bids[:depth]
	}

	asks = make([]Level, 0, len(b.asks))
	for p, q := range b.asks {
		asks = append(asks, Level{Price: p, Qty: q})
	}
	sort.Slice(asks, func(i, j int) bool { return asks[i].Price < asks[j].Price })
	if len(asks) > depth {
		asks = asks[:depth]
	}
	return
}
