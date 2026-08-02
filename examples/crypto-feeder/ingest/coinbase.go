package main

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

// Coinbase level2_batch channel — snapshot + incremental updates.
// URL: wss://ws-feed.exchange.coinbase.com

var coinbasePairs = map[string]string{
	"btcusdt": "BTC-USD",
	"ethusdt": "ETH-USD",
	"solusdt": "SOL-USD",
	"xrpusdt": "XRP-USD",
	"dogeusdt":  "DOGE-USD",
	"trumpusdt": "TRUMP-USD",
	// BNB not available on Coinbase
}

type coinbaseSubscribe struct {
	Type       string   `json:"type"`
	ProductIDs []string `json:"product_ids"`
	Channels   []string `json:"channels"`
}

type coinbaseMsg struct {
	Type      string          `json:"type"`
	ProductID string          `json:"product_id"`
	Bids      [][]string      `json:"bids,omitempty"`
	Asks      [][]string      `json:"asks,omitempty"`
	Changes   [][]string      `json:"changes,omitempty"`
}

func RunCoinbase(out chan<- OrderBook, done <-chan struct{}) {
	const wsURL = "wss://ws-feed.exchange.coinbase.com"

	// Reverse map: product_id → normalized pair
	productToPair := make(map[string]string)
	productIDs := make([]string, 0, len(coinbasePairs))
	for pair, product := range coinbasePairs {
		productToPair[product] = pair
		productIDs = append(productIDs, product)
	}

	// One order book per product
	books := make(map[string]*Book)
	for product := range productToPair {
		books[product] = NewBook()
	}

	backoff := time.Second
	for {
		select {
		case <-done:
			return
		default:
		}

		log.Printf("[coinbase] connecting to WebSocket...")
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("[coinbase] connect error: %v, retrying in %v", err, backoff)
			time.Sleep(backoff)
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		log.Printf("[coinbase] connected")

		// Reset books on reconnect
		for _, b := range books {
			b.Reset()
		}

		// Subscribe
		sub := coinbaseSubscribe{
			Type:       "subscribe",
			ProductIDs: productIDs,
			Channels:   []string{"level2_batch"},
		}
		if err := conn.WriteJSON(sub); err != nil {
			log.Printf("[coinbase] subscribe error: %v", err)
			conn.Close()
			continue
		}

		func() {
			defer conn.Close()
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			conn.SetPongHandler(func(string) error {
				conn.SetReadDeadline(time.Now().Add(30 * time.Second))
				return nil
			})

			for {
				select {
				case <-done:
					return
				default:
				}

				_, raw, err := conn.ReadMessage()
				if err != nil {
					log.Printf("[coinbase] read error: %v, reconnecting...", err)
					return
				}
				conn.SetReadDeadline(time.Now().Add(30 * time.Second))

				var msg coinbaseMsg
				if err := json.Unmarshal(raw, &msg); err != nil {
					continue
				}

				pair, ok := productToPair[msg.ProductID]
				if !ok {
					continue
				}
				book := books[msg.ProductID]

				switch msg.Type {
				case "snapshot":
					bids := parseCoinbaseLevels(msg.Bids)
					asks := parseCoinbaseLevels(msg.Asks)
					book.SetBids(bids)
					book.SetAsks(asks)
					emitCoinbase(book, pair, out)

				case "l2update":
					var bidUpdates, askUpdates []Level
					for _, change := range msg.Changes {
						if len(change) < 3 {
							continue
						}
						price, _ := strconv.ParseFloat(change[1], 64)
						qty, _ := strconv.ParseFloat(change[2], 64)
						l := Level{Price: price, Qty: qty}
						if change[0] == "buy" {
							bidUpdates = append(bidUpdates, l)
						} else {
							askUpdates = append(askUpdates, l)
						}
					}
					if len(bidUpdates) > 0 {
						book.UpdateBids(bidUpdates)
					}
					if len(askUpdates) > 0 {
						book.UpdateAsks(askUpdates)
					}
					emitCoinbase(book, pair, out)
				}
			}
		}()
	}
}

func parseCoinbaseLevels(raw [][]string) []Level {
	levels := make([]Level, 0, len(raw))
	for _, r := range raw {
		if len(r) < 2 {
			continue
		}
		price, _ := strconv.ParseFloat(r[0], 64)
		qty, _ := strconv.ParseFloat(r[1], 64)
		levels = append(levels, Level{Price: price, Qty: qty})
	}
	return levels
}

func emitCoinbase(book *Book, pair string, out chan<- OrderBook) {
	bids, asks := book.Snapshot(20)
	ob := OrderBook{
		Exchange:  "coinbase",
		Pair:      pair,
		Timestamp: time.Now().UnixMilli(),
		Bids:      bids,
		Asks:      asks,
	}
	select {
	case out <- ob:
	default:
	}
}
