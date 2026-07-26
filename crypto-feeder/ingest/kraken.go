package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

// Kraken v2 WebSocket — book channel with snapshot + incremental updates.
// URL: wss://ws.kraken.com/v2

var krakenPairs = map[string]string{
	"btcusdt": "BTC/USD",
	"ethusdt": "ETH/USD",
	"solusdt": "SOL/USD",
	"xrpusdt": "XRP/USD",
	"dogeusdt":  "DOGE/USD",
	"trumpusdt": "TRUMP/USD",
	// BNB not available on Kraken
}

type krakenSubscribe struct {
	Method string             `json:"method"`
	Params krakenSubscribeParams `json:"params"`
}

type krakenSubscribeParams struct {
	Channel string   `json:"channel"`
	Depth   int      `json:"depth"`
	Symbol  []string `json:"symbol"`
}

type krakenMsg struct {
	Channel string            `json:"channel"`
	Type    string            `json:"type"`
	Data    []krakenBookEntry `json:"data"`
}

type krakenBookEntry struct {
	Symbol string        `json:"symbol"`
	Bids   []krakenLevel `json:"bids"`
	Asks   []krakenLevel `json:"asks"`
}

type krakenLevel struct {
	Price float64 `json:"price"`
	Qty   float64 `json:"qty"`
}

func RunKraken(out chan<- OrderBook, done <-chan struct{}) {
	const wsURL = "wss://ws.kraken.com/v2"

	// Reverse map: kraken symbol → normalized pair
	symbolToPair := make(map[string]string)
	symbols := make([]string, 0, len(krakenPairs))
	for pair, sym := range krakenPairs {
		symbolToPair[sym] = pair
		symbols = append(symbols, sym)
	}

	// One order book per symbol
	books := make(map[string]*Book)
	for sym := range symbolToPair {
		books[sym] = NewBook()
	}

	backoff := time.Second
	for {
		select {
		case <-done:
			return
		default:
		}

		log.Printf("[kraken] connecting to WebSocket...")
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("[kraken] connect error: %v, retrying in %v", err, backoff)
			time.Sleep(backoff)
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		log.Printf("[kraken] connected")

		for _, b := range books {
			b.Reset()
		}

		sub := krakenSubscribe{
			Method: "subscribe",
			Params: krakenSubscribeParams{
				Channel: "book",
				Depth:   25,
				Symbol:  symbols,
			},
		}
		if err := conn.WriteJSON(sub); err != nil {
			log.Printf("[kraken] subscribe error: %v", err)
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
					log.Printf("[kraken] read error: %v, reconnecting...", err)
					return
				}
				conn.SetReadDeadline(time.Now().Add(30 * time.Second))

				var msg krakenMsg
				if err := json.Unmarshal(raw, &msg); err != nil {
					continue
				}
				if msg.Channel != "book" {
					continue
				}

				for _, entry := range msg.Data {
					pair, ok := symbolToPair[entry.Symbol]
					if !ok {
						continue
					}
					book := books[entry.Symbol]
					bids := krakenToLevels(entry.Bids)
					asks := krakenToLevels(entry.Asks)

					switch msg.Type {
					case "snapshot":
						book.SetBids(bids)
						book.SetAsks(asks)
					case "update":
						book.UpdateBids(bids)
						book.UpdateAsks(asks)
					default:
						continue
					}

					snapBids, snapAsks := book.Snapshot(20)
					ob := OrderBook{
						Exchange:  "kraken",
						Pair:      pair,
						Timestamp: time.Now().UnixMilli(),
						Bids:      snapBids,
						Asks:      snapAsks,
					}
					select {
					case out <- ob:
					default:
					}
				}
			}
		}()
	}
}

func krakenToLevels(kl []krakenLevel) []Level {
	levels := make([]Level, len(kl))
	for i, l := range kl {
		levels[i] = Level{Price: l.Price, Qty: l.Qty}
	}
	return levels
}
