package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// Binance partial book depth stream — top 20 levels every 100ms.
// Combined stream: wss://stream.binance.com:9443/stream?streams=btcusdt@depth20@100ms/...

var binancePairs = map[string]string{
	"btcusdt": "btcusdt",
	"ethusdt": "ethusdt",
	"solusdt": "solusdt",
	"xrpusdt": "xrpusdt",
	"bnbusdt":  "bnbusdt",
	"dogeusdt":  "dogeusdt",
	"trumpusdt": "trumpusdt",
}

type binanceCombinedMsg struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type binanceDepth struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

func parseBinanceLevels(raw [][]string) []Level {
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

func RunBinance(out chan<- OrderBook, done <-chan struct{}) {
	// Build combined stream URL
	streams := make([]string, 0, len(binancePairs))
	for _, sym := range binancePairs {
		streams = append(streams, fmt.Sprintf("%s@depth20@100ms", sym))
	}
	url := fmt.Sprintf("wss://stream.binance.com:9443/stream?streams=%s", strings.Join(streams, "/"))

	// Reverse map: stream name → normalized pair
	streamToPair := make(map[string]string)
	for pair, sym := range binancePairs {
		streamToPair[fmt.Sprintf("%s@depth20@100ms", sym)] = pair
	}

	backoff := time.Second
	for {
		select {
		case <-done:
			return
		default:
		}

		log.Printf("[binance] connecting to WebSocket...")
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			log.Printf("[binance] connect error: %v, retrying in %v", err, backoff)
			time.Sleep(backoff)
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		log.Printf("[binance] connected")

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

				_, msg, err := conn.ReadMessage()
				if err != nil {
					log.Printf("[binance] read error: %v, reconnecting...", err)
					return
				}
				conn.SetReadDeadline(time.Now().Add(30 * time.Second))

				var combined binanceCombinedMsg
				if err := json.Unmarshal(msg, &combined); err != nil {
					continue
				}

				pair, ok := streamToPair[combined.Stream]
				if !ok {
					continue
				}

				var depth binanceDepth
				if err := json.Unmarshal(combined.Data, &depth); err != nil {
					continue
				}

				ob := OrderBook{
					Exchange:  "binance",
					Pair:      pair,
					Timestamp: time.Now().UnixMilli(),
					Bids:      parseBinanceLevels(depth.Bids),
					Asks:      parseBinanceLevels(depth.Asks),
				}

				select {
				case out <- ob:
				default:
					// drop if channel is full
				}
			}
		}()
	}
}
