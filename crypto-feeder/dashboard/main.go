package main

import (
	"embed"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gorilla/websocket"
)

//go:embed index.html
var staticFS embed.FS

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// All MQTT topics to subscribe to
var allTopics = []string{
	"crypto/orderbook/binance/btcusdt",
	"crypto/orderbook/binance/ethusdt",
	"crypto/orderbook/binance/solusdt",
	"crypto/orderbook/binance/xrpusdt",
	"crypto/orderbook/binance/bnbusdt",
	"crypto/orderbook/binance/dogeusdt",
	"crypto/orderbook/binance/trumpusdt",
	"crypto/orderbook/coinbase/btcusdt",
	"crypto/orderbook/coinbase/ethusdt",
	"crypto/orderbook/coinbase/solusdt",
	"crypto/orderbook/coinbase/xrpusdt",
	"crypto/orderbook/coinbase/dogeusdt",
	"crypto/orderbook/coinbase/trumpusdt",
	"crypto/orderbook/kraken/btcusdt",
	"crypto/orderbook/kraken/ethusdt",
	"crypto/orderbook/kraken/solusdt",
	"crypto/orderbook/kraken/xrpusdt",
	"crypto/orderbook/kraken/dogeusdt",
	"crypto/orderbook/kraken/trumpusdt",
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Hub struct {
	mu      sync.RWMutex
	clients map[*websocket.Conn]bool
}

func NewHub() *Hub {
	return &Hub{clients: make(map[*websocket.Conn]bool)}
}

func (h *Hub) Add(conn *websocket.Conn) {
	h.mu.Lock()
	h.clients[conn] = true
	h.mu.Unlock()
}

func (h *Hub) Remove(conn *websocket.Conn) {
	h.mu.Lock()
	delete(h.clients, conn)
	h.mu.Unlock()
}

func (h *Hub) Broadcast(data []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for conn := range h.clients {
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			conn.Close()
			go h.Remove(conn)
		}
	}
}

// queryOxiDB sends a command to OxiDB over TCP and returns the response.
func queryOxiDB(addr string, cmd map[string]interface{}) (json.RawMessage, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, uint32(len(data)))

	conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write(header); err != nil {
		return nil, err
	}
	if _, err := conn.Write(data); err != nil {
		return nil, err
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var respLen [4]byte
	if _, err := conn.Read(respLen[:]); err != nil {
		return nil, err
	}
	rLen := binary.LittleEndian.Uint32(respLen[:])
	if rLen == 0 || rLen > 4<<20 {
		return nil, fmt.Errorf("bad response length: %d", rLen)
	}

	buf := make([]byte, rLen)
	n := 0
	for n < int(rLen) {
		nn, err := conn.Read(buf[n:])
		if err != nil {
			return nil, err
		}
		n += nn
	}

	return json.RawMessage(buf[:n]), nil
}

func main() {
	mqttBroker := env("MQTT_BROKER", "tcp://127.0.0.1:1883")
	listenAddr := env("LISTEN_ADDR", ":3000")
	oxidbAddr := env("OXIDB_HOST", "127.0.0.1:4444")

	log.SetFlags(log.Ltime | log.Lmicroseconds)
	hub := NewHub()

	// Latest state per exchange/pair
	var stateMu sync.RWMutex
	state := make(map[string]json.RawMessage) // "exchange/pair" → last orderbook JSON

	// MQTT client
	opts := mqtt.NewClientOptions()
	opts.AddBroker(mqttBroker)
	opts.SetClientID(fmt.Sprintf("crypto-dashboard-%d", time.Now().UnixNano()%10000))
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(3 * time.Second)
	opts.SetKeepAlive(20 * time.Second)
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		log.Printf("[mqtt] connected, subscribing to %d topics", len(allTopics))
		for _, topic := range allTopics {
			c.Subscribe(topic, 0, func(_ mqtt.Client, msg mqtt.Message) {
				data := msg.Payload()

				// Parse to extract exchange/pair for state key
				var ob struct {
					Exchange string `json:"exchange"`
					Pair     string `json:"pair"`
				}
				if json.Unmarshal(data, &ob) == nil {
					key := ob.Exchange + "/" + ob.Pair
					stateMu.Lock()
					state[key] = json.RawMessage(data)
					stateMu.Unlock()
				}

				hub.Broadcast(data)
			})
		}
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		log.Printf("[mqtt] connection lost: %v", err)
	})

	client := mqtt.NewClient(opts)
	log.Printf("[mqtt] connecting to %s...", mqttBroker)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("[mqtt] connect failed: %v", token.Error())
	}

	// WebSocket endpoint
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		hub.Add(conn)
		log.Printf("[ws] client connected (%d total)", len(hub.clients))

		// Send current state snapshot
		stateMu.RLock()
		for _, data := range state {
			conn.WriteMessage(websocket.TextMessage, data)
		}
		stateMu.RUnlock()

		// Keep reading to detect disconnect
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				hub.Remove(conn)
				conn.Close()
				log.Printf("[ws] client disconnected")
				return
			}
		}
	})

	// Historical data from OxiDB
	http.HandleFunc("/api/history", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		exchange := r.URL.Query().Get("exchange")
		pair := r.URL.Query().Get("pair")
		if exchange == "" || pair == "" {
			http.Error(w, "exchange and pair required", 400)
			return
		}

		// Last 2 hours of data
		since := time.Now().UnixMilli() - 2*60*60*1000
		cmd := map[string]interface{}{
			"cmd":        "find",
			"collection": "orderbooks",
			"query": map[string]interface{}{
				"exchange": exchange,
				"pair":     pair,
				"timestamp": map[string]interface{}{
					"$gte": since,
				},
			},
			"sort":  map[string]interface{}{"timestamp": 1},
			"limit": 5000,
		}

		resp, err := queryOxiDB(oxidbAddr, cmd)
		if err != nil {
			log.Printf("[api/history] query error: %v", err)
			http.Error(w, "query failed", 500)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(resp)
	})

	// 24h OHLC data from OxiDB (downsampled 5-min candles)
	http.HandleFunc("/api/ohlc", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		exchange := r.URL.Query().Get("exchange")
		pair := r.URL.Query().Get("pair")
		if exchange == "" || pair == "" {
			http.Error(w, "exchange and pair required", 400)
			return
		}

		cmd := map[string]interface{}{
			"cmd":        "find",
			"collection": "orderbooks_history",
			"query": map[string]interface{}{
				"exchange": exchange,
				"pair":     pair,
			},
			"sort":  map[string]interface{}{"period_start": -1},
			"limit": 288, // 24h at 5-min intervals
		}

		resp, err := queryOxiDB(oxidbAddr, cmd)
		if err != nil {
			log.Printf("[api/ohlc] query error: %v", err)
			http.Error(w, "query failed", 500)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(resp)
	})

	// Serve dashboard HTML
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		data, _ := staticFS.ReadFile("index.html")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(data)
	})

	log.Printf("dashboard listening on %s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}
