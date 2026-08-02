package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	mqttBroker := env("MQTT_BROKER", "tcp://127.0.0.1:1883")
	oxidbAddr := env("OXIDB_HOST", "127.0.0.1:4444")
	exchange := env("EXCHANGE", "") // "" = all, or "binance", "coinbase", "kraken"
	allPairs := []string{"btcusdt", "ethusdt", "solusdt", "xrpusdt", "bnbusdt", "dogeusdt", "trumpusdt"}
	allExchanges := []string{"binance", "coinbase", "kraken"}

	log.SetFlags(log.Ltime | log.Lmicroseconds)
	if exchange != "" {
		log.Printf("exchange=%s mqtt=%s oxidb=%s", exchange, mqttBroker, oxidbAddr)
	} else {
		log.Printf("exchange=all mqtt=%s oxidb=%s", mqttBroker, oxidbAddr)
	}

	// MQTT client
	opts := mqtt.NewClientOptions()
	opts.AddBroker(mqttBroker)
	clientID := "crypto-ingest"
	if exchange != "" {
		clientID = "crypto-ingest-" + exchange
	}
	opts.SetClientID(clientID)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(3 * time.Second)
	opts.SetKeepAlive(20 * time.Second)
	opts.SetOnConnectHandler(func(_ mqtt.Client) {
		log.Printf("[mqtt] connected to %s", mqttBroker)
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		log.Printf("[mqtt] connection lost: %v", err)
	})

	client := mqtt.NewClient(opts)
	log.Printf("[mqtt] connecting...")
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("[mqtt] connect failed: %v", token.Error())
	}

	// OxiDB store for snapshots
	store := NewOxiDBStore(oxidbAddr)
	storeCh := make(chan OrderBook, 10000)

	// Channel for order book updates from all exchanges
	out := make(chan OrderBook, 10000)
	done := make(chan struct{})

	// OxiDB writer goroutine — decoupled from main loop
	go func() {
		for ob := range storeCh {
			store.Store(ob)
		}
	}()

	// Start downsampler (only from binance ingest or default mode to avoid duplicates)
	if exchange == "binance" || exchange == "" {
		go RunDownsampler(oxidbAddr, allExchanges, allPairs, done)
		log.Printf("[downsample] started (5-min OHLC aggregation)")
	}

	// Start exchange feeds
	switch exchange {
	case "binance":
		go RunBinance(out, done)
	case "coinbase":
		go RunCoinbase(out, done)
	case "kraken":
		go RunKraken(out, done)
	default:
		go RunBinance(out, done)
		go RunCoinbase(out, done)
		go RunKraken(out, done)
	}

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	var published, stored uint64

	// Stats ticker
	statsTicker := time.NewTicker(10 * time.Second)
	defer statsTicker.Stop()

	log.Printf("ingest running, publishing to MQTT and storing to OxiDB")

	for {
		select {
		case <-sig:
			log.Printf("shutting down... published=%d stored=%d", published, stored)
			close(done)
			close(storeCh)
			store.Flush()
			client.Disconnect(1000)
			return

		case ob := <-out:
			// Publish to MQTT
			topic := fmt.Sprintf("crypto/orderbook/%s/%s", ob.Exchange, ob.Pair)
			data, err := json.Marshal(ob)
			if err != nil {
				continue
			}
			client.Publish(topic, 0, false, data)
			published++

			// Store to OxiDB (non-blocking)
			select {
			case storeCh <- ob:
				stored++
			default:
				// drop if store channel full
			}

		case <-statsTicker.C:
			log.Printf("[stats] published=%d stored=%d", published, stored)
		}
	}
}
