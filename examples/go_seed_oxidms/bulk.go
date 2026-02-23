// Bulk insert 1M submissions directly into OxiDB, bypassing the HTTP API.
// Uses InsertMany with concurrent connections, auto-reconnect on failure.
package main

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

const (
	host       = "127.0.0.1"
	port       = 4444
	collection = "_dms_submissions"
	formID     = "1"
	total      = 368_000
	batchSize  = 500
	workers    = 6
)

func genSubmission(rng *rand.Rand) map[string]any {
	first := firstNames[rng.Intn(len(firstNames))]
	last := lastNames[rng.Intn(len(lastNames))]

	nWords := 15 + rng.Intn(36)
	words := make([]string, nWords)
	for i := range words {
		words[i] = loremWords[rng.Intn(len(loremWords))]
	}
	desc := strings.Join(words, " ")

	nWords2 := 20 + rng.Intn(61)
	words2 := make([]string, nWords2)
	for i := range words2 {
		words2[i] = loremWords[rng.Intn(len(loremWords))]
	}
	notes := strings.Join(words2, " ")

	nTags := 2 + rng.Intn(3)
	tagList := make([]string, nTags)
	for i := range tagList {
		tagList[i] = tags[rng.Intn(len(tags))]
	}

	startDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(rng.Int63n(int64(365 * 3 * 24 * time.Hour))))
	joinDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(rng.Int63n(int64(365 * 6 * 24 * time.Hour))))
	now := time.Now().UTC().Format(time.RFC3339)

	return map[string]any{
		"formId": formID,
		"data": map[string]any{
			"full_name":     first + " " + last,
			"description":   desc,
			"age":           rng.Intn(50) + 18,
			"contact_email": fmt.Sprintf("%s.%s%d@example.com", strings.ToLower(first), strings.ToLower(last), rng.Intn(1000)),
			"start_date":    startDate.Format("2006-01-02"),
			"department":    departments[rng.Intn(len(departments))],
			"is_active":     rng.Intn(2) == 1,
			"priority":      priorities[rng.Intn(len(priorities))],
			"city":          cities[rng.Intn(len(cities))],
			"phone":         fmt.Sprintf("+1-%03d-%03d-%04d", rng.Intn(900)+100, rng.Intn(900)+100, rng.Intn(10000)),
			"salary":        float64(rng.Intn(150000)+30000) + float64(rng.Intn(100))/100.0,
			"notes":         notes,
			"status":        statuses[rng.Intn(len(statuses))],
			"category":      categories[rng.Intn(len(categories))],
			"score":         float64(rng.Intn(100)) + float64(rng.Intn(100))/100.0,
			"join_date":     joinDate.Format("2006-01-02"),
			"tags":          strings.Join(tagList, ", "),
			"approved":      rng.Intn(3) != 0,
			"experience":    []string{"Junior", "Mid", "Senior", "Lead"}[rng.Intn(4)],
		},
		"createdBy": "1",
		"createdAt": now,
		"updatedAt": now,
	}
}

func connect() *oxidb.Client {
	for {
		c, err := oxidb.Connect(host, port, 15*time.Second)
		if err != nil {
			fmt.Printf("  reconnect failed: %v, retrying in 2s...\n", err)
			time.Sleep(2 * time.Second)
			continue
		}
		return c
	}
}

func insertWithRetry(c **oxidb.Client, docs []map[string]any, wid int) {
	for attempt := 0; attempt < 5; attempt++ {
		_, err := (*c).InsertMany(collection, docs)
		if err == nil {
			return
		}
		fmt.Printf("  [w%d] error (attempt %d): %v — reconnecting\n", wid, attempt+1, err)
		(*c).Close()
		time.Sleep(time.Duration(500*(attempt+1)) * time.Millisecond)
		*c = connect()
	}
	fmt.Printf("  [w%d] WARN: batch dropped after 5 retries\n", wid)
}

func bulkMain() {
	fmt.Printf("Bulk insert: %d submissions, batch=%d, workers=%d\n", total, batchSize, workers)
	fmt.Printf("Target: OxiDB at %s:%d, collection=%s\n\n", host, port, collection)

	// Pre-connect all workers
	clients := make([]*oxidb.Client, workers)
	for i := range clients {
		clients[i] = connect()
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	pong, _ := clients[0].Ping()
	fmt.Printf("Connected (%s). Starting insert...\n\n", pong)

	var inserted atomic.Int64
	batches := total / batchSize
	batchCh := make(chan int, batches)
	for i := 0; i < batches; i++ {
		batchCh <- i
	}
	close(batchCh)

	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			c := clients[wid]
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(wid)*9999))

			for range batchCh {
				docs := make([]map[string]any, batchSize)
				for j := range docs {
					docs[j] = genSubmission(rng)
				}
				insertWithRetry(&c, docs, wid)
				clients[wid] = c // update in case reconnected

				n := inserted.Add(int64(batchSize))
				if n%50_000 == 0 {
					elapsed := time.Since(start).Seconds()
					rate := float64(n) / elapsed
					eta := time.Duration(float64(total-int(n))/rate) * time.Second
					fmt.Printf("  %7d / %d  (%5.0f/sec)  elapsed %s  ETA %s\n",
						n, total, rate,
						time.Since(start).Round(time.Second),
						eta.Round(time.Second))
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("\nDone! Inserted %d submissions in %s (%.0f/sec)\n",
		inserted.Load(), elapsed.Round(time.Millisecond), float64(inserted.Load())/elapsed.Seconds())
}
