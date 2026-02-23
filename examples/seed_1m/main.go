package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// OxiDB wire protocol: 4-byte LE length + JSON payload

func send(conn net.Conn, msg map[string]interface{}) (map[string]interface{}, error) {
	data, _ := json.Marshal(msg)
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := conn.Write(lenBuf); err != nil {
		return nil, err
	}
	if _, err := conn.Write(data); err != nil {
		return nil, err
	}

	// Read response
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, err
	}
	respLen := binary.LittleEndian.Uint32(lenBuf)
	respBuf := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		return nil, err
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(respBuf, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal: %w (raw: %s)", err, string(respBuf[:min(200, len(respBuf))]))
	}
	return resp, nil
}

var (
	firstNames = []string{
		"James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
		"William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
		"Thomas", "Sarah", "Charles", "Karen", "Christopher", "Lisa", "Daniel", "Nancy",
		"Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
		"Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
		"Kenneth", "Carol", "Kevin", "Amanda", "Brian", "Dorothy", "George", "Melissa",
		"Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
		"Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
		"Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna", "Stephen", "Brenda",
		"Larry", "Pamela", "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen",
		"Benjamin", "Samantha", "Samuel", "Katherine", "Raymond", "Christine", "Gregory", "Debra",
		"Frank", "Rachel", "Alexander", "Carolyn", "Patrick", "Janet", "Jack", "Catherine",
	}
	lastNames = []string{
		"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
		"Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
		"Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
		"Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young",
		"Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
		"Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
		"Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
		"Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy",
		"Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson", "Bailey",
		"Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
		"Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza",
		"Ruiz", "Hughes", "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers",
	}
	departments = []string{"Engineering", "Marketing", "Sales", "HR", "Finance", "Legal", "Operations", "Support"}
	priorities  = []string{"Low", "Medium", "High", "Critical"}
	statuses    = []string{"New", "In Progress", "Under Review", "Completed"}
	categories  = []string{"Bug Report", "Feature Request", "Improvement", "Documentation", "Security"}
	experiences = []string{"Junior", "Mid", "Senior", "Lead"}
	cities      = []string{
		"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
		"San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
		"Fort Worth", "Columbus", "Charlotte", "San Francisco", "Indianapolis", "Seattle",
		"Denver", "Washington", "Nashville", "Oklahoma City", "El Paso", "Boston",
		"Portland", "Las Vegas", "Memphis", "Louisville", "Baltimore", "Milwaukee",
		"Albuquerque", "Tucson", "Fresno", "Mesa", "Sacramento", "Atlanta", "Miami",
		"Raleigh", "Omaha", "Minneapolis", "Cleveland", "Tampa", "Detroit", "Pittsburgh",
	}
	tags = []string{
		"frontend", "backend", "devops", "security", "database", "mobile", "cloud",
		"testing", "automation", "analytics", "ml", "design", "documentation", "api",
		"infrastructure", "performance", "reliability", "monitoring", "ci-cd", "networking",
	}
	descriptions = []string{
		"Experienced professional with strong analytical skills and a passion for innovation.",
		"Team player focused on delivering high-quality results under tight deadlines.",
		"Self-motivated individual with expertise in cross-functional collaboration.",
		"Detail-oriented worker with a track record of exceeding performance targets.",
		"Creative problem solver who thrives in fast-paced environments.",
		"Dedicated contributor with deep domain expertise and leadership potential.",
		"Results-driven professional committed to continuous improvement and learning.",
		"Strategic thinker with excellent communication and project management skills.",
	}
	notes = []string{
		"Needs follow-up meeting with team lead.",
		"Outstanding performance in last quarter review.",
		"Recommended for advanced training program.",
		"Transferred from another department recently.",
		"Working on high-priority project Alpha.",
		"Mentoring two junior team members.",
		"Remote worker - timezone GMT+1.",
		"Participating in company hackathon.",
		"On track for mid-year promotion.",
		"Contributing to open source initiatives.",
	}
)

func genRecord(rng *rand.Rand, id int) map[string]interface{} {
	fn := firstNames[rng.Intn(len(firstNames))]
	ln := lastNames[rng.Intn(len(lastNames))]
	dept := departments[rng.Intn(len(departments))]
	age := 22 + rng.Intn(43)
	city := cities[rng.Intn(len(cities))]

	startDate := time.Date(2020+rng.Intn(7), time.Month(1+rng.Intn(12)), 1+rng.Intn(28), 0, 0, 0, 0, time.UTC)
	joinDate := startDate.AddDate(0, -rng.Intn(24), 0)

	nTags := 1 + rng.Intn(4)
	tagList := ""
	for i := 0; i < nTags; i++ {
		if i > 0 {
			tagList += ", "
		}
		tagList += tags[rng.Intn(len(tags))]
	}

	return map[string]interface{}{
		"full_name":     fmt.Sprintf("%s %s", fn, ln),
		"description":   descriptions[rng.Intn(len(descriptions))],
		"age":           age,
		"contact_email": fmt.Sprintf("%s.%s%d@company.com", toLowerCase(fn), toLowerCase(ln), id%1000),
		"start_date":    startDate.Format("2006-01-02"),
		"department":    dept,
		"is_active":     rng.Float32() < 0.85,
		"priority":      priorities[rng.Intn(len(priorities))],
		"city":          city,
		"phone":         fmt.Sprintf("+1-%03d-%03d-%04d", 200+rng.Intn(800), rng.Intn(1000), rng.Intn(10000)),
		"salary":        40000 + rng.Intn(160000),
		"notes":         notes[rng.Intn(len(notes))],
		"status":        statuses[rng.Intn(len(statuses))],
		"category":      categories[rng.Intn(len(categories))],
		"score":         rng.Intn(101),
		"join_date":     joinDate.Format("2006-01-02"),
		"tags":          tagList,
		"approved":      rng.Float32() < 0.7,
		"experience":    experiences[rng.Intn(len(experiences))],
	}
}

func toLowerCase(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
}

func worker(addr string, formID string, startID, count int, inserted *int64, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect error: %v\n", err)
		return
	}
	defer conn.Close()

	rng := rand.New(rand.NewSource(int64(startID)))
	batchSize := 500
	collection := "_dms_submissions"

	for i := 0; i < count; i += batchSize {
		end := i + batchSize
		if end > count {
			end = count
		}

		docs := make([]interface{}, 0, end-i)
		for j := i; j < end; j++ {
			id := startID + j
			doc := map[string]interface{}{
				"formId":    formID,
				"data":      genRecord(rng, id),
				"files":     []string{},
				"createdBy": "1",
				"createdAt": time.Now().UTC().Format(time.RFC3339),
				"updatedAt": time.Now().UTC().Format(time.RFC3339),
			}
			docs = append(docs, doc)
		}

		msg := map[string]interface{}{
			"cmd":        "insert_many",
			"collection": collection,
			"docs":       docs,
		}

		resp, err := send(conn, msg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "send error at %d: %v, reconnecting...\n", startID+i, err)
			conn.Close()
			conn, err = net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				fmt.Fprintf(os.Stderr, "reconnect failed: %v\n", err)
				return
			}
			i -= batchSize // retry
			continue
		}
		if errMsg, ok := resp["error"]; ok {
			fmt.Fprintf(os.Stderr, "oxidb error at %d: %v\n", startID+i, errMsg)
			continue
		}

		atomic.AddInt64(inserted, int64(end-i))
	}
}

func main() {
	addr := "127.0.0.1:4444"
	if v := os.Getenv("OXIDB_ADDR"); v != "" {
		addr = v
	}

	total := 1_000_000
	numWorkers := 8
	formID := "1"

	fmt.Printf("Inserting %d records into form %s via %s (%d workers, batch=500)\n", total, formID, addr, numWorkers)

	perWorker := total / numWorkers
	var inserted int64
	var wg sync.WaitGroup

	start := time.Now()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		count := perWorker
		if w == numWorkers-1 {
			count = total - w*perWorker
		}
		go worker(addr, formID, w*perWorker, count, &inserted, &wg)
	}

	// Progress reporter
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n := atomic.LoadInt64(&inserted)
				elapsed := time.Since(start).Seconds()
				rate := float64(n) / elapsed
				pct := float64(n) / float64(total) * 100
				fmt.Printf("  %d / %d (%.1f%%) — %.0f docs/sec\n", n, total, pct, rate)
			case <-done:
				return
			}
		}
	}()

	wg.Wait()
	close(done)

	elapsed := time.Since(start)
	n := atomic.LoadInt64(&inserted)
	fmt.Printf("\nDone: %d records in %s (%.0f docs/sec)\n", n, elapsed.Round(time.Millisecond), float64(n)/elapsed.Seconds())
}
