// Insert 15M submissions into the Employee Survey form via direct OxiDB TCP protocol.
// Documents are stored as native JSON (JSONB) — each submission is a structured JSON
// object with typed fields (strings, numbers, booleans, dates).
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/clients/go/oxidb"
)

const (
	total      = 15_000_000
	batchSize  = 5000
	collection = "_dms_submissions"
	formID     = "1"
	createdBy  = "1"
	connCount  = 16
	host       = "localhost"
	port       = 4444
)

// ── name pools ──────────────────────────────────────────────────────────────

var firstNames = []string{
	"James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "David", "Elizabeth",
	"William", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Christopher", "Karen",
	"Charles", "Lisa", "Daniel", "Nancy", "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra",
	"Donald", "Ashley", "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
	"Kenneth", "Carol", "Kevin", "Amanda", "Brian", "Dorothy", "George", "Melissa", "Timothy", "Deborah",
	"Emma", "Olivia", "Ava", "Sophia", "Isabella", "Mia", "Charlotte", "Amelia", "Harper", "Evelyn",
	"Alexander", "Benjamin", "Ethan", "Henry", "Sebastian", "Jack", "Aiden", "Owen", "Samuel", "Ryan",
	"Nathan", "Leo", "Lucas", "Mason", "Logan", "Oliver", "Elijah", "Liam", "Noah", "Jacob",
	"Aria", "Chloe", "Penelope", "Layla", "Riley", "Zoey", "Nora", "Lily", "Eleanor", "Hannah",
	"Lillian", "Addison", "Aubrey", "Ellie", "Stella", "Natalie", "Zoe", "Leah", "Hazel", "Violet",
}

var lastNames = []string{
	"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
	"Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
	"Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
	"Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
	"Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts",
	"Chen", "Kumar", "Patel", "Singh", "Kim", "Park", "Tanaka", "Muller", "Schneider", "Fischer",
	"Yamamoto", "Sato", "Suzuki", "Watanabe", "Ito", "Nakamura", "Kobayashi", "Kato", "Fujita", "Okada",
	"Costa", "Santos", "Ferreira", "Oliveira", "Silva", "Almeida", "Souza", "Lima", "Gomes", "Ribeiro",
}

var departments = []string{
	"Engineering", "Marketing", "Sales", "Finance", "HR", "Operations", "Legal", "Support",
}
var priorities = []string{"Low", "Medium", "High", "Critical"}
var statuses = []string{"New", "In Progress", "Under Review", "Completed"}
var categories = []string{"Bug Report", "Feature Request", "Improvement", "Documentation", "Security"}
var experiences = []string{"Junior", "Mid", "Senior", "Lead"}
var cities = []string{
	"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
	"San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
	"Fort Worth", "Columbus", "Charlotte", "Indianapolis", "Seattle", "Denver",
	"Boston", "Nashville", "Portland", "Las Vegas", "Memphis", "Louisville",
	"Berlin", "Munich", "London", "Paris", "Tokyo", "Sydney", "Toronto", "Istanbul",
}
var domains = []string{"gmail.com", "outlook.com", "company.com", "work.org", "mail.io", "proton.me", "yahoo.com", "icloud.com"}
var tags = []string{
	"urgent", "backend", "frontend", "database", "performance", "ux",
	"security", "devops", "testing", "mobile", "api", "infra",
}
var bioSnippets = []string{
	"Passionate about building scalable systems and mentoring junior developers.",
	"Experienced professional with a track record of delivering high-impact projects.",
	"Detail-oriented team player who thrives in fast-paced environments.",
	"Creative problem solver with expertise in cross-functional collaboration.",
	"Results-driven individual focused on continuous improvement and innovation.",
	"Strong communicator with experience leading distributed teams across time zones.",
	"Dedicated to writing clean, maintainable code and fostering engineering culture.",
	"Enthusiastic about data-driven decision making and process optimization.",
	"Skilled in stakeholder management and strategic planning for growth initiatives.",
	"Committed to building inclusive teams and developing talent at every level.",
	"Background in both startups and enterprise, bringing versatile perspective.",
	"Advocate for test-driven development and continuous integration best practices.",
	"Enjoys tackling ambiguous problems and turning them into clear solutions.",
	"Focused on user experience and delivering value to customers efficiently.",
	"Lifelong learner with interests spanning technology, design, and leadership.",
	"Expert in cloud architecture and distributed systems at scale.",
}

func lower(s string) string {
	b := make([]byte, len(s))
	for i := range s {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 32
		}
		b[i] = c
	}
	return string(b)
}

func makeBatch(rng *rand.Rand, size int) []map[string]any {
	docs := make([]map[string]any, size)
	for i := 0; i < size; i++ {
		first := firstNames[rng.Intn(len(firstNames))]
		last := lastNames[rng.Intn(len(lastNames))]
		name := first + " " + last
		email := fmt.Sprintf("%s.%s%d@%s", lower(first), lower(last), rng.Intn(9999)+1, domains[rng.Intn(len(domains))])
		age := rng.Intn(44) + 22
		salary := float64((rng.Intn(63)+7)*5000) + float64(rng.Intn(100))/100
		dept := departments[rng.Intn(len(departments))]
		city := cities[rng.Intn(len(cities))]
		phone := fmt.Sprintf("+1-%03d-%03d-%04d", rng.Intn(900)+100, rng.Intn(900)+100, rng.Intn(10000))
		hireDate := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, rng.Intn(2900))
		startDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, rng.Intn(1000))
		joinDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, rng.Intn(2200))
		bio := fmt.Sprintf("%s — %s %s", name, bioSnippets[rng.Intn(len(bioSnippets))], bioSnippets[rng.Intn(len(bioSnippets))])
		score := float64(rng.Intn(100)) + float64(rng.Intn(100))/100
		nTags := 2 + rng.Intn(3)
		tagList := make([]string, nTags)
		for t := 0; t < nTags; t++ {
			tagList[t] = tags[rng.Intn(len(tags))]
		}
		now := time.Now().UTC().Format(time.RFC3339)

		// Each document is a full JSONB submission object with typed fields
		docs[i] = map[string]any{
			"formId": formID,
			"data": map[string]any{
				"full_name":     name,
				"description":   bio,
				"age":           age,
				"contact_email": email,
				"start_date":    startDate.Format("2006-01-02"),
				"department":    dept,
				"is_active":     rng.Intn(2) == 1,
				"priority":      priorities[rng.Intn(len(priorities))],
				"city":          city,
				"phone":         phone,
				"salary":        salary,
				"notes":         bio,
				"status":        statuses[rng.Intn(len(statuses))],
				"category":      categories[rng.Intn(len(categories))],
				"score":         score,
				"join_date":     joinDate.Format("2006-01-02"),
				"tags":          tagList,
				"approved":      rng.Intn(3) != 0,
				"experience":    experiences[rng.Intn(len(experiences))],
				"hire_date":     hireDate.Format("2006-01-02"),
			},
			"files":     []string{},
			"createdBy": createdBy,
			"createdAt": now,
			"updatedAt": now,
		}
	}
	return docs
}

func main() {
	fmt.Printf("Connecting %d clients to OxiDB at %s:%d...\n", connCount, host, port)

	clients := make([]*oxidb.Client, connCount)
	for i := 0; i < connCount; i++ {
		c, err := oxidb.Connect(host, port, 10*time.Second)
		if err != nil {
			fmt.Printf("Failed to connect client %d: %v\n", i, err)
			return
		}
		clients[i] = c
		defer c.Close()
	}

	// Test with a single insert
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	testBatch := makeBatch(rng, 1)
	if _, err := clients[0].InsertMany(collection, testBatch); err != nil {
		fmt.Printf("Test insert failed: %v\n", err)
		return
	}
	fmt.Println("Test insert OK — JSONB document stored successfully.")

	fmt.Printf("\nInserting %d submissions (JSONB) into Employee Survey (formId=%s)\n", total, formID)
	fmt.Printf("Batch size: %d | Connections: %d\n\n", batchSize, connCount)

	var inserted atomic.Int64
	inserted.Store(1) // count test insert
	var errors atomic.Int64
	start := time.Now()

	numBatches := (total - 1) / batchSize
	ch := make(chan int, connCount*2)

	var wg sync.WaitGroup
	for i := 0; i < connCount; i++ {
		wg.Add(1)
		go func(client *oxidb.Client, seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for range ch {
				batch := makeBatch(rng, batchSize)
				_, err := client.InsertMany(collection, batch)
				if err != nil {
					errors.Add(int64(batchSize))
					fmt.Printf("  ERROR: %v\n", err)
					continue
				}
				n := inserted.Add(int64(batchSize))
				if n%(500_000) < int64(batchSize) {
					elapsed := time.Since(start).Seconds()
					rate := float64(n) / elapsed
					pct := float64(n) / float64(total) * 100
					eta := float64(total-n) / rate
					fmt.Printf("  %12d / %d  (%5.1f%%)  |  %.0f rec/s  |  errors: %d  |  ETA: %.0fs\n",
						n, total, pct, rate, errors.Load(), eta)
				}
			}
		}(clients[i], time.Now().UnixNano()+int64(i)*99999)
	}

	for b := 0; b < numBatches; b++ {
		ch <- b
	}
	close(ch)
	wg.Wait()

	elapsed := time.Since(start).Seconds()
	ins := inserted.Load()
	errs := errors.Load()
	fmt.Printf("\nDone! %d submissions inserted in %.1fs (%.0f rec/s), %d errors.\n", ins, elapsed, float64(ins)/elapsed, errs)
}
