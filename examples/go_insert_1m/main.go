package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parisxmas/OxiDB/go/oxidb"
)

const (
	total      = 15_000_000
	batchSize  = 5000
	collection = "_dms_submissions"
	formID     = "2"
	createdBy  = "2"
	connCount  = 16
	host       = "localhost"
	port       = 4444
)

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
	"Engineering", "Marketing", "Sales", "Finance", "HR", "Operations", "Legal", "Design",
	"Product", "Research", "Support", "Security", "DevOps", "QA", "Data Science", "Analytics",
}
var levels = []string{"Junior", "Mid", "Senior", "Lead", "Principal", "Staff", "Director", "VP"}
var domains = []string{"gmail.com", "outlook.com", "company.com", "work.org", "mail.io", "proton.me", "yahoo.com", "icloud.com"}
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
	"Pioneered several internal tools that improved team productivity by 40 percent.",
	"Regularly speaks at conferences about modern engineering practices.",
	"Contributed to open-source projects with thousands of GitHub stars.",
	"Holds multiple patents in machine learning and natural language processing.",
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
		salary := (rng.Intn(63) + 7) * 5000
		dept := departments[rng.Intn(len(departments))]
		level := levels[rng.Intn(len(levels))]
		remote := rng.Float64() < 0.4
		hireDate := time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, rng.Intn(5800))
		bio := fmt.Sprintf("%s — %s %s", name, bioSnippets[rng.Intn(len(bioSnippets))], bioSnippets[rng.Intn(len(bioSnippets))])
		now := time.Now().UTC().Format(time.RFC3339)

		docs[i] = map[string]any{
			"formId": formID,
			"data": map[string]any{
				"full_name":        name,
				"email_address":    email,
				"age":              age,
				"salary":           salary,
				"hire_date":        hireDate.Format("2006-01-02"),
				"department":       dept,
				"experience_level": level,
				"is_remote":        remote,
				"bio":              bio,
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
	fmt.Println("Test insert OK.")

	fmt.Printf("\nInserting %d records in batches of %d with %d connections...\n\n", total, batchSize, connCount)

	var inserted atomic.Int64
	inserted.Store(1) // count test insert
	var errors atomic.Int64
	start := time.Now()

	numBatches := (total - 1) / batchSize // -1 for test insert
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
				if n%(250_000) < int64(batchSize) {
					elapsed := time.Since(start).Seconds()
					rate := float64(n) / elapsed
					eta := float64(total-n) / rate
					fmt.Printf("  %10d / %d  |  %.0f rec/s  |  errors: %d  |  ETA: %.0fs\n",
						n, total, rate, errors.Load(), eta)
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
	fmt.Printf("\nBulk insert done in %.1fs. %d inserted, %d errors. (%.0f rec/s)\n", elapsed, ins, errs, float64(ins)/elapsed)

	// ------------------------------------------------------------------
	// Post-insert: indexes, queries, aggregation, compaction
	// ------------------------------------------------------------------
	c := clients[0]

	fmt.Println("\n--- Index Creation ---")
	indexStart := time.Now()
	for _, field := range []string{"data.department", "data.experience_level", "data.salary", "data.hire_date"} {
		if err := c.CreateIndex(collection, field); err != nil {
			fmt.Printf("  Index %s: %v\n", field, err)
		}
	}
	if err := c.CreateCompositeIndex(collection, []string{"data.department", "data.experience_level"}); err != nil {
		fmt.Printf("  Composite index: %v\n", err)
	}
	fmt.Printf("  5 indexes created in %.1fs\n", time.Since(indexStart).Seconds())

	// List indexes
	indexes, err := c.ListIndexes(collection)
	if err == nil {
		fmt.Printf("  Total indexes: %d\n", len(indexes))
	}

	fmt.Println("\n--- Queries ---")

	// Count by department
	cnt, _ := c.Count(collection, map[string]any{"data.department": "Engineering"})
	fmt.Printf("  Engineering employees: %d\n", cnt)

	// Find top 5 by salary
	limit5 := 5
	docs, _ := c.Find(collection, map[string]any{}, &oxidb.FindOptions{
		Sort:  map[string]any{"data.salary": -1},
		Limit: &limit5,
	})
	fmt.Println("  Top 5 salaries:")
	for _, d := range docs {
		data, _ := d["data"].(map[string]any)
		fmt.Printf("    %v (%v) — $%v\n", data["full_name"], data["department"], data["salary"])
	}

	fmt.Println("\n--- Aggregation ---")

	// Average salary by department
	agg, _ := c.Aggregate(collection, []map[string]any{
		{"$group": map[string]any{
			"_id":    "$data.department",
			"avg":    map[string]any{"$avg": "$data.salary"},
			"count":  map[string]any{"$sum": 1},
		}},
		{"$sort": map[string]any{"avg": -1}},
		{"$limit": 5},
	})
	fmt.Println("  Top 5 departments by avg salary:")
	for _, row := range agg {
		fmt.Printf("    %-15v avg=$%.0f (%v employees)\n", row["_id"], row["avg"], row["count"])
	}

	fmt.Println("\n--- UpdateOne ---")
	upRes, _ := c.UpdateOne(collection,
		map[string]any{"data.department": "Engineering"},
		map[string]any{"$set": map[string]any{"data.featured": true}})
	fmt.Printf("  UpdateOne(Engineering -> featured=true): modified=%v\n", upRes["modified"])

	fmt.Println("\n--- Compact ---")
	compStart := time.Now()
	stats, err := c.Compact(collection)
	if err == nil {
		fmt.Printf("  old_size=%v, new_size=%v, docs_kept=%v (%.1fs)\n",
			stats["old_size"], stats["new_size"], stats["docs_kept"],
			time.Since(compStart).Seconds())
	}

	fmt.Printf("\nAll done.\n")
}
