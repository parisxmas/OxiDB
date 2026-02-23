package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	totalRows  = 50_000
	batchSize  = 500
	queryIters = 100

	oxidbDSN = "postgres://localhost:5433/oxidb"
	pgDSN    = "postgres://postgres:bench@localhost:5434/postgres?sslmode=disable"
)

var (
	departments = []string{"Engineering", "Sales", "Marketing", "Finance", "HR", "Support", "Legal", "Operations", "Product", "Design"}
	cities      = []string{"Istanbul", "Berlin", "London", "Paris", "Tokyo", "NYC", "SF", "Toronto", "Sydney", "Singapore"}
	countries   = []string{"TR", "DE", "UK", "FR", "JP", "US", "US", "CA", "AU", "SG"}
	firstNames  = []string{"Ali", "Ayse", "Max", "Lena", "John", "Emma", "Yuki", "Hans", "Marie", "Chen"}
	lastNames   = []string{"Yilmaz", "Mueller", "Smith", "Dupont", "Tanaka", "Brown", "Sato", "Weber", "Martin", "Wang"}
)

type BenchResult struct {
	Name  string
	OxiDB time.Duration
	PG    time.Duration
}

func (r BenchResult) Ratio() float64 {
	if r.OxiDB == 0 {
		return 0
	}
	return float64(r.PG) / float64(r.OxiDB)
}

func main() {
	ctx := context.Background()
	results := make([]BenchResult, 0, 6)

	// Connect to OxiDB
	fmt.Println("Connecting to OxiDB (localhost:5433)...")
	oxidb, err := pgx.Connect(ctx, oxidbDSN)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
		fmt.Println("  Make sure OxiDB is running with OXIDB_PG_PORT=5433")
		return
	}
	defer oxidb.Close(ctx)
	fmt.Println("  Connected.")

	// Connect to PostgreSQL
	fmt.Println("Connecting to PostgreSQL (localhost:5434)...")
	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
		fmt.Println("  Make sure PostgreSQL docker is running on port 5434")
		return
	}
	defer pg.Close(ctx)
	fmt.Println("  Connected.")

	// Setup tables
	fmt.Println("\nSetting up tables...")
	setupTable(ctx, oxidb, "OxiDB")
	setupTable(ctx, pg, "PostgreSQL")

	// Pre-generate all data
	fmt.Println("Pre-generating data...")
	rng := rand.New(rand.NewSource(42))
	rows := generateRows(rng, totalRows)
	fmt.Printf("  Generated %d rows\n", len(rows))

	// Pre-build INSERT SQL batches (same strings for both DBs)
	fmt.Println("Pre-building INSERT SQL batches...")
	insertBatches := buildInsertBatches(rows)
	fmt.Printf("  Built %d batches\n", len(insertBatches))

	// === Benchmark 1: Bulk INSERT ===
	fmt.Printf("\n[1/6] INSERT %dk rows (batch=%d, multi-row VALUES)...\n", totalRows/1000, batchSize)

	oxidbDur := benchInsert(ctx, oxidb, insertBatches, "OxiDB")
	fmt.Printf("  OxiDB:      %s\n", fmtDur(oxidbDur))

	pgDur := benchInsert(ctx, pg, insertBatches, "PostgreSQL")
	fmt.Printf("  PostgreSQL: %s\n", fmtDur(pgDur))

	results = append(results, BenchResult{
		Name:  fmt.Sprintf("INSERT %dk rows", totalRows/1000),
		OxiDB: oxidbDur,
		PG:    pgDur,
	})

	// Create indexes on both for fair comparison
	fmt.Println("\nCreating indexes on PostgreSQL...")
	pg.Exec(ctx, "CREATE INDEX IF NOT EXISTS idx_dept ON bench_users(department)")
	pg.Exec(ctx, "CREATE INDEX IF NOT EXISTS idx_age ON bench_users(age)")
	pg.Exec(ctx, "CREATE INDEX IF NOT EXISTS idx_active ON bench_users(is_active)")
	pg.Exec(ctx, "CREATE INDEX IF NOT EXISTS idx_score ON bench_users(score DESC)")

	fmt.Println("Creating indexes on OxiDB...")
	oxidb.Exec(ctx, "CREATE INDEX idx_dept ON bench_users(department)")
	oxidb.Exec(ctx, "CREATE INDEX idx_age ON bench_users(age)")
	oxidb.Exec(ctx, "CREATE INDEX idx_active ON bench_users(is_active)")
	oxidb.Exec(ctx, "CREATE INDEX idx_score ON bench_users(score)")

	// Warmup
	fmt.Println("\nWarming up...")
	warmup(ctx, oxidb)
	warmup(ctx, pg)

	// === Benchmark 2: Point SELECT ===
	fmt.Printf("\n[2/6] Point SELECT (WHERE department='X') x%d...\n", queryIters)

	oxidbDur = benchPointSelect(ctx, oxidb, rng)
	fmt.Printf("  OxiDB:      %s\n", fmtDur(oxidbDur))

	pgDur = benchPointSelect(ctx, pg, rng)
	fmt.Printf("  PostgreSQL: %s\n", fmtDur(pgDur))

	results = append(results, BenchResult{
		Name:  fmt.Sprintf("Point SELECT x%d", queryIters),
		OxiDB: oxidbDur,
		PG:    pgDur,
	})

	// === Benchmark 3: Range SELECT ===
	fmt.Printf("\n[3/6] Range SELECT (WHERE age BETWEEN a AND b LIMIT 100) x%d...\n", queryIters)

	oxidbDur = benchRangeSelect(ctx, oxidb, rng)
	fmt.Printf("  OxiDB:      %s\n", fmtDur(oxidbDur))

	pgDur = benchRangeSelect(ctx, pg, rng)
	fmt.Printf("  PostgreSQL: %s\n", fmtDur(pgDur))

	results = append(results, BenchResult{
		Name:  fmt.Sprintf("Range SELECT x%d", queryIters),
		OxiDB: oxidbDur,
		PG:    pgDur,
	})

	// === Benchmark 4: COUNT ===
	fmt.Printf("\n[4/6] COUNT(*) WHERE is_active=true x%d...\n", queryIters)

	oxidbDur = benchCount(ctx, oxidb)
	fmt.Printf("  OxiDB:      %s\n", fmtDur(oxidbDur))

	pgDur = benchCount(ctx, pg)
	fmt.Printf("  PostgreSQL: %s\n", fmtDur(pgDur))

	results = append(results, BenchResult{
		Name:  "COUNT(*) WHERE active",
		OxiDB: oxidbDur,
		PG:    pgDur,
	})

	// === Benchmark 5: ORDER BY + LIMIT ===
	fmt.Printf("\n[5/6] ORDER BY score DESC LIMIT 50 x%d...\n", queryIters)

	oxidbDur = benchOrderBy(ctx, oxidb)
	fmt.Printf("  OxiDB:      %s\n", fmtDur(oxidbDur))

	pgDur = benchOrderBy(ctx, pg)
	fmt.Printf("  PostgreSQL: %s\n", fmtDur(pgDur))

	results = append(results, BenchResult{
		Name:  "ORDER BY + LIMIT 50",
		OxiDB: oxidbDur,
		PG:    pgDur,
	})

	// === Benchmark 6: GROUP BY ===
	fmt.Printf("\n[6/6] GROUP BY department (COUNT, AVG) x%d...\n", queryIters)

	oxidbDur = benchGroupBy(ctx, oxidb)
	fmt.Printf("  OxiDB:      %s\n", fmtDur(oxidbDur))

	pgDur = benchGroupBy(ctx, pg)
	fmt.Printf("  PostgreSQL: %s\n", fmtDur(pgDur))

	results = append(results, BenchResult{
		Name:  "GROUP BY department",
		OxiDB: oxidbDur,
		PG:    pgDur,
	})

	// Print report
	printReport(results)

	// Cleanup tables
	fmt.Println("\nCleaning up...")
	oxidb.Exec(ctx, "DROP TABLE bench_users")
	pg.Exec(ctx, "DROP TABLE bench_users")
	fmt.Println("Done.")
}

func setupTable(ctx context.Context, conn *pgx.Conn, label string) {
	conn.Exec(ctx, "DROP TABLE IF EXISTS bench_users")
	_, err := conn.Exec(ctx, `CREATE TABLE bench_users (
		id INT,
		name TEXT,
		email TEXT,
		age INT,
		department TEXT,
		salary FLOAT,
		is_active BOOLEAN,
		city TEXT,
		country TEXT,
		score FLOAT
	)`)
	if err != nil {
		fmt.Printf("  %s CREATE TABLE error: %v\n", label, err)
	}
}

type row struct {
	id         int
	name       string
	email      string
	age        int
	department string
	salary     float64
	isActive   bool
	city       string
	country    string
	score      float64
}

func generateRows(rng *rand.Rand, n int) []row {
	rows := make([]row, n)
	for i := 0; i < n; i++ {
		first := firstNames[rng.Intn(len(firstNames))]
		last := lastNames[rng.Intn(len(lastNames))]
		cityIdx := rng.Intn(len(cities))
		rows[i] = row{
			id:         i + 1,
			name:       first + " " + last,
			email:      fmt.Sprintf("%s.%s%d@example.com", strings.ToLower(first), strings.ToLower(last), i),
			age:        20 + rng.Intn(45),
			department: departments[rng.Intn(len(departments))],
			salary:     30000 + rng.Float64()*120000,
			isActive:   rng.Float32() > 0.2,
			city:       cities[cityIdx],
			country:    countries[cityIdx],
			score:      rng.Float64() * 100,
		}
	}
	return rows
}

func escStr(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// buildInsertBatches pre-builds multi-row INSERT SQL strings.
// Each batch: INSERT INTO bench_users (...) VALUES (...), (...), ...
func buildInsertBatches(rows []row) []string {
	batches := make([]string, 0, (len(rows)+batchSize-1)/batchSize)
	var sb strings.Builder

	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		sb.Reset()
		sb.WriteString("INSERT INTO bench_users (id, name, email, age, department, salary, is_active, city, country, score) VALUES ")
		for j, r := range rows[i:end] {
			if j > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "(%d, '%s', '%s', %d, '%s', %.2f, %s, '%s', '%s', %.4f)",
				r.id, escStr(r.name), escStr(r.email), r.age, escStr(r.department),
				r.salary, boolStr(r.isActive), escStr(r.city), escStr(r.country), r.score)
		}
		batches = append(batches, sb.String())
	}
	return batches
}

func benchInsert(ctx context.Context, conn *pgx.Conn, batches []string, label string) time.Duration {
	start := time.Now()
	for i, sql := range batches {
		_, err := conn.Exec(ctx, sql)
		if err != nil {
			fmt.Printf("    %s INSERT error at batch %d: %v\n", label, i, err)
			return time.Since(start)
		}
		if (i+1)%100 == 0 {
			fmt.Printf("    %s: %dk/%dk\n", label, (i+1)*batchSize/1000, totalRows/1000)
		}
	}
	return time.Since(start)
}

func warmup(ctx context.Context, conn *pgx.Conn) {
	conn.QueryRow(ctx, "SELECT COUNT(*) FROM bench_users WHERE department = 'Engineering'").Scan(new(int))
	conn.QueryRow(ctx, "SELECT COUNT(*) FROM bench_users WHERE age BETWEEN 25 AND 35").Scan(new(int))
	conn.QueryRow(ctx, "SELECT COUNT(*) FROM bench_users WHERE is_active = true").Scan(new(int))
}

func benchPointSelect(ctx context.Context, conn *pgx.Conn, rng *rand.Rand) time.Duration {
	start := time.Now()
	for i := 0; i < queryIters; i++ {
		dept := departments[rng.Intn(len(departments))]
		sql := fmt.Sprintf("SELECT id, name, email, age, salary FROM bench_users WHERE department = '%s' LIMIT 100", dept)
		rows, err := conn.Query(ctx, sql)
		if err != nil {
			fmt.Printf("    Point SELECT error: %v\n", err)
			return time.Since(start)
		}
		for rows.Next() {
			var id, age int
			var name, email string
			var salary float64
			rows.Scan(&id, &name, &email, &age, &salary)
		}
		rows.Close()
	}
	return time.Since(start)
}

func benchRangeSelect(ctx context.Context, conn *pgx.Conn, rng *rand.Rand) time.Duration {
	start := time.Now()
	for i := 0; i < queryIters; i++ {
		low := 20 + rng.Intn(30)
		high := low + 5 + rng.Intn(10)
		sql := fmt.Sprintf("SELECT id, name, age, city FROM bench_users WHERE age BETWEEN %d AND %d LIMIT 100", low, high)
		rows, err := conn.Query(ctx, sql)
		if err != nil {
			fmt.Printf("    Range SELECT error: %v\n", err)
			return time.Since(start)
		}
		for rows.Next() {
			var id, age int
			var name, city string
			rows.Scan(&id, &name, &age, &city)
		}
		rows.Close()
	}
	return time.Since(start)
}

func benchCount(ctx context.Context, conn *pgx.Conn) time.Duration {
	start := time.Now()
	for i := 0; i < queryIters; i++ {
		var cnt int
		err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM bench_users WHERE is_active = true").Scan(&cnt)
		if err != nil {
			fmt.Printf("    COUNT error: %v\n", err)
			return time.Since(start)
		}
	}
	return time.Since(start)
}

func benchOrderBy(ctx context.Context, conn *pgx.Conn) time.Duration {
	start := time.Now()
	for i := 0; i < queryIters; i++ {
		rows, err := conn.Query(ctx, "SELECT id, name, score FROM bench_users ORDER BY score DESC LIMIT 50")
		if err != nil {
			fmt.Printf("    ORDER BY error: %v\n", err)
			return time.Since(start)
		}
		for rows.Next() {
			var id int
			var name string
			var score float64
			rows.Scan(&id, &name, &score)
		}
		rows.Close()
	}
	return time.Since(start)
}

func benchGroupBy(ctx context.Context, conn *pgx.Conn) time.Duration {
	start := time.Now()
	for i := 0; i < queryIters; i++ {
		rows, err := conn.Query(ctx, "SELECT department, COUNT(*), AVG(salary) FROM bench_users GROUP BY department")
		if err != nil {
			fmt.Printf("    GROUP BY error: %v\n", err)
			return time.Since(start)
		}
		for rows.Next() {
			var dept string
			var cnt int
			var avg float64
			rows.Scan(&dept, &cnt, &avg)
		}
		rows.Close()
	}
	return time.Since(start)
}

func printReport(results []BenchResult) {
	fmt.Println()
	fmt.Println("╔══════════════════════════════╦════════════╦════════════╦═════════╗")
	fmt.Println("║ Operation                    ║ OxiDB      ║ PostgreSQL ║ Ratio   ║")
	fmt.Println("╠══════════════════════════════╬════════════╬════════════╬═════════╣")
	for _, r := range results {
		ratio := r.Ratio()
		ratioStr := ""
		if ratio >= 1.0 {
			ratioStr = fmt.Sprintf("%.1fx", ratio)
		} else if ratio > 0 {
			ratioStr = fmt.Sprintf("%.1fx", ratio)
		} else {
			ratioStr = "N/A"
		}
		fmt.Printf("║ %-28s ║ %10s ║ %10s ║ %7s ║\n",
			r.Name, fmtDur(r.OxiDB), fmtDur(r.PG), ratioStr)
	}
	fmt.Println("╚══════════════════════════════╩════════════╩════════════╩═════════╝")
	fmt.Println()
	fmt.Println("Ratio = PostgreSQL time / OxiDB time (higher = OxiDB faster)")
}

func fmtDur(d time.Duration) string {
	if d >= time.Minute {
		return fmt.Sprintf("%.1fm", d.Minutes())
	}
	if d >= time.Second {
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}
