// Package comparison_blog benchmarks OxiDB vs PostgreSQL 16 on a realistic
// blog workload: 60 detailed posts, 1000 concurrent TCP connections, mixed
// read/write patterns including view-count increments and text search.
//
// Each goroutine opens its OWN TCP connection (OxiDB) or pgx.Conn (PostgreSQL).
// No connection pooling — this tests raw per-connection concurrency.
package comparison_blog

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/parisxmas/OxiDB/clients/go/oxidb"
)

// ═══════════════════════════════════════════════════════════════════════════
// Constants & globals
// ═══════════════════════════════════════════════════════════════════════════

const (
	oxidbHost = "127.0.0.1"
	oxidbPort = 4444
	pgDSN     = "postgres://bench:bench@127.0.0.1:5433/bench?sslmode=disable"

	blogCollection = "blog_posts"
	blogTable      = "blog_posts"
	numPosts       = 60
)

var (
	oxiClient *oxidb.Client
	ctx       = context.Background()

	timings     []TimingEntry
	timingMutex sync.Mutex

	// Seeded data — accessible by all tests
	blogPosts []map[string]any
)

// ═══════════════════════════════════════════════════════════════════════════
// Timing infrastructure
// ═══════════════════════════════════════════════════════════════════════════

type TimingEntry struct {
	Category  string
	Name      string
	OxiDur    time.Duration
	PgDur     time.Duration
	OxiCount  int
	PgCount   int
	OxiErr    string
	PgErr     string
	OxiDetail string
	PgDetail  string
}

func recordTiming(cat, name string, oxiDur, pgDur time.Duration, oxiCount, pgCount int, oxiErr, pgErr error) {
	timingMutex.Lock()
	defer timingMutex.Unlock()
	e := TimingEntry{
		Category: cat,
		Name:     name,
		OxiDur:   oxiDur,
		PgDur:    pgDur,
		OxiCount: oxiCount,
		PgCount:  pgCount,
	}
	if oxiErr != nil {
		e.OxiErr = oxiErr.Error()
	}
	if pgErr != nil {
		e.PgErr = pgErr.Error()
	}
	timings = append(timings, e)
}

func recordTimingDetailed(cat, name string, oxiDur, pgDur time.Duration, oxiCount, pgCount int, oxiErr, pgErr error, oxiDetail, pgDetail string) {
	timingMutex.Lock()
	defer timingMutex.Unlock()
	e := TimingEntry{
		Category:  cat,
		Name:      name,
		OxiDur:    oxiDur,
		PgDur:     pgDur,
		OxiCount:  oxiCount,
		PgCount:   pgCount,
		OxiDetail: oxiDetail,
		PgDetail:  pgDetail,
	}
	if oxiErr != nil {
		e.OxiErr = oxiErr.Error()
	}
	if pgErr != nil {
		e.PgErr = pgErr.Error()
	}
	timings = append(timings, e)
}

// ═══════════════════════════════════════════════════════════════════════════
// Latency collector for percentile tracking
// ═══════════════════════════════════════════════════════════════════════════

type LatencyCollector struct {
	mu      sync.Mutex
	samples []time.Duration
}

func NewLatencyCollector() *LatencyCollector {
	return &LatencyCollector{samples: make([]time.Duration, 0, 10000)}
}

func (lc *LatencyCollector) Record(d time.Duration) {
	lc.mu.Lock()
	lc.samples = append(lc.samples, d)
	lc.mu.Unlock()
}

func (lc *LatencyCollector) Percentile(p float64) time.Duration {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if len(lc.samples) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(lc.samples))
	copy(sorted, lc.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * p / 100.0)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func (lc *LatencyCollector) Avg() time.Duration {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if len(lc.samples) == 0 {
		return 0
	}
	var total time.Duration
	for _, s := range lc.samples {
		total += s
	}
	return total / time.Duration(len(lc.samples))
}

func (lc *LatencyCollector) Count() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.samples)
}

func (lc *LatencyCollector) Summary() string {
	return fmt.Sprintf("avg=%s p50=%s p95=%s p99=%s n=%d",
		fmtDur(lc.Avg()), fmtDur(lc.Percentile(50)),
		fmtDur(lc.Percentile(95)), fmtDur(lc.Percentile(99)),
		lc.Count())
}

// ═══════════════════════════════════════════════════════════════════════════
// Blog data model — 60 posts with rich fields
// ═══════════════════════════════════════════════════════════════════════════

var (
	blogTitles = []string{
		"Getting Started with Rust for Web Development",
		"10 Tips for Better Database Performance",
		"Understanding Concurrent Programming Patterns",
		"A Deep Dive into Document Databases",
		"Building Scalable Microservices Architecture",
		"The Future of Serverless Computing",
		"Mastering Git Workflows for Teams",
		"Introduction to WebAssembly",
		"Designing REST APIs That Developers Love",
		"Event-Driven Architecture Explained",
		"Container Orchestration with Kubernetes",
		"GraphQL vs REST: A Practical Comparison",
		"Optimizing Frontend Performance",
		"Machine Learning in Production Systems",
		"Security Best Practices for Modern Web Apps",
		"The Rise of Edge Computing",
		"Building Real-Time Applications with WebSockets",
		"Data Modeling for NoSQL Databases",
		"CI/CD Pipeline Best Practices",
		"Introduction to Functional Programming",
		"Monitoring and Observability at Scale",
		"How to Write Clean Code That Lasts",
		"Understanding Distributed Consensus",
		"The Art of Technical Writing",
		"Testing Strategies for Complex Systems",
		"Database Indexing Deep Dive",
		"Building CLI Tools in Go",
		"Introduction to Service Mesh",
		"Performance Tuning PostgreSQL",
		"Rust vs Go: Choosing the Right Tool",
		"Building Search Engines from Scratch",
		"Understanding Memory Management",
		"Deploying Applications with Docker",
		"API Gateway Patterns and Practices",
		"Introduction to Time-Series Databases",
		"Building Offline-First Mobile Apps",
		"The State of Frontend Frameworks in 2025",
		"Understanding CAP Theorem in Practice",
		"Building Data Pipelines with Apache Kafka",
		"Introduction to Zero-Knowledge Proofs",
		"Scaling Databases Beyond a Single Node",
		"Building Progressive Web Applications",
		"Understanding TLS and Certificate Management",
		"The Complete Guide to Database Transactions",
		"Building Recommendation Systems",
		"Introduction to Chaos Engineering",
		"Designing for High Availability",
		"Understanding Content Delivery Networks",
		"Building Plugin Systems in Rust",
		"The Evolution of Package Managers",
		"Introduction to Property-Based Testing",
		"Building Geospatial Applications",
		"Understanding Browser Rendering Pipeline",
		"Building Multi-Tenant SaaS Applications",
		"The Guide to Technical Interviews",
		"Understanding Garbage Collection Algorithms",
		"Building Event Sourcing Systems",
		"Introduction to Formal Verification",
		"Optimizing Rust Compile Times",
		"Building Reliable Distributed Systems",
	}

	blogCategories = []string{
		"Engineering", "Database", "DevOps", "Frontend", "Backend",
		"Architecture", "Security", "Cloud", "Performance", "Career",
	}

	blogTags = []string{
		"rust", "go", "javascript", "typescript", "python",
		"docker", "kubernetes", "aws", "postgresql", "mongodb",
		"microservices", "api", "testing", "performance", "security",
		"devops", "ci-cd", "architecture", "tutorial", "best-practices",
	}

	blogAuthors = []struct {
		Name      string
		Bio       string
		AvatarURL string
	}{
		{"Alice Chen", "Senior engineer building high-performance systems in Rust and Go.", "https://example.com/avatars/alice.jpg"},
		{"Bob Martinez", "Database architect with 15 years of experience in distributed systems.", "https://example.com/avatars/bob.jpg"},
		{"Carol Williams", "DevOps lead passionate about CI/CD and infrastructure as code.", "https://example.com/avatars/carol.jpg"},
		{"David Kim", "Full-stack developer and open source contributor.", "https://example.com/avatars/david.jpg"},
		{"Eve Johnson", "Security researcher and technical writer.", "https://example.com/avatars/eve.jpg"},
		{"Frank Brown", "Cloud architect specializing in serverless and edge computing.", "https://example.com/avatars/frank.jpg"},
		{"Grace Lee", "Frontend engineer focused on performance and accessibility.", "https://example.com/avatars/grace.jpg"},
		{"Hank Davis", "Backend engineer with deep expertise in message queues and streaming.", "https://example.com/avatars/hank.jpg"},
		{"Ivy Taylor", "Machine learning engineer bridging research and production.", "https://example.com/avatars/ivy.jpg"},
		{"Jack Wilson", "Site reliability engineer and author of several tech books.", "https://example.com/avatars/jack.jpg"},
	}

	blogStatuses = []string{"published", "draft", "archived"}

	// ~500 word lorem-style content paragraphs for blog posts
	contentParagraphs = []string{
		"Modern software development requires a deep understanding of distributed systems, concurrency patterns, and data management strategies. Teams must balance the need for rapid iteration with the imperative of building reliable, maintainable systems that can scale to meet growing demands. This article explores the fundamental principles that guide experienced engineers in making these critical architectural decisions.",
		"When designing database schemas, it is essential to consider not only the current requirements but also how the data access patterns might evolve over time. Indexing strategies, denormalization trade-offs, and query optimization techniques all play crucial roles in ensuring that your application performs well under production workloads. We will examine each of these areas in detail.",
		"The containerization revolution has fundamentally changed how we think about application deployment and infrastructure management. Docker and Kubernetes have become the de facto standards for packaging and orchestrating services, but understanding the underlying concepts of process isolation, resource management, and network namespaces is still critical for debugging production issues.",
		"Testing is not just about finding bugs — it is about building confidence in your software. A well-designed test suite serves as living documentation, catches regressions before they reach production, and enables fearless refactoring. From unit tests to integration tests to end-to-end scenarios, each layer of the testing pyramid serves a distinct purpose in your quality assurance strategy.",
		"Performance optimization is both an art and a science. While profiling tools can identify bottlenecks, the real skill lies in understanding the trade-offs between different optimization strategies. Sometimes the fastest code is not the most maintainable, and sometimes a small increase in latency at one layer can dramatically improve overall system throughput by reducing contention elsewhere.",
	}
)

func generateBlogPosts(rng *rand.Rand) []map[string]any {
	posts := make([]map[string]any, numPosts)
	baseTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)

	for i := 0; i < numPosts; i++ {
		author := blogAuthors[rng.Intn(len(blogAuthors))]
		category := blogCategories[rng.Intn(len(blogCategories))]

		// 2-4 tags from pool
		numTags := 2 + rng.Intn(3)
		tagSet := make(map[string]bool)
		tags := make([]any, 0, numTags)
		for len(tags) < numTags {
			tag := blogTags[rng.Intn(len(blogTags))]
			if !tagSet[tag] {
				tagSet[tag] = true
				tags = append(tags, tag)
			}
		}

		// Build ~500 word content from paragraphs
		var contentParts []string
		for j := 0; j < 3+rng.Intn(3); j++ {
			contentParts = append(contentParts, contentParagraphs[rng.Intn(len(contentParagraphs))])
		}
		content := strings.Join(contentParts, "\n\n")

		publishedAt := baseTime.Add(time.Duration(i*3+rng.Intn(5)) * 24 * time.Hour)
		updatedAt := publishedAt.Add(time.Duration(rng.Intn(30)) * 24 * time.Hour)

		slug := strings.ToLower(strings.ReplaceAll(blogTitles[i], " ", "-"))
		slug = strings.ReplaceAll(slug, ":", "")
		slug = strings.ReplaceAll(slug, "'", "")

		status := "published"
		if rng.Intn(10) == 0 {
			status = blogStatuses[rng.Intn(len(blogStatuses))]
		}

		excerpt := content[:150] + "..."

		posts[i] = map[string]any{
			"id":                   i + 1,
			"title":                blogTitles[i],
			"slug":                 slug,
			"content":              content,
			"author_name":          author.Name,
			"author_bio":           author.Bio,
			"author_avatar_url":    author.AvatarURL,
			"category":             category,
			"tags":                 tags,
			"primary_tag":          tags[0],
			"image_url":            fmt.Sprintf("https://example.com/images/post-%d.jpg", i+1),
			"thumbnail_url":        fmt.Sprintf("https://example.com/thumbs/post-%d.jpg", i+1),
			"published_at":         publishedAt.Format(time.RFC3339),
			"updated_at":           updatedAt.Format(time.RFC3339),
			"status":               status,
			"view_count":           rng.Intn(50000),
			"like_count":           rng.Intn(5000),
			"comment_count":        rng.Intn(500),
			"featured":             rng.Intn(5) == 0,
			"reading_time_minutes": 3 + rng.Intn(15),
			"excerpt":              excerpt,
			"meta_description":     fmt.Sprintf("Learn about %s in this comprehensive guide.", strings.ToLower(blogTitles[i])),
		}
	}
	return posts
}

// ═══════════════════════════════════════════════════════════════════════════
// TestMain — global setup & teardown
// ═══════════════════════════════════════════════════════════════════════════

func TestMain(m *testing.M) {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║   OxiDB vs PostgreSQL — Blog Benchmark (1000 Connections)   ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")

	if err := waitFor("OxiDB", fmt.Sprintf("%s:%d", oxidbHost, oxidbPort), 60*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}

	if err := waitFor("PostgreSQL", "127.0.0.1:5433", 60*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}

	var err error
	oxiClient, err = oxidb.Connect(oxidbHost, oxidbPort, 30*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: OxiDB connect: %v\n", err)
		os.Exit(1)
	}

	// Verify PostgreSQL with a direct connection
	pgConn, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: PostgreSQL connect: %v\n", err)
		os.Exit(1)
	}
	pgConn.Close(ctx)
	fmt.Println("  PostgreSQL 16 ready (tmpfs, sync off, max_connections=1200).")
	fmt.Println()

	// Generate deterministic blog data
	rng := rand.New(rand.NewSource(42))
	blogPosts = generateBlogPosts(rng)

	code := m.Run()

	oxiClient.Close()
	generateHTMLReport()

	os.Exit(code)
}

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

func waitFor(name, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			conn.Close()
			fmt.Printf("  %s ready at %s\n", name, addr)
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("%s not reachable at %s after %s", name, addr, timeout)
}

func assertNoErr(t *testing.T, label string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", label, err)
	}
}

func intPtr(n int) *int { return &n }

func fmtDur(d time.Duration) string {
	if d == 0 {
		return "—"
	}
	if d < time.Millisecond {
		return d.Round(time.Microsecond).String()
	}
	if d < time.Second {
		return d.Round(100 * time.Microsecond).String()
	}
	return d.Round(time.Millisecond).String()
}

func fmtOps(n int) string {
	if n >= 1000 {
		return fmt.Sprintf("%.1fk", float64(n)/1000.0)
	}
	return fmt.Sprintf("%d", n)
}

// ═══════════════════════════════════════════════════════════════════════════
// PostgreSQL helpers — direct connection (no pool) for per-goroutine use
// ═══════════════════════════════════════════════════════════════════════════

func pgConnect() (*pgx.Conn, error) {
	return pgx.Connect(ctx, pgDSN)
}

func pgExec(conn *pgx.Conn, sql string, args ...any) error {
	_, err := conn.Exec(ctx, sql, args...)
	return err
}

func pgEnsureTable(conn *pgx.Conn) error {
	return pgExec(conn, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id SERIAL PRIMARY KEY,
		data JSONB NOT NULL
	)`, blogTable))
}

func pgInsertDoc(conn *pgx.Conn, doc map[string]any) error {
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	return pgExec(conn, fmt.Sprintf("INSERT INTO %s (data) VALUES ($1::jsonb)", blogTable), string(b))
}

func pgQuery(conn *pgx.Conn, where, orderBy string, limit int) ([]map[string]any, error) {
	q := fmt.Sprintf("SELECT data FROM %s", blogTable)
	if where != "" {
		q += " WHERE " + where
	}
	if orderBy != "" {
		q += " ORDER BY " + orderBy
	}
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []map[string]any
	for rows.Next() {
		var doc map[string]any
		if err := rows.Scan(&doc); err != nil {
			return nil, err
		}
		results = append(results, doc)
	}
	return results, rows.Err()
}

func pgQueryOne(conn *pgx.Conn, where string) (map[string]any, error) {
	docs, err := pgQuery(conn, where, "", 1)
	if err != nil {
		return nil, err
	}
	if len(docs) == 0 {
		return nil, nil
	}
	return docs[0], nil
}

func pgCount(conn *pgx.Conn, where string) (int, error) {
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s", blogTable)
	if where != "" {
		q += " WHERE " + where
	}
	var n int
	err := conn.QueryRow(ctx, q).Scan(&n)
	return n, err
}

func pgUpdate(conn *pgx.Conn, where, setExpr string) error {
	q := fmt.Sprintf("UPDATE %s SET %s", blogTable, setExpr)
	if where != "" {
		q += " WHERE " + where
	}
	_, err := conn.Exec(ctx, q)
	return err
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 1: Seed Data — Insert 60 posts + create indexes
// ═══════════════════════════════════════════════════════════════════════════

func TestSeedData(t *testing.T) {
	pgConn, err := pgConnect()
	assertNoErr(t, "pg connect", err)
	defer pgConn.Close(ctx)

	// Drop and recreate for clean state
	pgExec(pgConn, fmt.Sprintf("DROP TABLE IF EXISTS %s", blogTable))
	assertNoErr(t, "pg create table", pgEnsureTable(pgConn))
	_ = oxiClient.DropCollection(blogCollection)

	// ── Insert into OxiDB ─────────────────────────────────────────
	t0 := time.Now()
	for _, post := range blogPosts {
		_, err := oxiClient.Insert(blogCollection, post)
		assertNoErr(t, "OxiDB insert", err)
	}
	oxiInsertDur := time.Since(t0)
	t.Logf("OxiDB: inserted %d posts in %s", numPosts, oxiInsertDur)

	// ── Insert into PostgreSQL ────────────────────────────────────
	t0 = time.Now()
	for _, post := range blogPosts {
		assertNoErr(t, "pg insert", pgInsertDoc(pgConn, post))
	}
	pgInsertDur := time.Since(t0)
	t.Logf("PostgreSQL: inserted %d posts in %s", numPosts, pgInsertDur)

	recordTiming("Seed", fmt.Sprintf("Insert %d blog posts", numPosts), oxiInsertDur, pgInsertDur, numPosts, numPosts, nil, nil)

	// ── Create OxiDB indexes ──────────────────────────────────────
	t0 = time.Now()
	oxiClient.CreateUniqueIndex(blogCollection, "slug")
	oxiClient.CreateIndex(blogCollection, "category")
	oxiClient.CreateIndex(blogCollection, "status")
	oxiClient.CreateIndex(blogCollection, "author_name")
	oxiClient.CreateIndex(blogCollection, "published_at")
	oxiClient.CreateIndex(blogCollection, "view_count")
	oxiClient.CreateIndex(blogCollection, "featured")
	oxiClient.CreateIndex(blogCollection, "primary_tag")
	oxiClient.CreateTextIndex(blogCollection, []string{"title", "content"})
	oxiIndexDur := time.Since(t0)
	t.Logf("OxiDB: indexes created in %s", oxiIndexDur)

	// ── Create PostgreSQL indexes ─────────────────────────────────
	t0 = time.Now()
	pgExec(pgConn, fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_blog_slug ON %s ((data->>'slug'))", blogTable))
	pgExec(pgConn, fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_blog_category ON %s ((data->>'category'))", blogTable))
	pgExec(pgConn, fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_blog_status ON %s ((data->>'status'))", blogTable))
	pgExec(pgConn, fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_blog_author ON %s ((data->>'author_name'))", blogTable))
	pgExec(pgConn, fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_blog_published ON %s ((data->>'published_at'))", blogTable))
	pgExec(pgConn, fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_blog_views ON %s (((data->>'view_count')::int))", blogTable))
	pgExec(pgConn, fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_blog_featured ON %s (((data->>'featured')::boolean))", blogTable))
	pgExec(pgConn, fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_blog_ptag ON %s ((data->>'primary_tag'))", blogTable))
	pgExec(pgConn, fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_blog_gin ON %s USING GIN (data)", blogTable))
	// Full-text search index on title+content
	pgExec(pgConn, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_blog_fts ON %s USING GIN (
		to_tsvector('english', coalesce(data->>'title','') || ' ' || coalesce(data->>'content',''))
	)`, blogTable))
	pgIndexDur := time.Since(t0)
	t.Logf("PostgreSQL: indexes created in %s", pgIndexDur)

	recordTiming("Seed", "Create indexes", oxiIndexDur, pgIndexDur, 9, 10, nil, nil)

	// Verify counts
	oxiCount, err := oxiClient.Count(blogCollection, map[string]any{})
	assertNoErr(t, "OxiDB count", err)
	pgN, err := pgCount(pgConn, "")
	assertNoErr(t, "pg count", err)
	if oxiCount != numPosts || pgN != numPosts {
		t.Fatalf("count mismatch: OxiDB=%d, PostgreSQL=%d, expected=%d", oxiCount, pgN, numPosts)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 2: Single-Connection Query Patterns
// ═══════════════════════════════════════════════════════════════════════════

func TestSingleConnectionQueries(t *testing.T) {
	pgConn, err := pgConnect()
	assertNoErr(t, "pg connect", err)
	defer pgConn.Close(ctx)

	type queryCase struct {
		name     string
		oxiFn    func() (int, error)
		pgFn     func() (int, error)
	}

	slug := blogPosts[0]["slug"].(string)

	cases := []queryCase{
		{
			name: "Get post by slug",
			oxiFn: func() (int, error) {
				doc, err := oxiClient.FindOne(blogCollection, map[string]any{"slug": slug})
				if doc != nil {
					return 1, err
				}
				return 0, err
			},
			pgFn: func() (int, error) {
				doc, err := pgQueryOne(pgConn, fmt.Sprintf("data->>'slug' = '%s'", slug))
				if doc != nil {
					return 1, err
				}
				return 0, err
			},
		},
		{
			name: "Homepage: published, sort by date DESC, limit 10",
			oxiFn: func() (int, error) {
				docs, err := oxiClient.Find(blogCollection,
					map[string]any{"status": "published"},
					&oxidb.FindOptions{Sort: map[string]any{"published_at": -1}, Limit: intPtr(10)})
				return len(docs), err
			},
			pgFn: func() (int, error) {
				docs, err := pgQuery(pgConn,
					"data->>'status' = 'published'",
					"data->>'published_at' DESC", 10)
				return len(docs), err
			},
		},
		{
			name: "Posts by category: Engineering",
			oxiFn: func() (int, error) {
				docs, err := oxiClient.Find(blogCollection,
					map[string]any{"category": "Engineering"},
					&oxidb.FindOptions{Sort: map[string]any{"published_at": -1}, Limit: intPtr(10)})
				return len(docs), err
			},
			pgFn: func() (int, error) {
				docs, err := pgQuery(pgConn,
					"data->>'category' = 'Engineering'",
					"data->>'published_at' DESC", 10)
				return len(docs), err
			},
		},
		{
			name: "Posts by tag: rust",
			oxiFn: func() (int, error) {
				docs, err := oxiClient.Find(blogCollection,
					map[string]any{"primary_tag": "rust"},
					&oxidb.FindOptions{Limit: intPtr(10)})
				return len(docs), err
			},
			pgFn: func() (int, error) {
				docs, err := pgQuery(pgConn,
					"data->>'primary_tag' = 'rust'", "", 10)
				return len(docs), err
			},
		},
		{
			name: "Featured posts",
			oxiFn: func() (int, error) {
				docs, err := oxiClient.Find(blogCollection,
					map[string]any{"featured": true, "status": "published"},
					&oxidb.FindOptions{Sort: map[string]any{"published_at": -1}, Limit: intPtr(10)})
				return len(docs), err
			},
			pgFn: func() (int, error) {
				docs, err := pgQuery(pgConn,
					"(data->>'featured')::boolean = true AND data->>'status' = 'published'",
					"data->>'published_at' DESC", 10)
				return len(docs), err
			},
		},
		{
			name: "Recent posts (last 30 days from latest)",
			oxiFn: func() (int, error) {
				cutoff := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
				docs, err := oxiClient.Find(blogCollection,
					map[string]any{"published_at": map[string]any{"$gte": cutoff}, "status": "published"},
					&oxidb.FindOptions{Sort: map[string]any{"published_at": -1}, Limit: intPtr(20)})
				return len(docs), err
			},
			pgFn: func() (int, error) {
				docs, err := pgQuery(pgConn,
					"data->>'published_at' >= '2024-06-01T00:00:00Z' AND data->>'status' = 'published'",
					"data->>'published_at' DESC", 20)
				return len(docs), err
			},
		},
		{
			name: "Top posts by views (limit 10)",
			oxiFn: func() (int, error) {
				docs, err := oxiClient.Find(blogCollection,
					map[string]any{"status": "published"},
					&oxidb.FindOptions{Sort: map[string]any{"view_count": -1}, Limit: intPtr(10)})
				return len(docs), err
			},
			pgFn: func() (int, error) {
				docs, err := pgQuery(pgConn,
					"data->>'status' = 'published'",
					"(data->>'view_count')::int DESC", 10)
				return len(docs), err
			},
		},
		{
			name: "Text search: database",
			oxiFn: func() (int, error) {
				docs, err := oxiClient.TextSearch(blogCollection, "database", 20)
				return len(docs), err
			},
			pgFn: func() (int, error) {
				docs, err := pgQuery(pgConn,
					fmt.Sprintf("to_tsvector('english', coalesce(data->>'title','') || ' ' || coalesce(data->>'content','')) @@ plainto_tsquery('english', 'database')"),
					"", 20)
				return len(docs), err
			},
		},
		{
			name: "Aggregation: count by category",
			oxiFn: func() (int, error) {
				res, err := oxiClient.Aggregate(blogCollection, []map[string]any{
					{"$match": map[string]any{"status": "published"}},
					{"$group": map[string]any{"_id": "$category", "count": map[string]any{"$sum": 1}}},
					{"$sort": map[string]any{"count": -1}},
				})
				return len(res), err
			},
			pgFn: func() (int, error) {
				rows, err := pgConn.Query(ctx, fmt.Sprintf(
					`SELECT data->>'category', COUNT(*) FROM %s WHERE data->>'status' = 'published' GROUP BY data->>'category' ORDER BY COUNT(*) DESC`, blogTable))
				if err != nil {
					return 0, err
				}
				defer rows.Close()
				n := 0
				for rows.Next() {
					var cat string
					var count int
					rows.Scan(&cat, &count)
					n++
				}
				return n, rows.Err()
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t0 := time.Now()
			oxiCount, oxiErr := tc.oxiFn()
			oxiDur := time.Since(t0)

			t0 = time.Now()
			pgCount, pgErr := tc.pgFn()
			pgDur := time.Since(t0)

			if oxiErr != nil {
				t.Errorf("OxiDB error: %v", oxiErr)
			}
			if pgErr != nil {
				t.Errorf("PostgreSQL error: %v", pgErr)
			}

			t.Logf("OxiDB: %s (%d results)  PostgreSQL: %s (%d results)",
				fmtDur(oxiDur), oxiCount, fmtDur(pgDur), pgCount)
			recordTiming("Single Query", tc.name, oxiDur, pgDur, oxiCount, pgCount, oxiErr, pgErr)
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3: 1000 Concurrent Readers — each with own connection
// ═══════════════════════════════════════════════════════════════════════════

func TestConcurrent1000Readers(t *testing.T) {
	const numReaders = 1000

	// Each reader simulates browsing: homepage → click post → category → popular → search
	// 5-10 random reads per reader

	t.Run("OxiDB_1000_readers", func(t *testing.T) {
		lc := NewLatencyCollector()
		var wg sync.WaitGroup
		var errCount int64
		var mu sync.Mutex
		barrier := make(chan struct{})

		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				c, err := oxidb.Connect(oxidbHost, oxidbPort, 15*time.Second)
				if err != nil {
					mu.Lock()
					errCount++
					mu.Unlock()
					return
				}
				defer c.Close()

				lr := rand.New(rand.NewSource(int64(id)))
				<-barrier

				numOps := 5 + lr.Intn(6)
				for op := 0; op < numOps; op++ {
					t0 := time.Now()
					var opErr error
					switch lr.Intn(5) {
					case 0: // Homepage
						_, opErr = c.Find(blogCollection,
							map[string]any{"status": "published"},
							&oxidb.FindOptions{Sort: map[string]any{"published_at": -1}, Limit: intPtr(10)})
					case 1: // Get single post by slug
						post := blogPosts[lr.Intn(len(blogPosts))]
						_, opErr = c.FindOne(blogCollection, map[string]any{"slug": post["slug"]})
					case 2: // Category page
						cat := blogCategories[lr.Intn(len(blogCategories))]
						_, opErr = c.Find(blogCollection,
							map[string]any{"category": cat, "status": "published"},
							&oxidb.FindOptions{Sort: map[string]any{"published_at": -1}, Limit: intPtr(10)})
					case 3: // Popular sidebar
						_, opErr = c.Find(blogCollection,
							map[string]any{"status": "published"},
							&oxidb.FindOptions{Sort: map[string]any{"view_count": -1}, Limit: intPtr(5)})
					case 4: // Text search
						terms := []string{"database", "rust", "kubernetes", "performance", "testing"}
						_, opErr = c.TextSearch(blogCollection, terms[lr.Intn(len(terms))], 20)
					}
					dur := time.Since(t0)
					lc.Record(dur)
					if opErr != nil {
						mu.Lock()
						errCount++
						mu.Unlock()
					}
				}
			}(i)
		}

		t0 := time.Now()
		close(barrier)
		wg.Wait()
		totalDur := time.Since(t0)

		t.Logf("OxiDB 1000 readers: %s total | %s | errors: %d",
			fmtDur(totalDur), lc.Summary(), errCount)

		recordTimingDetailed("Concurrent Reads", "1000 readers",
			totalDur, 0, lc.Count(), 0, nil, nil,
			fmt.Sprintf("1000 conns | %s ops/s | %s",
				fmtOps(int(float64(lc.Count())/totalDur.Seconds())), lc.Summary()), "")
	})

	t.Run("PostgreSQL_1000_readers", func(t *testing.T) {
		lc := NewLatencyCollector()
		var wg sync.WaitGroup
		var errCount int64
		var mu sync.Mutex
		barrier := make(chan struct{})

		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				conn, err := pgx.Connect(ctx, pgDSN)
				if err != nil {
					mu.Lock()
					errCount++
					mu.Unlock()
					return
				}
				defer conn.Close(ctx)

				lr := rand.New(rand.NewSource(int64(id)))
				<-barrier

				numOps := 5 + lr.Intn(6)
				for op := 0; op < numOps; op++ {
					t0 := time.Now()
					var opErr error
					switch lr.Intn(5) {
					case 0: // Homepage
						_, opErr = pgQuery(conn,
							"data->>'status' = 'published'",
							"data->>'published_at' DESC", 10)
					case 1: // Get single post by slug
						post := blogPosts[lr.Intn(len(blogPosts))]
						_, opErr = pgQueryOne(conn,
							fmt.Sprintf("data->>'slug' = '%s'", post["slug"]))
					case 2: // Category page
						cat := blogCategories[lr.Intn(len(blogCategories))]
						_, opErr = pgQuery(conn,
							fmt.Sprintf("data->>'category' = '%s' AND data->>'status' = 'published'", cat),
							"data->>'published_at' DESC", 10)
					case 3: // Popular sidebar
						_, opErr = pgQuery(conn,
							"data->>'status' = 'published'",
							"(data->>'view_count')::int DESC", 5)
					case 4: // Text search
						terms := []string{"database", "rust", "kubernetes", "performance", "testing"}
						term := terms[lr.Intn(len(terms))]
						_, opErr = pgQuery(conn,
							fmt.Sprintf("to_tsvector('english', coalesce(data->>'title','') || ' ' || coalesce(data->>'content','')) @@ plainto_tsquery('english', '%s')", term),
							"", 20)
					}
					dur := time.Since(t0)
					lc.Record(dur)
					if opErr != nil {
						mu.Lock()
						errCount++
						mu.Unlock()
					}
				}
			}(i)
		}

		t0 := time.Now()
		close(barrier)
		wg.Wait()
		totalDur := time.Since(t0)

		t.Logf("PostgreSQL 1000 readers: %s total | %s | errors: %d",
			fmtDur(totalDur), lc.Summary(), errCount)

		// Update the OxiDB entry with PostgreSQL data
		timingMutex.Lock()
		for i := range timings {
			if timings[i].Name == "1000 readers" && timings[i].Category == "Concurrent Reads" {
				timings[i].PgDur = totalDur
				timings[i].PgCount = lc.Count()
				timings[i].PgDetail = fmt.Sprintf("1000 conns | %s ops/s | %s",
					fmtOps(int(float64(lc.Count())/totalDur.Seconds())), lc.Summary())
				break
			}
		}
		timingMutex.Unlock()
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4: 1000 Concurrent Mixed — 800 readers + 100 view writers + 100 comment writers
// ═══════════════════════════════════════════════════════════════════════════

func TestConcurrent1000Mixed(t *testing.T) {
	const (
		numReaders        = 800
		numViewWriters    = 100
		numCommentWriters = 100
	)

	t.Run("OxiDB_mixed", func(t *testing.T) {
		lcRead := NewLatencyCollector()
		lcWrite := NewLatencyCollector()
		var wg sync.WaitGroup
		var readErrs, writeErrs int64
		var mu sync.Mutex
		barrier := make(chan struct{})

		// Readers
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				c, err := oxidb.Connect(oxidbHost, oxidbPort, 15*time.Second)
				if err != nil {
					mu.Lock()
					readErrs++
					mu.Unlock()
					return
				}
				defer c.Close()
				lr := rand.New(rand.NewSource(int64(id)))
				<-barrier
				for op := 0; op < 5; op++ {
					t0 := time.Now()
					var opErr error
					switch lr.Intn(3) {
					case 0:
						_, opErr = c.Find(blogCollection,
							map[string]any{"status": "published"},
							&oxidb.FindOptions{Sort: map[string]any{"published_at": -1}, Limit: intPtr(10)})
					case 1:
						post := blogPosts[lr.Intn(len(blogPosts))]
						_, opErr = c.FindOne(blogCollection, map[string]any{"slug": post["slug"]})
					case 2:
						cat := blogCategories[lr.Intn(len(blogCategories))]
						_, opErr = c.Find(blogCollection,
							map[string]any{"category": cat},
							&oxidb.FindOptions{Limit: intPtr(10)})
					}
					lcRead.Record(time.Since(t0))
					if opErr != nil {
						mu.Lock()
						readErrs++
						mu.Unlock()
					}
				}
			}(i)
		}

		// View count writers ($inc view_count)
		for i := 0; i < numViewWriters; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				c, err := oxidb.Connect(oxidbHost, oxidbPort, 15*time.Second)
				if err != nil {
					mu.Lock()
					writeErrs++
					mu.Unlock()
					return
				}
				defer c.Close()
				lr := rand.New(rand.NewSource(int64(id + 10000)))
				<-barrier
				for op := 0; op < 10; op++ {
					post := blogPosts[lr.Intn(len(blogPosts))]
					t0 := time.Now()
					_, err := c.UpdateOne(blogCollection,
						map[string]any{"slug": post["slug"].(string)},
						map[string]any{"$inc": map[string]any{"view_count": 1}})
					lcWrite.Record(time.Since(t0))
					if err != nil {
						mu.Lock()
						writeErrs++
						mu.Unlock()
					}
				}
			}(i)
		}

		// Comment count writers ($inc comment_count)
		for i := 0; i < numCommentWriters; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				c, err := oxidb.Connect(oxidbHost, oxidbPort, 15*time.Second)
				if err != nil {
					mu.Lock()
					writeErrs++
					mu.Unlock()
					return
				}
				defer c.Close()
				lr := rand.New(rand.NewSource(int64(id + 20000)))
				<-barrier
				for op := 0; op < 10; op++ {
					post := blogPosts[lr.Intn(len(blogPosts))]
					t0 := time.Now()
					_, err := c.UpdateOne(blogCollection,
						map[string]any{"slug": post["slug"].(string)},
						map[string]any{"$inc": map[string]any{"comment_count": 1}})
					lcWrite.Record(time.Since(t0))
					if err != nil {
						mu.Lock()
						writeErrs++
						mu.Unlock()
					}
				}
			}(i)
		}

		t0 := time.Now()
		close(barrier)
		wg.Wait()
		oxiDur := time.Since(t0)
		oxiTotalOps := lcRead.Count() + lcWrite.Count()

		t.Logf("OxiDB mixed: %s total | reads: %s | writes: %s | read_err: %d write_err: %d",
			fmtDur(oxiDur), lcRead.Summary(), lcWrite.Summary(), readErrs, writeErrs)

		recordTimingDetailed("Concurrent Mixed", "1000 mixed (800R+100V+100C)",
			oxiDur, 0, oxiTotalOps, 0, nil, nil,
			fmt.Sprintf("1000 conns | %s ops/s | R: %s | W: %s | err: %d/%d",
				fmtOps(int(float64(oxiTotalOps)/oxiDur.Seconds())),
				lcRead.Summary(), lcWrite.Summary(), readErrs, writeErrs), "")
	})

	t.Run("PostgreSQL_mixed", func(t *testing.T) {
		lcRead := NewLatencyCollector()
		lcWrite := NewLatencyCollector()
		var wg sync.WaitGroup
		var readErrs, writeErrs int64
		var mu sync.Mutex
		barrier := make(chan struct{})

		// Readers
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				conn, err := pgx.Connect(ctx, pgDSN)
				if err != nil {
					mu.Lock()
					readErrs++
					mu.Unlock()
					return
				}
				defer conn.Close(ctx)
				lr := rand.New(rand.NewSource(int64(id)))
				<-barrier
				for op := 0; op < 5; op++ {
					t0 := time.Now()
					var opErr error
					switch lr.Intn(3) {
					case 0:
						_, opErr = pgQuery(conn,
							"data->>'status' = 'published'",
							"data->>'published_at' DESC", 10)
					case 1:
						post := blogPosts[lr.Intn(len(blogPosts))]
						_, opErr = pgQueryOne(conn,
							fmt.Sprintf("data->>'slug' = '%s'", post["slug"]))
					case 2:
						cat := blogCategories[lr.Intn(len(blogCategories))]
						_, opErr = pgQuery(conn,
							fmt.Sprintf("data->>'category' = '%s'", cat), "", 10)
					}
					lcRead.Record(time.Since(t0))
					if opErr != nil {
						mu.Lock()
						readErrs++
						mu.Unlock()
					}
				}
			}(i)
		}

		// View count writers
		for i := 0; i < numViewWriters; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				conn, err := pgx.Connect(ctx, pgDSN)
				if err != nil {
					mu.Lock()
					writeErrs++
					mu.Unlock()
					return
				}
				defer conn.Close(ctx)
				lr := rand.New(rand.NewSource(int64(id + 10000)))
				<-barrier
				for op := 0; op < 10; op++ {
					post := blogPosts[lr.Intn(len(blogPosts))]
					slug := post["slug"].(string)
					t0 := time.Now()
					err := pgUpdate(conn,
						fmt.Sprintf("data->>'slug' = '%s'", slug),
						"data = jsonb_set(data, '{view_count}', to_jsonb((data->>'view_count')::int + 1))")
					lcWrite.Record(time.Since(t0))
					if err != nil {
						mu.Lock()
						writeErrs++
						mu.Unlock()
					}
				}
			}(i)
		}

		// Comment count writers
		for i := 0; i < numCommentWriters; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				conn, err := pgx.Connect(ctx, pgDSN)
				if err != nil {
					mu.Lock()
					writeErrs++
					mu.Unlock()
					return
				}
				defer conn.Close(ctx)
				lr := rand.New(rand.NewSource(int64(id + 20000)))
				<-barrier
				for op := 0; op < 10; op++ {
					post := blogPosts[lr.Intn(len(blogPosts))]
					slug := post["slug"].(string)
					t0 := time.Now()
					err := pgUpdate(conn,
						fmt.Sprintf("data->>'slug' = '%s'", slug),
						"data = jsonb_set(data, '{comment_count}', to_jsonb((data->>'comment_count')::int + 1))")
					lcWrite.Record(time.Since(t0))
					if err != nil {
						mu.Lock()
						writeErrs++
						mu.Unlock()
					}
				}
			}(i)
		}

		t0 := time.Now()
		close(barrier)
		wg.Wait()
		pgDur := time.Since(t0)
		pgTotalOps := lcRead.Count() + lcWrite.Count()

		t.Logf("PostgreSQL mixed: %s total | reads: %s | writes: %s | read_err: %d write_err: %d",
			fmtDur(pgDur), lcRead.Summary(), lcWrite.Summary(), readErrs, writeErrs)

		// Update the OxiDB entry with PostgreSQL data
		timingMutex.Lock()
		for i := range timings {
			if timings[i].Name == "1000 mixed (800R+100V+100C)" && timings[i].Category == "Concurrent Mixed" {
				timings[i].PgDur = pgDur
				timings[i].PgCount = pgTotalOps
				timings[i].PgDetail = fmt.Sprintf("1000 conns | %s ops/s | R: %s | W: %s | err: %d/%d",
					fmtOps(int(float64(pgTotalOps)/pgDur.Seconds())),
					lcRead.Summary(), lcWrite.Summary(), readErrs, writeErrs)
				break
			}
		}
		timingMutex.Unlock()
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 5: Connection Storm — 1000 goroutines connect+query+close rapidly
// ═══════════════════════════════════════════════════════════════════════════

func TestConnectionStorm(t *testing.T) {
	const numStormers = 1000

	t.Run("OxiDB_storm", func(t *testing.T) {
		lc := NewLatencyCollector()
		var wg sync.WaitGroup
		var errCount int64
		var mu sync.Mutex
		barrier := make(chan struct{})

		for i := 0; i < numStormers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				lr := rand.New(rand.NewSource(int64(id)))
				<-barrier

				t0 := time.Now()
				c, err := oxidb.Connect(oxidbHost, oxidbPort, 15*time.Second)
				if err != nil {
					mu.Lock()
					errCount++
					mu.Unlock()
					return
				}

				// Single query
				post := blogPosts[lr.Intn(len(blogPosts))]
				_, err = c.FindOne(blogCollection, map[string]any{"slug": post["slug"]})
				c.Close()
				dur := time.Since(t0)
				lc.Record(dur)

				if err != nil {
					mu.Lock()
					errCount++
					mu.Unlock()
				}
			}(i)
		}

		t0 := time.Now()
		close(barrier)
		wg.Wait()
		oxiDur := time.Since(t0)

		t.Logf("OxiDB storm: %s total | %s | errors: %d",
			fmtDur(oxiDur), lc.Summary(), errCount)

		recordTimingDetailed("Connection Storm", "1000 connect+query+close",
			oxiDur, 0, lc.Count(), 0, nil, nil,
			fmt.Sprintf("%s ops/s | %s | %d err",
				fmtOps(int(float64(lc.Count())/oxiDur.Seconds())), lc.Summary(), errCount), "")
	})

	t.Run("PostgreSQL_storm", func(t *testing.T) {
		lc := NewLatencyCollector()
		var wg sync.WaitGroup
		var errCount int64
		var mu sync.Mutex
		barrier := make(chan struct{})

		for i := 0; i < numStormers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				lr := rand.New(rand.NewSource(int64(id)))
				<-barrier

				t0 := time.Now()
				conn, err := pgx.Connect(ctx, pgDSN)
				if err != nil {
					mu.Lock()
					errCount++
					mu.Unlock()
					return
				}

				post := blogPosts[lr.Intn(len(blogPosts))]
				_, err = pgQueryOne(conn,
					fmt.Sprintf("data->>'slug' = '%s'", post["slug"]))
				conn.Close(ctx)
				dur := time.Since(t0)
				lc.Record(dur)

				if err != nil {
					mu.Lock()
					errCount++
					mu.Unlock()
				}
			}(i)
		}

		t0 := time.Now()
		close(barrier)
		wg.Wait()
		pgDur := time.Since(t0)

		t.Logf("PostgreSQL storm: %s total | %s | errors: %d",
			fmtDur(pgDur), lc.Summary(), errCount)

		// Update entry
		timingMutex.Lock()
		for i := range timings {
			if timings[i].Name == "1000 connect+query+close" && timings[i].Category == "Connection Storm" {
				timings[i].PgDur = pgDur
				timings[i].PgCount = lc.Count()
				timings[i].PgDetail = fmt.Sprintf("%s ops/s | %s | %d err",
					fmtOps(int(float64(lc.Count())/pgDur.Seconds())), lc.Summary(), errCount)
				break
			}
		}
		timingMutex.Unlock()
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// HTML Report Generator
// ═══════════════════════════════════════════════════════════════════════════

func generateHTMLReport() {
	timingMutex.Lock()
	defer timingMutex.Unlock()

	if len(timings) == 0 {
		return
	}

	type catGroup struct {
		Name    string
		Entries []TimingEntry
	}
	catMap := map[string]*catGroup{}
	catOrder := []string{}
	for _, e := range timings {
		if _, ok := catMap[e.Category]; !ok {
			catMap[e.Category] = &catGroup{Name: e.Category}
			catOrder = append(catOrder, e.Category)
		}
		catMap[e.Category].Entries = append(catMap[e.Category].Entries, e)
	}

	oxiWins, pgWins, ties := 0, 0, 0
	for _, e := range timings {
		if e.PgDur == 0 || e.OxiDur == 0 {
			continue
		}
		if e.OxiDur < e.PgDur {
			oxiWins++
		} else if e.PgDur < e.OxiDur {
			pgWins++
		} else {
			ties++
		}
	}

	naCount := 0
	for _, e := range timings {
		if e.PgDur == 0 || e.OxiDur == 0 {
			naCount++
		}
	}

	var sb strings.Builder
	sb.WriteString(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>OxiDB vs PostgreSQL — Blog Benchmark Report</title>
<style>
  :root { --bg: #0d1117; --card: #161b22; --border: #30363d; --text: #e6edf3; --dim: #8b949e; --green: #3fb950; --red: #f85149; --blue: #58a6ff; --yellow: #d29922; --purple: #bc8cff; }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; background: var(--bg); color: var(--text); padding: 24px; }
  .container { max-width: 1200px; margin: 0 auto; }
  h1 { font-size: 28px; font-weight: 700; margin-bottom: 8px; }
  .subtitle { color: var(--dim); font-size: 14px; margin-bottom: 32px; }
  .summary { display: flex; gap: 16px; margin-bottom: 32px; flex-wrap: wrap; }
  .summary-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px 28px; flex: 1; min-width: 200px; text-align: center; }
  .summary-card .label { font-size: 13px; color: var(--dim); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }
  .summary-card .value { font-size: 36px; font-weight: 700; }
  .summary-card .value.green { color: var(--green); }
  .summary-card .value.red { color: var(--red); }
  .summary-card .value.blue { color: var(--blue); }
  .summary-card .value.yellow { color: var(--yellow); }
  .category { background: var(--card); border: 1px solid var(--border); border-radius: 12px; margin-bottom: 24px; overflow: hidden; }
  .category h2 { font-size: 18px; font-weight: 600; padding: 16px 20px; border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 8px; }
  .category h2 .badge { font-size: 12px; background: var(--border); color: var(--dim); padding: 2px 8px; border-radius: 10px; font-weight: 400; }
  table { width: 100%; border-collapse: collapse; font-size: 14px; }
  th { text-align: left; padding: 10px 16px; color: var(--dim); font-weight: 500; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 1px solid var(--border); }
  td { padding: 10px 16px; border-bottom: 1px solid var(--border); }
  tr:last-child td { border-bottom: none; }
  tr:hover { background: rgba(88,166,255,0.04); }
  .time { font-family: 'SF Mono', 'Fira Code', monospace; font-size: 13px; }
  .winner { font-weight: 600; }
  .oxi-win { color: var(--green); }
  .pg-win { color: var(--blue); }
  .tie { color: var(--dim); }
  .error { color: var(--red); font-size: 12px; }
  .bar-cell { width: 220px; }
  .bar-container { display: flex; height: 20px; border-radius: 4px; overflow: hidden; background: var(--border); }
  .bar-oxi { background: var(--green); min-width: 2px; }
  .bar-pg { background: var(--blue); min-width: 2px; }
  .speedup { font-family: 'SF Mono', 'Fira Code', monospace; font-size: 12px; color: var(--yellow); }
  .badge-pass { background: #1a3a2a; color: var(--green); padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
  .badge-fail { background: #3a1a1a; color: var(--red); padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
  .count { color: var(--dim); font-size: 12px; }
  .detail { color: var(--purple); font-size: 11px; display: block; margin-top: 2px; font-family: 'SF Mono', 'Fira Code', monospace; }
  .footer { margin-top: 32px; text-align: center; color: var(--dim); font-size: 12px; padding: 16px; border-top: 1px solid var(--border); }
</style>
</head>
<body>
<div class="container">
<h1>OxiDB vs PostgreSQL — Blog Benchmark</h1>
<p class="subtitle">1000 concurrent connections &mdash; 60 blog posts &mdash; ` + time.Now().Format("2006-01-02 15:04:05 MST") + ` &mdash; ` + fmt.Sprintf("%d tests", len(timings)) + `</p>

<div class="summary">
  <div class="summary-card"><div class="label">Total Tests</div><div class="value blue">` + fmt.Sprintf("%d", len(timings)) + `</div></div>
  <div class="summary-card"><div class="label">OxiDB Wins</div><div class="value green">` + fmt.Sprintf("%d", oxiWins) + `</div></div>
  <div class="summary-card"><div class="label">PostgreSQL Wins</div><div class="value blue">` + fmt.Sprintf("%d", pgWins) + `</div></div>
  <div class="summary-card"><div class="label">Ties / N/A</div><div class="value yellow">` + fmt.Sprintf("%d", ties+naCount) + `</div></div>
</div>
`)

	for _, catName := range catOrder {
		cg := catMap[catName]
		sb.WriteString(`<div class="category">
<h2>` + cg.Name + ` <span class="badge">` + fmt.Sprintf("%d tests", len(cg.Entries)) + `</span></h2>
<table>
<tr><th>Operation</th><th>OxiDB</th><th>PostgreSQL</th><th>Speedup</th><th>Winner</th><th>Result</th><th class="bar-cell">Comparison</th></tr>
`)
		for _, e := range cg.Entries {
			oxiStr := fmtDur(e.OxiDur)
			pgStr := fmtDur(e.PgDur)
			if e.PgDur == 0 {
				pgStr = "—"
			}
			if e.OxiDur == 0 {
				oxiStr = "—"
			}

			oxiTimeHTML := fmt.Sprintf(`<span>%s</span>`, oxiStr)
			if e.OxiDetail != "" {
				oxiTimeHTML += fmt.Sprintf(`<span class="detail">%s</span>`, e.OxiDetail)
			}
			pgTimeHTML := fmt.Sprintf(`<span>%s</span>`, pgStr)
			if e.PgDetail != "" {
				pgTimeHTML += fmt.Sprintf(`<span class="detail">%s</span>`, e.PgDetail)
			}

			winClass, winLabel := "tie", "—"
			speedupStr := "—"
			if e.PgDur > 0 && e.OxiDur > 0 {
				if e.OxiDur < e.PgDur {
					winClass = "oxi-win"
					winLabel = "OxiDB"
					factor := float64(e.PgDur) / float64(e.OxiDur)
					speedupStr = fmt.Sprintf("%.1fx", factor)
				} else if e.PgDur < e.OxiDur {
					winClass = "pg-win"
					winLabel = "PostgreSQL"
					factor := float64(e.OxiDur) / float64(e.PgDur)
					speedupStr = fmt.Sprintf("%.1fx", factor)
				} else {
					winLabel = "Tie"
					speedupStr = "1.0x"
				}
			}

			result := `<span class="badge-pass">PASS</span>`
			if e.OxiErr != "" {
				result = `<span class="badge-fail">FAIL</span> <span class="error">` + e.OxiErr + `</span>`
			}
			if e.PgErr != "" {
				result += ` <span class="badge-fail">PG FAIL</span> <span class="error">` + e.PgErr + `</span>`
			}
			if e.OxiCount > 0 || e.PgCount > 0 {
				result += fmt.Sprintf(` <span class="count">(%d / %d)</span>`, e.OxiCount, e.PgCount)
			}

			barHTML := ""
			if e.PgDur > 0 && e.OxiDur > 0 {
				total := float64(e.OxiDur + e.PgDur)
				oxiPct := float64(e.OxiDur) / total * 100
				pgPct := float64(e.PgDur) / total * 100
				barHTML = fmt.Sprintf(`<div class="bar-container"><div class="bar-oxi" style="width:%.1f%%" title="OxiDB: %s"></div><div class="bar-pg" style="width:%.1f%%" title="PostgreSQL: %s"></div></div>`, oxiPct, oxiStr, pgPct, pgStr)
			}

			sb.WriteString(fmt.Sprintf(`<tr>
  <td>%s</td>
  <td class="time">%s</td>
  <td class="time">%s</td>
  <td class="speedup">%s</td>
  <td class="winner %s">%s</td>
  <td>%s</td>
  <td class="bar-cell">%s</td>
</tr>
`, e.Name, oxiTimeHTML, pgTimeHTML, speedupStr, winClass, winLabel, result, barHTML))
		}
		sb.WriteString("</table>\n</div>\n")
	}

	sb.WriteString(`<div class="footer">Generated by OxiDB vs PostgreSQL Blog Benchmark &mdash; 1000 concurrent connections &mdash; ` + fmt.Sprintf("%d tests across %d categories", len(timings), len(catOrder)) + `</div>
</div>
</body>
</html>`)

	reportPath := "report.html"
	if err := os.WriteFile(reportPath, []byte(sb.String()), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write HTML report: %v\n", err)
		return
	}
	fmt.Printf("\n══════════════════════════════════════════════════════════════\n")
	fmt.Printf("  HTML Report: %s\n", reportPath)
	fmt.Printf("  OxiDB wins: %d | PostgreSQL wins: %d | Ties/N-A: %d\n", oxiWins, pgWins, ties+naCount)
	fmt.Printf("══════════════════════════════════════════════════════════════\n")

	// JSON summary
	summary := map[string]any{
		"total":    len(timings),
		"oxi_wins": oxiWins,
		"pg_wins":  pgWins,
		"timings":  timings,
	}
	jsonBytes, _ := json.MarshalIndent(summary, "", "  ")
	os.WriteFile("report.json", jsonBytes, 0644)
}

// MarshalJSON for TimingEntry so report.json is readable.
func (e TimingEntry) MarshalJSON() ([]byte, error) {
	type alias struct {
		Category  string  `json:"category"`
		Name      string  `json:"name"`
		OxiMs     float64 `json:"oxi_ms"`
		PgMs      float64 `json:"pg_ms"`
		Speedup   string  `json:"speedup"`
		OxiCount  int     `json:"oxi_count"`
		PgCount   int     `json:"pg_count"`
		Winner    string  `json:"winner"`
		OxiErr    string  `json:"oxi_err,omitempty"`
		PgErr     string  `json:"pg_err,omitempty"`
		OxiDetail string  `json:"oxi_detail,omitempty"`
		PgDetail  string  `json:"pg_detail,omitempty"`
	}
	winner := "tie"
	speedup := "1.0x"
	if e.PgDur == 0 || e.OxiDur == 0 {
		winner = "n/a"
		speedup = "n/a"
	} else if e.OxiDur < e.PgDur {
		winner = "oxidb"
		speedup = fmt.Sprintf("%.1fx", float64(e.PgDur)/float64(e.OxiDur))
	} else if e.PgDur < e.OxiDur {
		winner = "postgresql"
		speedup = fmt.Sprintf("%.1fx", float64(e.OxiDur)/float64(e.PgDur))
	}
	return json.Marshal(alias{
		Category:  e.Category,
		Name:      e.Name,
		OxiMs:     float64(e.OxiDur.Microseconds()) / 1000.0,
		PgMs:      float64(e.PgDur.Microseconds()) / 1000.0,
		Speedup:   speedup,
		OxiCount:  e.OxiCount,
		PgCount:   e.PgCount,
		Winner:    winner,
		OxiErr:    e.OxiErr,
		PgErr:     e.PgErr,
		OxiDetail: e.OxiDetail,
		PgDetail:  e.PgDetail,
	})
}
