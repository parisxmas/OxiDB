package oxidb_test

import (
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/parisxmas/OxiDB/clients/go/oxidb"
)

// tsdbClient connects to a TSDB-enabled server (OXIDB_TSDB=1). Defaults to the
// demo port 4477; skips the test if unreachable.
func tsdbClient(t *testing.T) *oxidb.Client {
	t.Helper()
	host := "127.0.0.1"
	port := 4477
	if h := os.Getenv("OXIDB_TSDB_HOST"); h != "" {
		host = h
	}
	if p := os.Getenv("OXIDB_TSDB_PORT"); p != "" {
		port, _ = strconv.Atoi(p)
	}
	c, err := oxidb.Connect(host, port, 3*time.Second)
	if err != nil {
		t.Skipf("no TSDB server at %s:%d: %v", host, port, err)
	}
	return c
}

func TestTSDB_WriteQueryRollup(t *testing.T) {
	ts := tsdbClient(t).TSDB().DB("perf")
	base := int64(1_699_999_800_000)

	// Write 5 minutes of 1s samples (usage = second-of-minute), plus int/bool/str.
	pts := make([]oxidb.TSPoint, 0, 300)
	for i := int64(0); i < 300; i++ {
		pts = append(pts, oxidb.TSPoint{
			Measurement: "gocpu",
			Tags:        map[string]string{"host": "g1"},
			Fields:      map[string]any{"usage": float64(i % 60), "cores": int64(8), "up": true},
			TimestampMS: base + i*1000,
		})
	}
	n, err := ts.Write(pts...)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if n != 300 {
		t.Fatalf("written = %d, want 300", n)
	}

	// mean over the whole range ≈ 29.5.
	series, err := ts.Query(oxidb.TSQuery{Measurement: "gocpu", Field: "usage", Agg: "mean"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	v, _ := series[0].Points[0].Value.(float64)
	if math.Abs(v-29.5) > 1e-6 {
		t.Fatalf("mean = %v, want 29.5", v)
	}

	// int field type surfaced.
	s2, _ := ts.Query(oxidb.TSQuery{Measurement: "gocpu", Field: "cores", Agg: "last"})
	if s2[0].Type != "integer" {
		t.Fatalf("cores type = %q, want integer", s2[0].Type)
	}

	// Line protocol ingest.
	if _, err := ts.WriteLineProtocol("golp,host=g1 temp=42.5 " + strconv.FormatInt(base, 10)); err != nil {
		t.Fatalf("write_lp: %v", err)
	}

	// Rollup gocpu → 1m and refresh.
	if err := ts.AddRollup("gocpu", "1m", 60000, []string{"mean", "max", "count"}); err != nil {
		t.Fatalf("rollup_add: %v", err)
	}
	if _, err := ts.RefreshRollups(); err != nil {
		t.Fatalf("rollup_refresh: %v", err)
	}
	roll, err := ts.Query(oxidb.TSQuery{
		Measurement: "gocpu@1m", Field: "usage_mean",
		Start: base, End: base + 300*1000, IntervalMS: 60000, Agg: "last",
	})
	if err != nil {
		t.Fatalf("rollup query: %v", err)
	}
	if len(roll[0].Points) != 5 {
		t.Fatalf("rollup buckets = %d, want 5", len(roll[0].Points))
	}
	for _, p := range roll[0].Points {
		if fv, _ := p.Value.(float64); math.Abs(fv-29.5) > 1e-6 {
			t.Fatalf("rollup mean = %v, want 29.5", fv)
		}
	}
}
