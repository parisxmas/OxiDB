package oxidb

// Time-series engine (oxidb-tsdb) client. Reachable when the server is started
// with OXIDB_TSDB=1. Entirely separate from document collections and the SQL
// engine — its own compressed columnar storage.
//
//	ts := c.TSDB()
//	ts.Write(oxidb.TSPoint{
//	    Measurement: "cpu",
//	    Tags:        map[string]string{"host": "a"},
//	    Fields:      map[string]any{"usage": 0.9},
//	    TimestampMS: time.Now().UnixMilli(),
//	})
//	series, _ := ts.Query(oxidb.TSQuery{Measurement: "cpu", Field: "usage", Agg: "mean"})

// TSDBClient is a handle to the time-series engine, optionally scoped to a
// database. Obtain one with Client.TSDB().
type TSDBClient struct {
	c  *Client
	db string
}

// TSDB returns a time-series engine handle bound to the session's current
// database.
func (c *Client) TSDB() *TSDBClient { return &TSDBClient{c: c} }

// DB returns a copy of the handle scoped to a specific database.
func (t *TSDBClient) DB(name string) *TSDBClient {
	return &TSDBClient{c: t.c, db: name}
}

// TSPoint is one time-series sample. Fields may hold float64, int64/int, bool,
// or string values (mapped to float/integer/boolean/string fields).
type TSPoint struct {
	Measurement string
	Tags        map[string]string
	Fields      map[string]any
	TimestampMS int64
}

// TSQuery describes a time-series query.
type TSQuery struct {
	Measurement string
	Field       string
	// Tags filters (all AND together).
	Tags map[string]string
	// Half-open time range in epoch ms. Zero means unbounded.
	Start, End int64
	// Group by these tag keys.
	GroupBy []string
	// Downsample bucket width in ms; 0 = a single bucket.
	IntervalMS int64
	// Aggregation: mean/sum/min/max/count/distinct/first/last/rate/percentile,
	// or the shorthand p95/p99. Defaults to mean.
	Agg string
	// Percentile parameter for Agg=="percentile" (0..=100).
	P float64
}

// TSResultPoint is one aggregated output point. Value is a float64 (numeric
// aggs) or string (first/last on a text field).
type TSResultPoint struct {
	TS    int64
	Value any
}

// TSSeries is one output group.
type TSSeries struct {
	Tags   map[string]string
	Type   string // "float" | "integer" | "boolean" | "string"
	Points []TSResultPoint
}

func (t *TSDBClient) payload(op string) map[string]any {
	p := map[string]any{"engine": "tsdb", "cmd": "tsdb", "op": op}
	if t.db != "" {
		p["db"] = t.db
	}
	return p
}

// Write ingests one or more points. Returns the number written.
func (t *TSDBClient) Write(points ...TSPoint) (int, error) {
	arr := make([]map[string]any, 0, len(points))
	for _, pt := range points {
		m := map[string]any{"measurement": pt.Measurement, "ts": pt.TimestampMS, "fields": pt.Fields}
		if len(pt.Tags) > 0 {
			m["tags"] = pt.Tags
		}
		arr = append(arr, m)
	}
	p := t.payload("write")
	p["points"] = arr
	return writtenCount(t.c.checked(p))
}

// WriteLineProtocol ingests InfluxDB line protocol (ms timestamps; the server's
// clock is used for lines without one). Returns the number of points written.
func (t *TSDBClient) WriteLineProtocol(lp string) (int, error) {
	p := t.payload("write_lp")
	p["lp"] = lp
	return writtenCount(t.c.checked(p))
}

// Query runs a time-series query and returns one series per output group.
func (t *TSDBClient) Query(q TSQuery) ([]TSSeries, error) {
	p := t.payload("query")
	p["measurement"] = q.Measurement
	p["field"] = q.Field
	if len(q.Tags) > 0 {
		p["tags"] = q.Tags
	}
	if q.Start != 0 {
		p["start"] = q.Start
	}
	if q.End != 0 {
		p["end"] = q.End
	}
	if len(q.GroupBy) > 0 {
		p["group_by"] = q.GroupBy
	}
	if q.IntervalMS > 0 {
		p["interval"] = q.IntervalMS
	}
	if q.Agg != "" {
		p["agg"] = q.Agg
	}
	if q.P != 0 {
		p["p"] = q.P
	}
	data, err := t.c.checked(p)
	if err != nil {
		return nil, err
	}
	items, _ := data.([]any)
	out := make([]TSSeries, 0, len(items))
	for _, it := range items {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		var s TSSeries
		s.Type, _ = m["type"].(string)
		if tags, ok := m["tags"].(map[string]any); ok {
			s.Tags = make(map[string]string, len(tags))
			for k, v := range tags {
				if sv, ok := v.(string); ok {
					s.Tags[k] = sv
				}
			}
		}
		if pts, ok := m["points"].([]any); ok {
			for _, pv := range pts {
				pm, ok := pv.(map[string]any)
				if !ok {
					continue
				}
				var rp TSResultPoint
				if ts, ok := pm["ts"].(float64); ok {
					rp.TS = int64(ts)
				}
				rp.Value = pm["value"]
				s.Points = append(s.Points, rp)
			}
		}
		out = append(out, s)
	}
	return out, nil
}

// Stats returns {series, points, bytes}.
func (t *TSDBClient) Stats() (map[string]any, error) {
	data, err := t.c.checked(t.payload("stats"))
	if err != nil {
		return nil, err
	}
	m, _ := data.(map[string]any)
	return m, nil
}

// Retention drops whole blocks older than cutoffMS. Returns points removed.
func (t *TSDBClient) Retention(cutoffMS int64) (int, error) {
	p := t.payload("retention")
	p["cutoff"] = cutoffMS
	data, err := t.c.checked(p)
	if err != nil {
		return 0, err
	}
	m, _ := data.(map[string]any)
	n, _ := m["removed"].(float64)
	return int(n), nil
}

// Checkpoint forces a durable snapshot.
func (t *TSDBClient) Checkpoint() error {
	_, err := t.c.checked(t.payload("checkpoint"))
	return err
}

// AddRollup defines a continuous-aggregate rule: roll every numeric series of
// measurement into a derived measurement "<measurement>@<label>" at intervalMS,
// materializing the named aggs (e.g. []string{"mean","max","count"}).
func (t *TSDBClient) AddRollup(measurement, label string, intervalMS int64, aggs []string) error {
	p := t.payload("rollup_add")
	p["measurement"] = measurement
	if label != "" {
		p["label"] = label
	}
	p["interval"] = intervalMS
	if len(aggs) > 0 {
		p["aggs"] = aggs
	}
	_, err := t.c.checked(p)
	return err
}

// RefreshRollups materializes completed buckets for all rules. Returns the
// number of rollup points written.
func (t *TSDBClient) RefreshRollups() (int, error) {
	return writtenCount(t.c.checked(t.payload("rollup_refresh")))
}

// Rollups lists the registered rollup rules.
func (t *TSDBClient) Rollups() ([]map[string]any, error) {
	data, err := t.c.checked(t.payload("rollups"))
	if err != nil {
		return nil, err
	}
	items, _ := data.([]any)
	out := make([]map[string]any, 0, len(items))
	for _, it := range items {
		if m, ok := it.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out, nil
}

func writtenCount(data any, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	m, _ := data.(map[string]any)
	n, _ := m["written"].(float64)
	return int(n), nil
}
