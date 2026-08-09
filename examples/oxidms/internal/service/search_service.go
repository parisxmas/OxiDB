package service

import (
	"encoding/json"
	"strconv"
	"sync"

	"github.com/parisxmas/OxiDB/OxiDMS/internal/db"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/models"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/repository"
	"github.com/parisxmas/OxiDB/go/oxidb"
)

type SearchService struct {
	pool *db.Pool
	subs *repository.SubmissionRepo
}

func NewSearchService(pool *db.Pool, subs *repository.SubmissionRepo) *SearchService {
	return &SearchService{pool: pool, subs: subs}
}

type SearchRequest struct {
	FormID    string                       `json:"formId"`
	Filters   map[string]FilterDescriptor  `json:"filters,omitempty"`
	TextQuery string                       `json:"textQuery,omitempty"`
	Skip      int                          `json:"skip"`
	Limit     int                          `json:"limit"`
}

type FilterDescriptor struct {
	Value any     `json:"value,omitempty"`
	Min   any     `json:"min,omitempty"`
	Max   any     `json:"max,omitempty"`
	Op    string  `json:"op,omitempty"` // eq, ne, gt, gte, lt, lte, in
}

type SearchResult struct {
	Docs  []map[string]any `json:"docs"`
	Total int              `json:"total"`
	Mode  string           `json:"mode"`
}

func (s *SearchService) Search(req SearchRequest) (*SearchResult, error) {
	c := s.pool.Get()

	if req.Limit == 0 {
		req.Limit = 20
	}

	hasFilters := len(req.Filters) > 0
	hasText := req.TextQuery != ""

	// Mode 1: Structured only — run Find and Count in parallel
	if hasFilters && !hasText {
		query := buildQuery(req.FormID, req.Filters)
		var docs []map[string]any
		var total int
		var findErr, countErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			c1 := s.pool.Get()
			docs, findErr = c1.Find(repository.SubmissionsCollection, query, &oxidb.FindOptions{
				Skip:  &req.Skip,
				Limit: &req.Limit,
				Sort:  map[string]any{"createdAt": -1},
			})
		}()
		go func() {
			defer wg.Done()
			c2 := s.pool.Get()
			total, countErr = c2.Count(repository.SubmissionsCollection, query)
		}()
		wg.Wait()
		if findErr != nil {
			return nil, findErr
		}
		if countErr != nil {
			return nil, countErr
		}
		return &SearchResult{Docs: docs, Total: total, Mode: "structured"}, nil
	}

	// Mode 2: FTS only — use TextSearch directly on submissions collection
	if !hasFilters && hasText {
		subs, err := s.subs.TextSearch(req.TextQuery, req.Skip+req.Limit)
		if err != nil {
			return nil, err
		}

		total := len(subs)
		// Apply pagination
		end := req.Skip + req.Limit
		if end > total {
			end = total
		}
		docs := make([]map[string]any, 0)
		if req.Skip < total {
			for _, sub := range subs[req.Skip:end] {
				doc := submissionToMap(sub)
				docs = append(docs, doc)
			}
		}
		return &SearchResult{Docs: docs, Total: total, Mode: "fts"}, nil
	}

	// Mode 3: Combined — TextSearch + structured filter intersection
	if hasFilters && hasText {
		subs, err := s.subs.TextSearch(req.TextQuery, 500)
		if err != nil {
			return nil, err
		}

		structuredQuery := buildQuery(req.FormID, req.Filters)
		docs := make([]map[string]any, 0)
		for _, sub := range subs {
			// Check if this submission matches the structured filters
			combined := map[string]any{
				"$and": []any{
					map[string]any{"_id": toNumericID(sub.ID)},
					structuredQuery,
				},
			}
			match, err := c.FindOne(repository.SubmissionsCollection, combined)
			if err != nil || match == nil {
				continue
			}
			docs = append(docs, match)
			if len(docs) >= req.Skip+req.Limit {
				break
			}
		}

		paged := docs
		if req.Skip < len(docs) {
			end := req.Skip + req.Limit
			if end > len(docs) {
				end = len(docs)
			}
			paged = docs[req.Skip:end]
		} else {
			paged = nil
		}
		return &SearchResult{Docs: paged, Total: len(docs), Mode: "combined"}, nil
	}

	// No filters, no text — return all for form (parallel Find + Count)
	query := map[string]any{}
	if req.FormID != "" {
		query["formId"] = req.FormID
	}
	var docs []map[string]any
	var total int
	var findErr, countErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		c1 := s.pool.Get()
		docs, findErr = c1.Find(repository.SubmissionsCollection, query, &oxidb.FindOptions{
			Skip:  &req.Skip,
			Limit: &req.Limit,
			Sort:  map[string]any{"createdAt": -1},
		})
	}()
	go func() {
		defer wg.Done()
		c2 := s.pool.Get()
		total, countErr = c2.Count(repository.SubmissionsCollection, query)
	}()
	wg.Wait()
	if findErr != nil {
		return nil, findErr
	}
	if countErr != nil {
		return nil, countErr
	}
	return &SearchResult{Docs: docs, Total: total, Mode: "all"}, nil
}

func buildQuery(formID string, filters map[string]FilterDescriptor) map[string]any {
	conditions := []any{}

	if formID != "" {
		conditions = append(conditions, map[string]any{"formId": formID})
	}

	for field, filter := range filters {
		dataField := "data." + field

		// Range filter
		if filter.Min != nil || filter.Max != nil {
			if filter.Min != nil && filter.Min != "" {
				conditions = append(conditions, map[string]any{dataField: map[string]any{"$gte": filter.Min}})
			}
			if filter.Max != nil && filter.Max != "" {
				conditions = append(conditions, map[string]any{dataField: map[string]any{"$lte": filter.Max}})
			}
			continue
		}

		// Simple value filter
		if filter.Value != nil && filter.Value != "" {
			conditions = append(conditions, map[string]any{dataField: filter.Value})
		}
	}

	if len(conditions) == 0 {
		return map[string]any{}
	}
	if len(conditions) == 1 {
		return conditions[0].(map[string]any)
	}
	return map[string]any{"$and": conditions}
}

func submissionToMap(s models.Submission) map[string]any {
	data, _ := json.Marshal(s)
	var m map[string]any
	json.Unmarshal(data, &m)
	return m
}

func toNumericID(id string) any {
	if n, err := strconv.ParseFloat(id, 64); err == nil {
		return n
	}
	return id
}
