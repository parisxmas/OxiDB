package service

import (
	"github.com/parisxmas/OxiDB/OxiDMS/internal/db"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/repository"
	"github.com/parisxmas/OxiDB/go/oxidb"
)

type SearchService struct {
	pool *db.Pool
}

func NewSearchService(pool *db.Pool) *SearchService {
	return &SearchService{pool: pool}
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

	// Mode 1: Structured only
	if hasFilters && !hasText {
		query := buildQuery(req.FormID, req.Filters)
		docs, err := c.Find(repository.SubmissionsCollection, query, &oxidb.FindOptions{
			Skip:  &req.Skip,
			Limit: &req.Limit,
			Sort:  map[string]any{"createdAt": -1},
		})
		if err != nil {
			return nil, err
		}
		total, _ := c.Count(repository.SubmissionsCollection, query)
		return &SearchResult{Docs: docs, Total: total, Mode: "structured"}, nil
	}

	// Mode 2: FTS only
	if !hasFilters && hasText {
		bucket := repository.BlobBucket
		ftsResults, err := c.Search(req.TextQuery, &bucket, 500)
		if err != nil {
			return nil, err
		}

		// Look up submission docs that match FTS hits
		docs := make([]map[string]any, 0)
		for i, hit := range ftsResults {
			if i < req.Skip {
				continue
			}
			if len(docs) >= req.Limit {
				break
			}
			key, _ := hit["key"].(string)
			if key == "" {
				continue
			}
			// Find document metadata by blobKey, then find submission
			docMeta, err := c.FindOne(repository.DocumentsCollection, map[string]any{"blobKey": key})
			if err != nil || docMeta == nil {
				continue
			}
			subID, _ := docMeta["submissionId"].(string)
			if subID == "" {
				continue
			}
			sub, err := c.FindOne(repository.SubmissionsCollection, map[string]any{"_id": subID})
			if err != nil || sub == nil {
				continue
			}
			score, _ := hit["score"].(float64)
			sub["_score"] = score
			docs = append(docs, sub)
		}
		return &SearchResult{Docs: docs, Total: len(ftsResults), Mode: "fts"}, nil
	}

	// Mode 3: Combined
	if hasFilters && hasText {
		bucket := repository.BlobBucket
		ftsResults, err := c.Search(req.TextQuery, &bucket, 500)
		if err != nil {
			return nil, err
		}

		structuredQuery := buildQuery(req.FormID, req.Filters)
		docs := make([]map[string]any, 0)
		for _, hit := range ftsResults {
			key, _ := hit["key"].(string)
			if key == "" {
				continue
			}
			docMeta, err := c.FindOne(repository.DocumentsCollection, map[string]any{"blobKey": key})
			if err != nil || docMeta == nil {
				continue
			}
			subID, _ := docMeta["submissionId"].(string)
			if subID == "" {
				continue
			}
			// Check submission matches structured filters
			combined := map[string]any{
				"$and": []any{
					map[string]any{"_id": subID},
					structuredQuery,
				},
			}
			sub, err := c.FindOne(repository.SubmissionsCollection, combined)
			if err != nil || sub == nil {
				continue
			}
			score, _ := hit["score"].(float64)
			sub["_score"] = score
			docs = append(docs, sub)
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

	// No filters, no text — return all for form
	query := map[string]any{}
	if req.FormID != "" {
		query["formId"] = req.FormID
	}
	docs, err := c.Find(repository.SubmissionsCollection, query, &oxidb.FindOptions{
		Skip:  &req.Skip,
		Limit: &req.Limit,
		Sort:  map[string]any{"createdAt": -1},
	})
	if err != nil {
		return nil, err
	}
	total, _ := c.Count(repository.SubmissionsCollection, query)
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
