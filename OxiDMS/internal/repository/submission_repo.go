package repository

import (
	"encoding/json"
	"fmt"

	"github.com/parisxmas/OxiDB/OxiDMS/internal/db"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/models"
	"github.com/parisxmas/OxiDB/go/oxidb"
)

const SubmissionsCollection = "_dms_submissions"

type SubmissionRepo struct {
	pool *db.Pool
}

func NewSubmissionRepo(pool *db.Pool) *SubmissionRepo {
	return &SubmissionRepo{pool: pool}
}

func (r *SubmissionRepo) EnsureIndexes() error {
	c := r.pool.Get()
	if err := c.CreateIndex(SubmissionsCollection, "formId"); err != nil {
		return err
	}
	return c.CreateCompositeIndex(SubmissionsCollection, []string{"formId", "createdAt"})
}

func (r *SubmissionRepo) Create(sub *models.Submission) (string, error) {
	c := r.pool.Get()
	doc := submissionToDoc(sub)
	result, err := c.Insert(SubmissionsCollection, doc)
	if err != nil {
		return "", err
	}
	return extractID(result), nil
}

func (r *SubmissionRepo) FindByFormID(formID string, skip, limit int) ([]models.Submission, int, error) {
	c := r.pool.Get()
	query := map[string]any{"formId": formID}

	total, err := c.Count(SubmissionsCollection, query)
	if err != nil {
		return nil, 0, err
	}

	docs, err := c.Find(SubmissionsCollection, query, &oxidb.FindOptions{
		Sort:  map[string]any{"createdAt": -1},
		Skip:  &skip,
		Limit: &limit,
	})
	if err != nil {
		return nil, 0, err
	}

	subs := make([]models.Submission, 0, len(docs))
	for _, d := range docs {
		s, err := docToSubmission(d)
		if err != nil {
			continue
		}
		subs = append(subs, *s)
	}
	return subs, total, nil
}

func (r *SubmissionRepo) FindByID(id string) (*models.Submission, error) {
	c := r.pool.Get()
	doc, err := c.FindOne(SubmissionsCollection, map[string]any{"_id": toNumericID(id)})
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, nil
	}
	return docToSubmission(doc)
}

func (r *SubmissionRepo) Update(id string, sub *models.Submission) error {
	c := r.pool.Get()
	doc := submissionToDoc(sub)
	_, err := c.UpdateOne(SubmissionsCollection, map[string]any{"_id": toNumericID(id)}, map[string]any{"$set": doc})
	return err
}

func (r *SubmissionRepo) Delete(id string) error {
	c := r.pool.Get()
	_, err := c.DeleteOne(SubmissionsCollection, map[string]any{"_id": toNumericID(id)})
	return err
}

func (r *SubmissionRepo) TextSearch(query string, limit int) ([]models.Submission, error) {
	c := r.pool.Get()
	docs, err := c.TextSearch(SubmissionsCollection, query, limit)
	if err != nil {
		return nil, err
	}
	subs := make([]models.Submission, 0, len(docs))
	for _, d := range docs {
		s, err := docToSubmission(d)
		if err != nil {
			continue
		}
		subs = append(subs, *s)
	}
	return subs, nil
}

func (r *SubmissionRepo) EnsureTextIndex(fields []string) error {
	c := r.pool.Get()
	return c.CreateTextIndex(SubmissionsCollection, fields)
}

func (r *SubmissionRepo) ListIndexes() ([]map[string]any, error) {
	c := r.pool.Get()
	return c.ListIndexes(SubmissionsCollection)
}

func (r *SubmissionRepo) Compact() (map[string]any, error) {
	c := r.pool.Get()
	return c.Compact(SubmissionsCollection)
}

func (r *SubmissionRepo) CountByFormID(formID string) (int, error) {
	c := r.pool.Get()
	return c.Count(SubmissionsCollection, map[string]any{"formId": formID})
}

func submissionToDoc(s *models.Submission) map[string]any {
	data, _ := json.Marshal(s)
	var doc map[string]any
	json.Unmarshal(data, &doc)
	delete(doc, "_id")
	return doc
}

func docToSubmission(doc map[string]any) (*models.Submission, error) {
	normalizeID(doc)
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal submission doc: %w", err)
	}
	var s models.Submission
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("unmarshal submission: %w", err)
	}
	return &s, nil
}
