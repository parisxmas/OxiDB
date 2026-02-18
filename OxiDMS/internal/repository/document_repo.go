package repository

import (
	"encoding/json"
	"fmt"

	"github.com/parisxmas/OxiDB/OxiDMS/internal/db"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/models"
	"github.com/parisxmas/OxiDB/go/oxidb"
)

const (
	DocumentsCollection = "_dms_documents"
	BlobBucket          = "dms_files"
)

type DocumentRepo struct {
	pool *db.Pool
}

func NewDocumentRepo(pool *db.Pool) *DocumentRepo {
	return &DocumentRepo{pool: pool}
}

func (r *DocumentRepo) EnsureIndexes() error {
	c := r.pool.Get()
	if err := c.CreateIndex(DocumentsCollection, "formId"); err != nil {
		return err
	}
	return c.CreateIndex(DocumentsCollection, "submissionId")
}

func (r *DocumentRepo) EnsureBucket() error {
	c := r.pool.Get()
	return c.CreateBucket(BlobBucket)
}

func (r *DocumentRepo) Create(doc *models.Document) (string, error) {
	c := r.pool.Get()
	d := documentToDoc(doc)
	result, err := c.Insert(DocumentsCollection, d)
	if err != nil {
		return "", err
	}
	return extractID(result), nil
}

func (r *DocumentRepo) FindByID(id string) (*models.Document, error) {
	c := r.pool.Get()
	doc, err := c.FindOne(DocumentsCollection, map[string]any{"_id": toNumericID(id)})
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, nil
	}
	return docToDocument(doc)
}

func (r *DocumentRepo) FindAll(skip, limit int) ([]models.Document, int, error) {
	c := r.pool.Get()
	query := map[string]any{}

	total, err := c.Count(DocumentsCollection, query)
	if err != nil {
		return nil, 0, err
	}

	docs, err := c.Find(DocumentsCollection, query, &oxidb.FindOptions{
		Sort:  map[string]any{"createdAt": -1},
		Skip:  &skip,
		Limit: &limit,
	})
	if err != nil {
		return nil, 0, err
	}

	result := make([]models.Document, 0, len(docs))
	for _, d := range docs {
		doc, err := docToDocument(d)
		if err != nil {
			continue
		}
		result = append(result, *doc)
	}
	return result, total, nil
}

func (r *DocumentRepo) FindBySubmission(submissionID string) ([]models.Document, error) {
	c := r.pool.Get()
	docs, err := c.Find(DocumentsCollection, map[string]any{"submissionId": submissionID}, nil)
	if err != nil {
		return nil, err
	}
	result := make([]models.Document, 0, len(docs))
	for _, d := range docs {
		doc, err := docToDocument(d)
		if err != nil {
			continue
		}
		result = append(result, *doc)
	}
	return result, nil
}

func (r *DocumentRepo) Delete(id string) error {
	c := r.pool.Get()
	_, err := c.Delete(DocumentsCollection, map[string]any{"_id": toNumericID(id)})
	return err
}

func (r *DocumentRepo) PutBlob(key string, data []byte, contentType string) error {
	c := r.pool.Get()
	_, err := c.PutObject(BlobBucket, key, data, contentType, nil)
	return err
}

func (r *DocumentRepo) GetBlob(key string) ([]byte, map[string]any, error) {
	c := r.pool.Get()
	return c.GetObject(BlobBucket, key)
}

func (r *DocumentRepo) DeleteBlob(key string) error {
	c := r.pool.Get()
	return c.DeleteObject(BlobBucket, key)
}

func (r *DocumentRepo) CountAll() (int, error) {
	c := r.pool.Get()
	return c.Count(DocumentsCollection, map[string]any{})
}

func documentToDoc(d *models.Document) map[string]any {
	data, _ := json.Marshal(d)
	var doc map[string]any
	json.Unmarshal(data, &doc)
	delete(doc, "_id")
	return doc
}

func docToDocument(doc map[string]any) (*models.Document, error) {
	normalizeID(doc)
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal document doc: %w", err)
	}
	var d models.Document
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("unmarshal document: %w", err)
	}
	return &d, nil
}
