package repository

import (
	"encoding/json"
	"fmt"

	"github.com/parisxmas/OxiDB/OxiDMS/internal/db"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/models"
	"github.com/parisxmas/OxiDB/go/oxidb"
)

const FormsCollection = "_dms_forms"

type FormRepo struct {
	pool *db.Pool
}

func NewFormRepo(pool *db.Pool) *FormRepo {
	return &FormRepo{pool: pool}
}

func (r *FormRepo) EnsureIndexes() error {
	c := r.pool.Get()
	return c.CreateUniqueIndex(FormsCollection, "slug")
}

func (r *FormRepo) Create(form *models.Form) (string, error) {
	c := r.pool.Get()
	doc := formToDoc(form)
	result, err := c.Insert(FormsCollection, doc)
	if err != nil {
		return "", err
	}
	return extractID(result), nil
}

func (r *FormRepo) FindAll() ([]models.Form, error) {
	c := r.pool.Get()
	docs, err := c.Find(FormsCollection, map[string]any{}, &oxidb.FindOptions{
		Sort: map[string]any{"createdAt": -1},
	})
	if err != nil {
		return nil, err
	}
	forms := make([]models.Form, 0, len(docs))
	for _, d := range docs {
		f, err := docToForm(d)
		if err != nil {
			continue
		}
		forms = append(forms, *f)
	}
	return forms, nil
}

func (r *FormRepo) FindByID(id string) (*models.Form, error) {
	c := r.pool.Get()
	doc, err := c.FindOne(FormsCollection, map[string]any{"_id": toNumericID(id)})
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, nil
	}
	return docToForm(doc)
}

func (r *FormRepo) FindBySlug(slug string) (*models.Form, error) {
	c := r.pool.Get()
	doc, err := c.FindOne(FormsCollection, map[string]any{"slug": slug})
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, nil
	}
	return docToForm(doc)
}

func (r *FormRepo) Update(id string, form *models.Form) error {
	c := r.pool.Get()
	doc := formToDoc(form)
	_, err := c.Update(FormsCollection, map[string]any{"_id": toNumericID(id)}, map[string]any{"$set": doc})
	return err
}

func (r *FormRepo) Delete(id string) error {
	c := r.pool.Get()
	_, err := c.Delete(FormsCollection, map[string]any{"_id": toNumericID(id)})
	return err
}

func (r *FormRepo) Count() (int, error) {
	c := r.pool.Get()
	return c.Count(FormsCollection, map[string]any{})
}

func formToDoc(f *models.Form) map[string]any {
	data, _ := json.Marshal(f)
	var doc map[string]any
	json.Unmarshal(data, &doc)
	delete(doc, "_id")
	return doc
}

func docToForm(doc map[string]any) (*models.Form, error) {
	normalizeID(doc)
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal form doc: %w", err)
	}
	var f models.Form
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("unmarshal form: %w", err)
	}
	return &f, nil
}
