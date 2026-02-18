package service

import (
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/parisxmas/OxiDB/OxiDMS/internal/db"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/models"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/repository"
)

type FormService struct {
	forms *repository.FormRepo
	pool  *db.Pool
}

func NewFormService(forms *repository.FormRepo, pool *db.Pool) *FormService {
	return &FormService{forms: forms, pool: pool}
}

func (s *FormService) Create(name, description, createdBy string, fields []map[string]any) (*models.Form, error) {
	if name == "" {
		return nil, errors.New("form name is required")
	}
	if len(fields) == 0 {
		return nil, errors.New("at least one field is required")
	}

	slug := generateSlug(name)

	// Check slug uniqueness
	existing, _ := s.forms.FindBySlug(slug)
	if existing != nil {
		slug = slug + "-" + time.Now().Format("20060102150405")
	}

	now := time.Now().UTC().Format(time.RFC3339)
	form := &models.Form{
		Name:        name,
		Slug:        slug,
		Description: description,
		Fields:      fields,
		CreatedBy:   createdBy,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	id, err := s.forms.Create(form)
	if err != nil {
		return nil, err
	}
	form.ID = id

	// Create indexes for indexed fields
	c := s.pool.Get()
	for _, f := range form.TypedFields() {
		if f.Indexed {
			c.CreateIndex(repository.SubmissionsCollection, "data."+f.Name)
		}
	}

	return form, nil
}

func (s *FormService) List() ([]models.Form, error) {
	return s.forms.FindAll()
}

func (s *FormService) Get(id string) (*models.Form, error) {
	form, err := s.forms.FindByID(id)
	if err != nil {
		return nil, err
	}
	if form == nil {
		return nil, errors.New("form not found")
	}
	return form, nil
}

func (s *FormService) Update(id, name, description string, fields []map[string]any) (*models.Form, error) {
	form, err := s.forms.FindByID(id)
	if err != nil {
		return nil, err
	}
	if form == nil {
		return nil, errors.New("form not found")
	}

	if name != "" {
		form.Name = name
	}
	form.Description = description
	if len(fields) > 0 {
		form.Fields = fields
	}
	form.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	if err := s.forms.Update(id, form); err != nil {
		return nil, err
	}
	return form, nil
}

func (s *FormService) Delete(id string) error {
	form, err := s.forms.FindByID(id)
	if err != nil {
		return err
	}
	if form == nil {
		return errors.New("form not found")
	}
	return s.forms.Delete(id)
}

var nonAlphaNum = regexp.MustCompile(`[^a-z0-9]+`)

func generateSlug(name string) string {
	slug := strings.ToLower(strings.TrimSpace(name))
	slug = nonAlphaNum.ReplaceAllString(slug, "-")
	slug = strings.Trim(slug, "-")
	if slug == "" {
		slug = "form"
	}
	return slug
}
