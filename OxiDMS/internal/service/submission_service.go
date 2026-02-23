package service

import (
	"errors"
	"time"

	"github.com/parisxmas/OxiDB/OxiDMS/internal/models"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/repository"
)

type SubmissionService struct {
	subs  *repository.SubmissionRepo
	forms *repository.FormRepo
}

func NewSubmissionService(subs *repository.SubmissionRepo, forms *repository.FormRepo) *SubmissionService {
	return &SubmissionService{subs: subs, forms: forms}
}

func (s *SubmissionService) Create(formID string, data map[string]any, fileIDs []string, createdBy string) (*models.Submission, error) {
	form, err := s.forms.FindByID(formID)
	if err != nil {
		return nil, err
	}
	if form == nil {
		return nil, errors.New("form not found")
	}

	// Validate required fields
	for _, f := range form.TypedFields() {
		if f.Required && f.Type != "file" {
			val, exists := data[f.Name]
			if !exists || val == nil || val == "" {
				return nil, errors.New("required field missing: " + f.Label)
			}
		}
	}

	now := time.Now().UTC().Format(time.RFC3339)
	sub := &models.Submission{
		FormID:    formID,
		Data:      data,
		Files:     fileIDs,
		CreatedBy: createdBy,
		CreatedAt: now,
		UpdatedAt: now,
	}

	id, err := s.subs.Create(sub)
	if err != nil {
		return nil, err
	}
	sub.ID = id
	return sub, nil
}

func (s *SubmissionService) List(formID string, skip, limit int) ([]models.Submission, int, error) {
	return s.subs.FindByFormID(formID, skip, limit)
}

func (s *SubmissionService) Get(id string) (*models.Submission, error) {
	sub, err := s.subs.FindByID(id)
	if err != nil {
		return nil, err
	}
	if sub == nil {
		return nil, errors.New("submission not found")
	}
	return sub, nil
}

func (s *SubmissionService) Update(id string, data map[string]any) (*models.Submission, error) {
	sub, err := s.subs.FindByID(id)
	if err != nil {
		return nil, err
	}
	if sub == nil {
		return nil, errors.New("submission not found")
	}

	sub.Data = data
	sub.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	if err := s.subs.Update(id, sub); err != nil {
		return nil, err
	}
	return sub, nil
}

func (s *SubmissionService) Delete(id string) error {
	sub, err := s.subs.FindByID(id)
	if err != nil {
		return err
	}
	if sub == nil {
		return errors.New("submission not found")
	}
	return s.subs.Delete(id)
}

func (s *SubmissionService) CountByForm(formID string) (int, error) {
	return s.subs.CountByFormID(formID)
}
