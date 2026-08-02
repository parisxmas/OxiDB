package service

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/models"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/repository"
)

type DocumentService struct {
	docs *repository.DocumentRepo
}

func NewDocumentService(docs *repository.DocumentRepo) *DocumentService {
	return &DocumentService{docs: docs}
}

func (s *DocumentService) Upload(fileName string, data []byte, contentType, formID, submissionID, uploadedBy string) (*models.Document, error) {
	if len(data) == 0 {
		return nil, errors.New("file data is empty")
	}

	if contentType == "" {
		contentType = detectContentType(fileName)
	}

	blobKey := fmt.Sprintf("%s_%s", uuid.New().String(), fileName)

	if err := s.docs.PutBlob(blobKey, data, contentType); err != nil {
		return nil, fmt.Errorf("upload blob: %w", err)
	}

	doc := &models.Document{
		FileName:     fileName,
		ContentType:  contentType,
		Size:         int64(len(data)),
		BlobKey:      blobKey,
		FormID:       formID,
		SubmissionID: submissionID,
		UploadedBy:   uploadedBy,
		CreatedAt:    time.Now().UTC().Format(time.RFC3339),
	}

	id, err := s.docs.Create(doc)
	if err != nil {
		return nil, err
	}
	doc.ID = id
	return doc, nil
}

func (s *DocumentService) Download(id string) ([]byte, *models.Document, error) {
	doc, err := s.docs.FindByID(id)
	if err != nil {
		return nil, nil, err
	}
	if doc == nil {
		return nil, nil, errors.New("document not found")
	}

	data, _, err := s.docs.GetBlob(doc.BlobKey)
	if err != nil {
		return nil, nil, fmt.Errorf("download blob: %w", err)
	}
	return data, doc, nil
}

func (s *DocumentService) List(skip, limit int) ([]models.Document, int, error) {
	return s.docs.FindAll(skip, limit)
}

func (s *DocumentService) ListBySubmission(submissionID string) ([]models.Document, error) {
	return s.docs.FindBySubmission(submissionID)
}

func (s *DocumentService) Delete(id string) error {
	doc, err := s.docs.FindByID(id)
	if err != nil {
		return err
	}
	if doc == nil {
		return errors.New("document not found")
	}
	s.docs.DeleteBlob(doc.BlobKey)
	return s.docs.Delete(id)
}

func (s *DocumentService) Count() (int, error) {
	return s.docs.CountAll()
}

func detectContentType(fileName string) string {
	ext := strings.ToLower(filepath.Ext(fileName))
	types := map[string]string{
		".pdf":  "application/pdf",
		".png":  "image/png",
		".jpg":  "image/jpeg",
		".jpeg": "image/jpeg",
		".gif":  "image/gif",
		".svg":  "image/svg+xml",
		".doc":  "application/msword",
		".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		".xls":  "application/vnd.ms-excel",
		".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		".csv":  "text/csv",
		".txt":  "text/plain",
		".json": "application/json",
		".xml":  "application/xml",
		".zip":  "application/zip",
	}
	if ct, ok := types[ext]; ok {
		return ct
	}
	return "application/octet-stream"
}
