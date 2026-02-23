package handler

import (
	"net/http"

	"github.com/parisxmas/OxiDB/OxiDMS/internal/repository"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/service"
)

type DashboardHandler struct {
	formSvc *service.FormService
	subSvc  *service.SubmissionService
	docSvc  *service.DocumentService
	formRepo *repository.FormRepo
}

func NewDashboardHandler(formSvc *service.FormService, subSvc *service.SubmissionService, docSvc *service.DocumentService, formRepo *repository.FormRepo) *DashboardHandler {
	return &DashboardHandler{formSvc: formSvc, subSvc: subSvc, docSvc: docSvc, formRepo: formRepo}
}

func (h *DashboardHandler) Dashboard(w http.ResponseWriter, r *http.Request) {
	forms, _ := h.formSvc.List()
	formCount := len(forms)

	totalSubs := 0
	formStats := make([]map[string]any, 0, len(forms))
	for _, f := range forms {
		count, _ := h.subSvc.CountByForm(f.ID)
		totalSubs += count
		formStats = append(formStats, map[string]any{
			"id":              f.ID,
			"name":            f.Name,
			"slug":            f.Slug,
			"submissionCount": count,
			"fieldCount":      len(f.Fields),
			"createdAt":       f.CreatedAt,
		})
	}

	docCount, _ := h.docSvc.Count()

	writeJSON(w, http.StatusOK, map[string]any{
		"formCount":       formCount,
		"submissionCount": totalSubs,
		"documentCount":   docCount,
		"forms":           formStats,
	})
}
