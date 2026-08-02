package handler

import (
	"net/http"

	"github.com/parisxmas/OxiDB/OxiDMS/internal/repository"
)

type AdminHandler struct {
	subRepo *repository.SubmissionRepo
}

func NewAdminHandler(subRepo *repository.SubmissionRepo) *AdminHandler {
	return &AdminHandler{subRepo: subRepo}
}

func (h *AdminHandler) ListIndexes(w http.ResponseWriter, r *http.Request) {
	indexes, err := h.subRepo.ListIndexes()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"indexes": indexes})
}

func (h *AdminHandler) Compact(w http.ResponseWriter, r *http.Request) {
	stats, err := h.subRepo.Compact()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, stats)
}
