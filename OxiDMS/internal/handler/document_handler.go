package handler

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/auth"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/service"
)

type DocumentHandler struct {
	svc *service.DocumentService
}

func NewDocumentHandler(svc *service.DocumentService) *DocumentHandler {
	return &DocumentHandler{svc: svc}
}

func (h *DocumentHandler) List(w http.ResponseWriter, r *http.Request) {
	skip, _ := strconv.Atoi(r.URL.Query().Get("skip"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit == 0 {
		limit = 20
	}

	docs, total, err := h.svc.List(skip, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"docs":  docs,
		"total": total,
		"skip":  skip,
		"limit": limit,
	})
}

func (h *DocumentHandler) Upload(w http.ResponseWriter, r *http.Request) {
	// Max 12MB
	r.ParseMultipartForm(12 << 20)

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "file is required")
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read file")
		return
	}

	formID := r.FormValue("formId")
	submissionID := r.FormValue("submissionId")
	claims := auth.GetUser(r.Context())

	doc, err := h.svc.Upload(header.Filename, data, header.Header.Get("Content-Type"), formID, submissionID, claims.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, doc)
}

func (h *DocumentHandler) Download(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "docId")
	data, doc, err := h.svc.Download(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	w.Header().Set("Content-Type", doc.ContentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`inline; filename="%s"`, doc.FileName))
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Write(data)
}

func (h *DocumentHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "docId")
	if err := h.svc.Delete(id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": id})
}
