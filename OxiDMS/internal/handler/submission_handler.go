package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/auth"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/service"
)

type SubmissionHandler struct {
	subSvc *service.SubmissionService
	docSvc *service.DocumentService
}

func NewSubmissionHandler(subSvc *service.SubmissionService, docSvc *service.DocumentService) *SubmissionHandler {
	return &SubmissionHandler{subSvc: subSvc, docSvc: docSvc}
}

func (h *SubmissionHandler) List(w http.ResponseWriter, r *http.Request) {
	formID := chi.URLParam(r, "formId")
	skip, _ := strconv.Atoi(r.URL.Query().Get("skip"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit == 0 {
		limit = 20
	}

	subs, total, err := h.subSvc.List(formID, skip, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"submissions": subs,
		"total":       total,
		"skip":        skip,
		"limit":       limit,
	})
}

func (h *SubmissionHandler) Create(w http.ResponseWriter, r *http.Request) {
	formID := chi.URLParam(r, "formId")
	claims := auth.GetUser(r.Context())

	// Parse multipart — supports files + JSON data
	if err := r.ParseMultipartForm(12 << 20); err != nil {
		// Not multipart — try JSON body
		var req struct {
			Data  map[string]any `json:"data"`
			Files []string       `json:"files"`
		}
		if err := readJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		sub, err := h.subSvc.Create(formID, req.Data, req.Files, claims.UserID)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, sub)
		return
	}

	// Multipart: parse data field as JSON
	var data map[string]any
	dataStr := r.FormValue("data")
	if dataStr != "" {
		if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
			writeError(w, http.StatusBadRequest, "invalid data JSON")
			return
		}
	}
	if data == nil {
		data = map[string]any{}
	}

	// Upload attached files
	var fileIDs []string
	if r.MultipartForm != nil && r.MultipartForm.File != nil {
		for _, fileHeaders := range r.MultipartForm.File {
			for _, fh := range fileHeaders {
				f, err := fh.Open()
				if err != nil {
					continue
				}
				fileData, err := io.ReadAll(f)
				f.Close()
				if err != nil {
					continue
				}
				doc, err := h.docSvc.Upload(fh.Filename, fileData, fh.Header.Get("Content-Type"), formID, "", claims.UserID)
				if err != nil {
					continue
				}
				fileIDs = append(fileIDs, doc.ID)
			}
		}
	}

	sub, err := h.subSvc.Create(formID, data, fileIDs, claims.UserID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, sub)
}

func (h *SubmissionHandler) Get(w http.ResponseWriter, r *http.Request) {
	subID := chi.URLParam(r, "subId")
	sub, err := h.subSvc.Get(subID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	docs, _ := h.docSvc.ListBySubmission(subID)
	writeJSON(w, http.StatusOK, map[string]any{
		"submission": sub,
		"documents":  docs,
	})
}

func (h *SubmissionHandler) Update(w http.ResponseWriter, r *http.Request) {
	subID := chi.URLParam(r, "subId")
	var req struct {
		Data map[string]any `json:"data"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	sub, err := h.subSvc.Update(subID, req.Data)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

func (h *SubmissionHandler) Delete(w http.ResponseWriter, r *http.Request) {
	subID := chi.URLParam(r, "subId")
	if err := h.subSvc.Delete(subID); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": subID})
}
