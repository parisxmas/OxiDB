package handler

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/auth"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/service"
)

type FormHandler struct {
	svc *service.FormService
}

func NewFormHandler(svc *service.FormService) *FormHandler {
	return &FormHandler{svc: svc}
}

func (h *FormHandler) List(w http.ResponseWriter, r *http.Request) {
	forms, err := h.svc.List()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, forms)
}

func (h *FormHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name        string           `json:"name"`
		Description string           `json:"description"`
		Fields      []map[string]any `json:"fields"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	claims := auth.GetUser(r.Context())
	form, err := h.svc.Create(req.Name, req.Description, claims.UserID, req.Fields)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, form)
}

func (h *FormHandler) Get(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "formId")
	form, err := h.svc.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, form)
}

func (h *FormHandler) Update(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "formId")
	var req struct {
		Name        string           `json:"name"`
		Description string           `json:"description"`
		Fields      []map[string]any `json:"fields"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	form, err := h.svc.Update(id, req.Name, req.Description, req.Fields)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, form)
}

func (h *FormHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "formId")
	if err := h.svc.Delete(id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": id})
}
