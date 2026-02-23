package router

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/auth"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/handler"
	mw "github.com/parisxmas/OxiDB/OxiDMS/internal/middleware"
)

func New(
	jwtSecret string,
	authH *handler.AuthHandler,
	formH *handler.FormHandler,
	subH *handler.SubmissionHandler,
	docH *handler.DocumentHandler,
	searchH *handler.SearchHandler,
	dashH *handler.DashboardHandler,
	adminH *handler.AdminHandler,
) *chi.Mux {
	r := chi.NewRouter()

	// Global middleware
	r.Use(mw.Recovery)
	r.Use(mw.Logger)
	r.Use(mw.CORS)

	r.Route("/api/v1", func(r chi.Router) {
		// Public routes
		r.Post("/auth/login", authH.Login)
		r.Post("/auth/register", authH.Register)

		// Protected routes
		r.Group(func(r chi.Router) {
			r.Use(auth.Middleware(jwtSecret))

			// Auth
			r.Get("/auth/me", authH.Me)

			// Dashboard
			r.Get("/dashboard", dashH.Dashboard)

			// Forms
			r.Get("/forms", formH.List)
			r.Post("/forms", formH.Create)
			r.Get("/forms/{formId}", formH.Get)
			r.Put("/forms/{formId}", formH.Update)
			r.Delete("/forms/{formId}", formH.Delete)

			// Submissions
			r.Get("/forms/{formId}/submissions", subH.List)
			r.Post("/forms/{formId}/submissions", subH.Create)
			r.Get("/forms/{formId}/submissions/{subId}", subH.Get)
			r.Put("/forms/{formId}/submissions/{subId}", subH.Update)
			r.Delete("/forms/{formId}/submissions/{subId}", subH.Delete)

			// Documents
			r.Get("/documents", docH.List)
			r.Post("/documents", docH.Upload)
			r.Get("/documents/{docId}/download", docH.Download)
			r.Delete("/documents/{docId}", docH.Delete)

			// Search
			r.Post("/search", searchH.Search)

			// Admin
			r.Get("/admin/indexes", adminH.ListIndexes)
			r.Post("/admin/compact", adminH.Compact)
		})
	})

	// Serve frontend SPA from /var/www/oxidms (falls back to index.html)
	staticDir := getEnv("DMS_STATIC_DIR", "/var/www/oxidms")
	spaHandler(r, staticDir)

	return r
}

func spaHandler(r *chi.Mux, staticDir string) {
	fs := http.Dir(staticDir)
	fileServer := http.FileServer(fs)

	r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// If path has a file extension and the file exists, serve it directly
		if strings.Contains(filepath.Base(path), ".") {
			if f, err := os.Stat(filepath.Join(staticDir, path)); err == nil && !f.IsDir() {
				fileServer.ServeHTTP(w, r)
				return
			}
		}

		// SPA fallback: serve index.html for all other routes
		http.ServeFile(w, r, filepath.Join(staticDir, "index.html"))
	})
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
