package router

import (
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
		})
	})

	return r
}
