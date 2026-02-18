package main

import (
	"log"
	"net/http"

	"github.com/parisxmas/OxiDB/OxiDMS/internal/config"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/db"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/handler"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/repository"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/router"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/service"
)

func main() {
	cfg := config.Load()

	// Connect to OxiDB
	pool, err := db.NewPool(cfg.OxiDBHost, cfg.OxiDBPort, cfg.PoolSize)
	if err != nil {
		log.Fatalf("Failed to connect to OxiDB: %v", err)
	}
	defer pool.Close()
	log.Printf("Connected to OxiDB at %s:%d (pool size: %d)", cfg.OxiDBHost, cfg.OxiDBPort, cfg.PoolSize)

	// Repositories
	userRepo := repository.NewUserRepo(pool)
	formRepo := repository.NewFormRepo(pool)
	subRepo := repository.NewSubmissionRepo(pool)
	docRepo := repository.NewDocumentRepo(pool)

	// Ensure indexes (ignore errors for already-existing indexes)
	userRepo.EnsureIndexes()
	formRepo.EnsureIndexes()
	subRepo.EnsureIndexes()
	docRepo.EnsureIndexes()
	docRepo.EnsureBucket()

	// Services
	authSvc := service.NewAuthService(userRepo, cfg.JWTSecret)
	formSvc := service.NewFormService(formRepo, pool)
	subSvc := service.NewSubmissionService(subRepo, formRepo)
	docSvc := service.NewDocumentService(docRepo)
	searchSvc := service.NewSearchService(pool)

	// Seed admin user
	if err := authSvc.SeedAdmin(cfg.AdminEmail, cfg.AdminPass); err != nil {
		log.Printf("Warning: failed to seed admin: %v", err)
	}

	// Handlers
	authH := handler.NewAuthHandler(authSvc)
	formH := handler.NewFormHandler(formSvc)
	subH := handler.NewSubmissionHandler(subSvc, docSvc)
	docH := handler.NewDocumentHandler(docSvc)
	searchH := handler.NewSearchHandler(searchSvc)
	dashH := handler.NewDashboardHandler(formSvc, subSvc, docSvc, formRepo)

	// Router
	r := router.New(cfg.JWTSecret, authH, formH, subH, docH, searchH, dashH)

	log.Printf("OxiDMS server starting on %s", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, r); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
