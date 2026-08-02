package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/parisxmas/OxiDB/OxiDMS/internal/config"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/db"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/gelf"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/handler"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/repository"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/router"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/service"
)

func main() {
	cfg := config.Load()

	// GELF UDP logging
	if cfg.GelfAddr != "" {
		gelfWriter, err := gelf.New(cfg.GelfAddr)
		if err != nil {
			log.Printf("Warning: GELF init failed: %v", err)
		} else {
			log.SetOutput(io.MultiWriter(os.Stderr, gelfWriter))
			log.Printf("GELF logging: enabled (%s)", cfg.GelfAddr)
		}
	}

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

	// Services
	authSvc := service.NewAuthService(userRepo, cfg.JWTSecret)
	formSvc := service.NewFormService(formRepo, pool)
	subSvc := service.NewSubmissionService(subRepo, formRepo)
	docSvc := service.NewDocumentService(docRepo)
	searchSvc := service.NewSearchService(pool, subRepo)

	// Handlers
	authH := handler.NewAuthHandler(authSvc)
	formH := handler.NewFormHandler(formSvc)
	subH := handler.NewSubmissionHandler(subSvc, docSvc)
	docH := handler.NewDocumentHandler(docSvc)
	searchH := handler.NewSearchHandler(searchSvc)
	dashH := handler.NewDashboardHandler(formSvc, subSvc, docSvc, formRepo)
	adminH := handler.NewAdminHandler(subRepo)

	// Router
	r := router.New(cfg.JWTSecret, authH, formH, subH, docH, searchH, dashH, adminH)

	// Start HTTP server immediately, run all index creation and admin seeding
	// in background on a DEDICATED connection so long-running index builds
	// (16M+ docs) don't block the HTTP handler pool.
	go func() {
		log.Printf("Background init: starting")
		initPool, err := db.NewPool(cfg.OxiDBHost, cfg.OxiDBPort, 1)
		if err != nil {
			log.Printf("Warning: init pool connect failed, using main pool: %v", err)
			initPool = pool
		} else {
			log.Printf("Background init: dedicated connection ready")
		}
		defer func() {
			if initPool != pool {
				initPool.Close()
			}
		}()

		initUserRepo := repository.NewUserRepo(initPool)
		initFormRepo := repository.NewFormRepo(initPool)
		initDocRepo := repository.NewDocumentRepo(initPool)
		initSubRepo := repository.NewSubmissionRepo(initPool)
		initAuthSvc := service.NewAuthService(initUserRepo, cfg.JWTSecret)

		// Small collections first (instant)
		log.Printf("Background init: creating user indexes...")
		initUserRepo.EnsureIndexes()
		log.Printf("Background init: creating form indexes...")
		initFormRepo.EnsureIndexes()
		log.Printf("Background init: creating document indexes...")
		initDocRepo.EnsureIndexes()
		log.Printf("Background init: ensuring blob bucket...")
		initDocRepo.EnsureBucket()

		// Seed admin (needs _dms_users index)
		log.Printf("Background init: seeding admin user...")
		if err := initAuthSvc.SeedAdmin(cfg.AdminEmail, cfg.AdminPass); err != nil {
			log.Printf("Warning: failed to seed admin: %v", err)
		}
		log.Printf("Background init: admin seeded, small indexes ready")

		// Large collection indexes (can take minutes)
		log.Printf("Background init: creating submission indexes (may take minutes on large datasets)...")
		start := time.Now()
		if err := initSubRepo.EnsureIndexes(); err != nil {
			log.Printf("Warning: submission index creation failed: %v", err)
		} else {
			log.Printf("Background init: submission indexes ready (%s)", time.Since(start).Round(time.Second))
		}
		log.Printf("Background init: creating text index on submissions (may take minutes)...")
		start = time.Now()
		if err := initSubRepo.EnsureTextIndex([]string{"data"}); err != nil {
			log.Printf("Warning: text index creation failed: %v", err)
		} else {
			log.Printf("Background init: text index ready (%s)", time.Since(start).Round(time.Second))
		}
		log.Printf("Background init: all done")
	}()

	log.Printf("OxiDMS server starting on %s", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, r); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
