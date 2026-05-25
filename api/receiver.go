package api

import (
	"database/sql"
	"log"
	"net/http"

	"github.com/doug-martin/goqu/v9"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func Run(db *sql.DB, port string) {
	dialect := goqu.Dialect("default")
	telService := TelemetryService{
		Ch: db,
		DB: &dialect,
	}
	telController := TelemetryController{
		service: telService,
	}

	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	r.Use(middleware.Logger)
	telController.RegisterRoutes(r)
	cronController := NewCronController(db)
	cronController.RegisterRoutes(r)
	addr := ":" + port
	log.Printf("[api] listening on %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, r))
}
