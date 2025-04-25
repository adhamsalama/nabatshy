package receiver

import (
	"log"
	"net/http"

	"github.com/doug-martin/goqu/v9"
	"github.com/go-chi/chi/v5"
)

func Run() {
	conn := InitClickHouse()
	db := goqu.Dialect("default")
	telService := TelemetryService{
		Ch: &conn,
		DB: &db,
	}
	telController := TelemetryController{
		service: telService,
	}

	r := chi.NewRouter()

	telController.RegisterRoutes(r)
	// Start HTTP server
	addr := ":4318"
	log.Printf("listening on %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, r))
}
