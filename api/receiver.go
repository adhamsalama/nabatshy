package api

import (
	"log"
	"net/http"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/doug-martin/goqu/v9"
	"github.com/go-chi/chi/v5"
)

func Run(conn clickhouse.Conn) {
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
	addr := ":3000"
	log.Printf("listening on %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, r))
}
