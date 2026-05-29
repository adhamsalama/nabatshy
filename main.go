package main

import (
	"embed"
	"flag"
	"os"

	"nabatshy/api"
	"nabatshy/collector"
	"nabatshy/db"
	"nabatshy/utils"
)

//go:embed ui/dist/*
var content embed.FS

const uiDir = "ui/dist"

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	utils.LoadEnv(".env")

	otelPort := flag.String("otel-port", envOr("OTEL_PORT", "4318"), "OTel collector port")
	apiPort := flag.String("api-port", envOr("API_PORT", "3000"), "API server port")
	uiPort := flag.String("ui-port", envOr("UI_PORT", "8081"), "UI server port")
	dbPath := flag.String("db-path", envOr("DUCKDB_PATH", "nabatshy.db"), "DuckDB data file path")
	inMemory := flag.Bool("in-memory", os.Getenv("DUCKDB_IN_MEMORY") == "true", "Use in-memory DuckDB (no persistence)")
	demoMode := flag.Bool("demo-mode", os.Getenv("DEMO_MODE") == "true", "Disable SQL and Cron features (demo mode)")
	flag.Parse()

	os.Setenv("DUCKDB_PATH", *dbPath)
	sqlDB := db.InitDuckDB(*inMemory)
	go func() { collector.Run(sqlDB, *otelPort, *demoMode) }()
	go utils.ServeUI(content, uiDir, *uiPort)
	api.Run(sqlDB, *apiPort, *demoMode)
}
