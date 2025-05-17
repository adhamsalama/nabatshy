package main

import (
	"os"

	"nabatshy/api"
	"nabatshy/collector"
	"nabatshy/db"
	"nabatshy/utils"
)

func main() {
	if os.Getenv("ENV") != "production" {
		envPath := ".env"
		utils.LoadEnv(envPath)
	}

	databaseAddr := os.Getenv("CLICKHOUSE_ADDR")
	databaseDB := os.Getenv("CLICKHOUSE_DB")
	databaseUsername := os.Getenv("CLICKHOUSE_USERNAME")
	databasePassword := os.Getenv("CLICKHOUSE_PASSWORD")
	conn := db.InitClickHouse(databaseAddr, databaseDB, databaseUsername, databasePassword)
	go func() { collector.Run(conn) }()
	api.Run(conn)
}
