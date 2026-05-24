package db

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/duckdb/duckdb-go/v2"
)

const createTable = `
CREATE TABLE IF NOT EXISTS denormalized_span (
    trace_id                VARCHAR,
    span_id                 VARCHAR,
    parent_span_id          VARCHAR,
    flags                   INTEGER,
    kind                    VARCHAR,
    name                    VARCHAR,
    start_time_unix_nano    BIGINT,
    end_time_unix_nano      BIGINT,
    duration_ns             BIGINT,
    scope_id                VARCHAR,
    scope_name              VARCHAR,
    resource_id             VARCHAR,
    resource_schema_url     VARCHAR,
    resource_attributes_key   VARCHAR[],
    resource_attributes_value VARCHAR[],
    span_attributes_key       VARCHAR[],
    span_attributes_value     VARCHAR[],
    events_time_unix_nano     BIGINT[],
    events_name               VARCHAR[],
    events_attributes_key     VARCHAR[][],
    events_attributes_value   VARCHAR[][]
);`

func InitDuckDB() *sql.DB {
	path := os.Getenv("DUCKDB_PATH")
	if path == "" {
		path = "nabatshy.db"
	}
	db, err := sql.Open("duckdb", path)
	if err != nil {
		panic(fmt.Sprintf("opening duckdb: %v", err))
	}
	if _, err := db.Exec(createTable); err != nil {
		panic(fmt.Sprintf("creating schema: %v", err))
	}
	return db
}
