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
);
CREATE TABLE IF NOT EXISTS cron_jobs (
    id          VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL,
    query       VARCHAR NOT NULL,
    interval_seconds INTEGER NOT NULL,
    created_at  BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS log_record (
    timestamp_unix_nano          BIGINT,
    observed_time_unix_nano      BIGINT,
    severity_text                VARCHAR,
    severity_number              INTEGER,
    body                         VARCHAR,
    trace_id                     VARCHAR,
    span_id                      VARCHAR,
    service_name                 VARCHAR,
    attributes_key               VARCHAR[],
    attributes_value             VARCHAR[],
    resource_attributes_key      VARCHAR[],
    resource_attributes_value    VARCHAR[],
    scope_name                   VARCHAR
);
CREATE TABLE IF NOT EXISTS metric_data_point (
    metric_name                   VARCHAR,
    metric_description            VARCHAR,
    metric_unit                   VARCHAR,
    metric_type                   VARCHAR,
    time_unix_nano                BIGINT,
    start_time_unix_nano          BIGINT,
    value_double                  DOUBLE,
    value_int                     BIGINT,
    aggregation_temporality       VARCHAR,
    is_monotonic                  BOOLEAN,
    histogram_count               BIGINT,
    histogram_sum                 DOUBLE,
    histogram_min                 DOUBLE,
    histogram_max                 DOUBLE,
    histogram_bucket_counts       BIGINT[],
    histogram_explicit_bounds     DOUBLE[],
    attributes_key                VARCHAR[],
    attributes_value              VARCHAR[],
    resource_attributes_key       VARCHAR[],
    resource_attributes_value     VARCHAR[],
    scope_name                    VARCHAR
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
	if _, err := db.Exec("INSTALL fts; LOAD fts"); err != nil {
		panic(fmt.Sprintf("loading fts extension: %v", err))
	}
	if _, err := db.Exec("PRAGMA create_fts_index('log_record', 'rowid', 'body', overwrite=1)"); err != nil {
		panic(fmt.Sprintf("building fts index: %v", err))
	}
	return db
}
