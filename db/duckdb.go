package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"os"
	"path/filepath"

	duckdb "github.com/duckdb/duckdb-go/v2"
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
    resource_attributes       MAP(VARCHAR, VARIANT),
    span_attributes           MAP(VARCHAR, VARIANT),
    events_time_unix_nano     BIGINT[],
    events_name               VARCHAR[],
    events_attributes         MAP(VARCHAR, VARIANT)[]
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
    attributes                   MAP(VARCHAR, VARIANT),
    resource_attributes          MAP(VARCHAR, VARIANT),
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
    attributes                    MAP(VARCHAR, VARIANT),
    resource_attributes           MAP(VARCHAR, VARIANT),
    scope_name                    VARCHAR
);`

func InitDuckDB(inMemory bool) *sql.DB {
	var db *sql.DB

	if inMemory {
		log.Println("[duckdb] mode: in-memory (no persistence)")
		var err error
		db, err = sql.Open("duckdb", "")
		if err != nil {
			panic(fmt.Sprintf("opening duckdb: %v", err))
		}
	} else {
		path := os.Getenv("DUCKDB_PATH")
		if path == "" {
			path = "nabatshy.db"
		}
		log.Printf("[duckdb] mode: persistent (%s)\n", path)

		absPath, err := filepath.Abs(path)
		if err != nil {
			panic(fmt.Sprintf("resolving db path: %v", err))
		}

		// Open an in-memory session and ATTACH the file with STORAGE_VERSION
		// 'v1.5.0'. For new databases this creates them at the required storage
		// version so VARIANT columns are supported. For existing databases the
		// STORAGE_VERSION option is silently ignored by DuckDB and the database
		// opens at its current storage version. The init func runs on every new
		// connection in the pool, ensuring all connections use the right database.
		connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
			_, err := execer.ExecContext(context.Background(),
				fmt.Sprintf("ATTACH IF NOT EXISTS '%s' AS nabatshy (STORAGE_VERSION 'v1.5.0'); USE nabatshy", absPath),
				nil,
			)
			return err
		})
		if err != nil {
			panic(fmt.Sprintf("opening duckdb connector: %v", err))
		}
		db = sql.OpenDB(connector)
	}

	db.SetMaxOpenConns(1)
	if _, err := db.Exec(createTable); err != nil {
		panic(fmt.Sprintf("creating schema: %v", err))
	}
	if _, err := db.Exec("INSTALL fts; LOAD fts"); err != nil {
		panic(fmt.Sprintf("loading fts extension: %v", err))
	}
	// Create FTS index only if it doesn't already exist. The LogWriter rebuilds
	// it with overwrite=1 after each batch, so we just need it present on startup.
	db.Exec("PRAGMA create_fts_index('log_record', 'rowid', 'body')")
	return db
}
