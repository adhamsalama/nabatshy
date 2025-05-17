package db

import (
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func InitClickHouse(addr, db, username, password string) clickhouse.Conn {
	var err error
	var ch clickhouse.Conn
	ch, err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: db,
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
	if err != nil {
		errMsg := fmt.Sprintf("connecting to clickhouse err: %v", err)
		panic(errMsg)
	}
	return ch
}

/**
CREATE TABLE resource (
    resource_id UUID DEFAULT generateUUIDv4(),
    schema_url String,
    PRIMARY KEY (resource_id)
) ENGINE = MergeTree
ORDER BY (resource_id);

CREATE TABLE resource_attributes (
    resource_id UUID,
    key String,
    value String,
    PRIMARY KEY (resource_id, key)
) ENGINE = MergeTree
ORDER BY (resource_id, key);


CREATE TABLE scope (
    scope_id UUID DEFAULT generateUUIDv4(),
    name String,
    resource_id UUID,
    PRIMARY KEY (scope_id)
) ENGINE = MergeTree
ORDER BY (scope_id);


CREATE TABLE span (
    trace_id String,
    span_id String,
    parent_span_id String,
    flags Int32,
    name String,
    start_time_unix_nano Int64,
    end_time_unix_nano Int64,
    duration_ns Int64 MATERIALIZED (end_time_unix_nano - start_time_unix_nano),
    scope_id UUID,
    PRIMARY KEY (trace_id, span_id)
) ENGINE = MergeTree
ORDER BY (trace_id, span_id);


CREATE TABLE event (
    span_id String,
    time_unix_nano Int64,
    name String,
    PRIMARY KEY (span_id, time_unix_nano)
) ENGINE = MergeTree
ORDER BY (span_id, time_unix_nano);
*/
