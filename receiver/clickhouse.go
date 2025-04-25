package receiver

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

func InitClickHouse() clickhouse.Conn {
	var err error
	var ch clickhouse.Conn
	ch, err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "admin",
			Password: "password",
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

func InsertResource(
	ch *clickhouse.Conn,
	ctx context.Context, schemaURL string,
) (string, error) {
	resourceID := generateUUID()
	err := (*ch).Exec(ctx, "INSERT INTO resource (resource_id, schema_url) VALUES (?, ?)",
		resourceID, schemaURL)
	return resourceID, err
}

func InsertResourceAttributes(
	ch *clickhouse.Conn,
	ctx context.Context, resourceID string, attrs map[string]string,
) error {
	batch, err := (*ch).PrepareBatch(ctx, "INSERT INTO resource_attributes (resource_id, key, value) VALUES")
	if err != nil {
		return err
	}
	for k, v := range attrs {
		if err := batch.Append(resourceID, k, v); err != nil {
			return err
		}
	}
	return batch.Send()
}

func InsertScope(
	ch *clickhouse.Conn,
	ctx context.Context, name string, resourceID string,
) (string, error) {
	scopeID := generateUUID()
	err := (*ch).Exec(ctx, "INSERT INTO scope (scope_id, name, resource_id) VALUES (?, ?, ?)",
		scopeID, name, resourceID)
	return scopeID, err
}

type Span struct {
	TraceID       string
	SpanID        string
	ParentSpanID  string
	Flags         int32
	Name          string
	StartUnixNano int64
	EndUnixNano   int64
}

func InsertSpans(
	ch *clickhouse.Conn,
	ctx context.Context, scopeID string, spans []Span,
) error {
	batch, err := (*ch).PrepareBatch(ctx, "INSERT INTO span (trace_id, span_id, parent_span_id, flags, name, start_time_unix_nano, end_time_unix_nano, scope_id) VALUES")
	if err != nil {
		return err
	}
	for _, s := range spans {
		if err := batch.Append(s.TraceID, s.SpanID, s.ParentSpanID, s.Flags, s.Name, s.StartUnixNano, s.EndUnixNano, scopeID); err != nil {
			return err
		}
	}
	return batch.Send()
}

type SpanEvent struct {
	SpanID       string
	TimeUnixNano int64
	Name         string
}

func InsertSpanEvents(
	ch *clickhouse.Conn,
	ctx context.Context, events []SpanEvent,
) error {
	batch, err := (*ch).PrepareBatch(ctx, "INSERT INTO event (span_id, time_unix_nano, name) VALUES")
	if err != nil {
		return err
	}
	for _, e := range events {
		if err := batch.Append(e.SpanID, e.TimeUnixNano, e.Name); err != nil {
			return err
		}
	}
	return batch.Send()
}

func generateUUID() string {
	return uuid.New().String()
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
