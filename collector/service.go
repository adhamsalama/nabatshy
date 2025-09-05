package collector

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"nabatshy/utils"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/doug-martin/goqu/v9"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

var InsertDenormalizedSpans = utils.InsertDenormalizedSpans

type TelemetryCollectorService struct {
	Ch *clickhouse.Conn
	DB *goqu.DialectWrapper
}

type Trace struct {
	TraceID  string  `db:"trace_id"`
	Name     string  `db:"name"`
	Duration float64 `db:"duration_ms"`
}

type ServiceTrace struct {
	TraceID  string  `db:"trace_id"`
	Name     string  `db:"name"`
	Duration float64 `db:"duration_ms"`
}

type TraceSpan struct {
	SpanID       string  `db:"span_id"`
	ParentSpanID string  `db:"parent_span_id"`
	Name         string  `db:"name"`
	Service      string  `db:"service_name"`
	StartTime    int64   `db:"start_time_unix_nano"`
	EndTime      int64   `db:"end_time_unix_nano"`
	Duration     float64 `db:"duration_ms"`
}

type EndpointLatency struct {
	Endpoint     string  `db:"endpoint"`
	Service      string  `db:"service"`
	AvgDuration  float64 `db:"avg_duration_ms"`
	MinDuration  float64 `db:"min_duration_ms"`
	MaxDuration  float64 `db:"max_duration_ms"`
	P50Duration  float64 `db:"p50_duration_ms"`
	P90Duration  float64 `db:"p90_duration_ms"`
	P99Duration  float64 `db:"p99_duration_ms"`
	RequestCount uint64  `db:"request_count"`
}

type ServiceDependency struct {
	Source    string `db:"parent_service"`
	Target    string `db:"child_service"`
	CallCount uint64 `db:"call_count"`
}

type TraceHeatmapPoint struct {
	Hour        time.Time `db:"hour"`
	TraceCount  uint64    `db:"trace_count"`
	AvgDuration float64   `db:"avg_duration_ms"`
}

type SpanDetail struct {
	SpanID             string            `db:"span_id"`
	TraceID            string            `db:"trace_id"`
	ParentSpanID       string            `db:"parent_span_id"`
	Name               string            `db:"name"`
	Service            string            `db:"service_name"`
	StartTime          int64             `db:"start_time_unix_nano"`
	EndTime            int64             `db:"end_time_unix_nano"`
	Duration           float64           `db:"duration_ms"`
	AvgDuration        float64           `db:"avg_duration_ms"`
	P50Duration        float64           `db:"p50_duration_ms"`
	P90Duration        float64           `db:"p90_duration_ms"`
	P99Duration        float64           `db:"p99_duration_ms"`
	DurationDiff       float64           `db:"duration_diff_percent"`
	ResourceAttributes map[string]string `json:"resourceAttributes"`
	SpanAttributes     map[string]string `json:"spanAttributes"`
}

type TraceList struct {
	TraceID    string  `db:"trace_id"`
	RootSpan   string  `db:"root_span"`
	TotalSpans uint64  `db:"total_spans"`
	Duration   float64 `db:"duration_ms"`
	Timestamp  int64   `db:"timestamp"`
	Issues     uint64  `db:"issues"`
}

func (s *TelemetryCollectorService) ingestTrace(req *coltrace.ExportTraceServiceRequest) error {
	ctx := context.Background()
	for _, rs := range req.ResourceSpans {
		resourceAttrs := extractAttributes(rs.Resource.Attributes)
		resourceSchemaURL := rs.SchemaUrl

		for _, ss := range rs.ScopeSpans {
			scopeName := ss.Scope.Name

			var spans []utils.Span
			for _, span := range ss.Spans {
				// Collect events for the span
				var events []utils.Event
				for _, e := range span.Events {
					events = append(events,
						utils.Event{
							TimeUnixNano: int64(e.TimeUnixNano),
							Name:         e.Name,
						},
					)
				}

				// Collect resource attributes as a nested structure
				var resourceAttributes []utils.ResourceAttribute
				for k, v := range resourceAttrs {
					resourceAttributes = append(resourceAttributes,
						utils.ResourceAttribute{
							Key:   k,
							Value: v,
						},
					)
				}

				// Extract span attributes (this is where db.statement will be)
				spanAttrs := extractAttributes(span.Attributes)
				var spanAttributes []utils.ResourceAttribute
				for k, v := range spanAttrs {
					spanAttributes = append(spanAttributes,
						utils.ResourceAttribute{
							Key:   k,
							Value: v,
						},
					)
				}

				// Append the denormalized span
				spans = append(spans, utils.Span{
					TraceID:            encodeBytes(span.TraceId),
					SpanID:             encodeBytes(span.SpanId),
					ParentSpanID:       encodeBytes(span.ParentSpanId),
					Flags:              int32(span.Flags),
					Name:               span.Name,
					StartTimeUnixNano:  int64(span.StartTimeUnixNano),
					EndTimeUnixNano:    int64(span.EndTimeUnixNano),
					ScopeName:          scopeName,
					ResourceSchemaURL:  resourceSchemaURL,
					ResourceAttributes: resourceAttributes,
					SpanAttributes:     spanAttributes,
					Events:             events,
				})
			}

			// Insert denormalized spans into the database
			if err := InsertDenormalizedSpans(s.Ch, ctx, spans); err != nil {
				return err
			}
		}
	}
	return nil
}

func extractAttributes(attrs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		if val := kv.GetValue(); val != nil {
			switch v := val.Value.(type) {
			case *commonpb.AnyValue_StringValue:
				m[kv.Key] = v.StringValue
			case *commonpb.AnyValue_IntValue:
				m[kv.Key] = strconv.FormatInt(v.IntValue, 10)
			case *commonpb.AnyValue_DoubleValue:
				m[kv.Key] = strconv.FormatFloat(v.DoubleValue, 'f', -1, 64)
			case *commonpb.AnyValue_BoolValue:
				m[kv.Key] = strconv.FormatBool(v.BoolValue)
			default:
				fmt.Printf("Unhandled value type for key %s: %T\n", kv.Key, v)
			}
		}
	}
	return m
}

func encodeBytes(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}
