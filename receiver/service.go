package receiver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/doug-martin/goqu/v9"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

type TelemetryService struct {
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
}

type TraceList struct {
	TraceID    string  `db:"trace_id"`
	RootSpan   string  `db:"root_span"`
	TotalSpans uint64  `db:"total_spans"`
	Duration   float64 `db:"duration_ms"`
	Timestamp  int64   `db:"timestamp"`
	Issues     uint64  `db:"issues"`
}

type SearchResult struct {
	TraceID       string  `db:"trace_id"`
	SpanID        string  `db:"span_id"`
	Name          string  `db:"name"`
	Service       string  `db:"service_name"`
	Duration      float64 `db:"duration_ms"`
	StartTime     int64   `db:"start_time_unix_nano"`
	EndTime       int64   `db:"end_time_unix_nano"`
	ResourceAttrs map[string]string
}

type SearchResponse struct {
	Results    []SearchResult `json:"results"`
	TotalCount uint64         `json:"totalCount"`
	Page       int            `json:"page"`
	PageSize   int            `json:"pageSize"`
}

type SortOption struct {
	Field string `json:"field"` // "start_time", "end_time", or "duration"
	Order string `json:"order"` // "asc" or "desc"
}

type TimeRangeMetrics struct {
	Timestamp   time.Time `json:"timestamp" db:"timestamp"`
	Count       uint64    `json:"count" db:"count"`
	AvgDuration float64   `json:"avg_duration_ms" db:"avg_duration"`
	TraceID     string    `json:"trace_id" db:"trace_id"`
}

func (m TimeRangeMetrics) MarshalJSON() ([]byte, error) {
	type Alias TimeRangeMetrics
	return json.Marshal(&struct {
		Timestamp string `json:"timestamp"`
		*Alias
	}{
		Timestamp: m.Timestamp.Format(time.RFC3339),
		Alias:     (*Alias)(&m),
	})
}

type ServiceMetrics struct {
	Service     string  `db:"service" json:"service"`
	Count       uint64  `db:"count" json:"count"`
	AvgDuration float64 `db:"avg_duration_ms" json:"avg_duration_ms"`
	ErrorRate   float64 `db:"error_rate" json:"error_rate"`
}

type EndpointMetrics struct {
	Endpoint    string  `db:"endpoint" json:"endpoint"`
	Count       uint64  `db:"count" json:"count"`
	AvgDuration float64 `db:"avg_duration_ms" json:"avg_duration_ms"`
	P95Duration float64 `db:"p95_duration_ms" json:"p95_duration_ms"`
}

type SlowTrace struct {
	TraceID   string  `db:"trace_id" json:"trace_id"`
	Name      string  `db:"name" json:"name"`
	Duration  float64 `db:"duration_ms" json:"duration_ms"`
	Service   string  `db:"service" json:"service"`
	StartTime int64   `db:"start_time" json:"start_time"`
}

func (s *TelemetryService) GetTopSlowTraces(ctx context.Context, n uint) ([]Trace, error) {
	ds := s.DB.
		From("span").
		Select(
			goqu.C("trace_id"),
			goqu.C("name"),
			goqu.L("duration_ns / 1000000").As("duration_ms"),
		).
		Where(goqu.C("parent_span_id").Eq("")).
		Order(goqu.C("start_time_unix_nano").Desc(), goqu.C("duration_ms").Desc()).
		Limit(n)
	sqlStr, args, err := ds.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := (*s.Ch).Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []Trace
	for rows.Next() {
		var t Trace
		if err := rows.Scan(&t.TraceID, &t.Name, &t.Duration); err != nil {
			return nil, err
		}
		results = append(results, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *TelemetryService) ingestTrace(req *coltrace.ExportTraceServiceRequest) error {
	ctx := context.Background()
	for _, rs := range req.ResourceSpans {
		resourceAttrs := extractAttributes(rs.Resource.Attributes)
		resourceID, err := InsertResource(s.Ch, ctx, rs.SchemaUrl)
		if err != nil {
			return err
		}
		if err := InsertResourceAttributes(s.Ch, ctx, resourceID, resourceAttrs); err != nil {
			return err
		}

		for _, ss := range rs.ScopeSpans {
			scopeID, err := InsertScope(s.Ch, ctx, ss.Scope.Name, resourceID)
			if err != nil {
				return err
			}

			var spans []Span
			var events []SpanEvent
			for _, s := range ss.Spans {
				spans = append(spans, Span{
					TraceID:       encodeBytes(s.TraceId),
					SpanID:        encodeBytes(s.SpanId),
					ParentSpanID:  encodeBytes(s.ParentSpanId),
					Flags:         int32(s.Flags),
					Name:          s.Name,
					StartUnixNano: int64(s.StartTimeUnixNano),
					EndUnixNano:   int64(s.EndTimeUnixNano),
				})

				for _, e := range s.Events {
					events = append(events, SpanEvent{
						SpanID:       encodeBytes(s.SpanId),
						TimeUnixNano: int64(e.TimeUnixNano),
						Name:         e.Name,
					})
				}
			}

			if err := InsertSpans(s.Ch, ctx, scopeID, spans); err != nil {
				return err
			}
			if err := InsertSpanEvents(s.Ch, ctx, events); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *TelemetryService) GetServiceTraces(ctx context.Context, service string) ([]ServiceTrace, error) {
	ds := s.DB.
		From("span").
		Join(goqu.T("scope"), goqu.On(goqu.I("span.scope_id").Eq(goqu.I("scope.scope_id")))).
		Select(
			goqu.I("span.trace_id"),
			goqu.I("span.name"),
			goqu.L("span.duration_ns / 1000000").As("duration_ms"),
		).
		Where(goqu.I("scope.name").Eq(service)).
		Order(goqu.I("span.start_time_unix_nano").Desc()).
		Limit(100)

	sqlStr, args, err := ds.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := (*s.Ch).Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var traces []ServiceTrace
	for rows.Next() {
		var t ServiceTrace
		if err := rows.Scan(&t.TraceID, &t.Name, &t.Duration); err != nil {
			return nil, err
		}
		traces = append(traces, t)
	}
	return traces, rows.Err()
}

func (s *TelemetryService) GetTraceDetails(ctx context.Context, traceID string) ([]TraceSpan, error) {
	ds := s.DB.
		From("span").
		Join(goqu.T("scope"), goqu.On(goqu.I("span.scope_id").Eq(goqu.I("scope.scope_id")))).
		Select(
			goqu.I("span.span_id"),
			goqu.I("span.parent_span_id"),
			goqu.I("span.name"),
			goqu.I("scope.name").As("service_name"),
			goqu.I("span.start_time_unix_nano"),
			goqu.I("span.end_time_unix_nano"),
			goqu.L("span.duration_ns / 1000000").As("duration_ms"),
		).
		Where(goqu.I("span.trace_id").Eq(traceID)).
		Order(goqu.I("span.start_time_unix_nano").Asc())

	sqlStr, args, err := ds.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := (*s.Ch).Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var spans []TraceSpan
	for rows.Next() {
		var s TraceSpan
		if err := rows.Scan(&s.SpanID, &s.ParentSpanID, &s.Name, &s.Service, &s.StartTime, &s.EndTime, &s.Duration); err != nil {
			return nil, err
		}
		spans = append(spans, s)
	}
	return spans, rows.Err()
}

func (s *TelemetryService) GetEndpointLatencies(ctx context.Context) ([]EndpointLatency, error) {
	ds := s.DB.
		From("span").
		Join(goqu.T("scope"), goqu.On(goqu.I("span.scope_id").Eq(goqu.I("scope.scope_id")))).
		Select(
			goqu.I("span.name").As("endpoint"),
			goqu.I("scope.name").As("service"),
			goqu.L("avg(span.duration_ns / 1000000)").As("avg_duration_ms"),
			goqu.L("min(span.duration_ns / 1000000)").As("min_duration_ms"),
			goqu.L("max(span.duration_ns / 1000000)").As("max_duration_ms"),
			goqu.L("quantile(0.5)(span.duration_ns / 1000000)").As("p50_duration_ms"),
			goqu.L("quantile(0.9)(span.duration_ns / 1000000)").As("p90_duration_ms"),
			goqu.L("quantile(0.99)(span.duration_ns / 1000000)").As("p99_duration_ms"),
			goqu.L("count(*)").As("request_count"),
		).
		Where(goqu.I("span.parent_span_id").Eq("")).
		GroupBy(goqu.I("span.name"), goqu.I("scope.name")).
		Order(goqu.L("avg_duration_ms").Desc())

	sqlStr, args, err := ds.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := (*s.Ch).Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var latencies []EndpointLatency
	for rows.Next() {
		var l EndpointLatency
		if err := rows.Scan(
			&l.Endpoint,
			&l.Service,
			&l.AvgDuration,
			&l.MinDuration,
			&l.MaxDuration,
			&l.P50Duration,
			&l.P90Duration,
			&l.P99Duration,
			&l.RequestCount,
		); err != nil {
			return nil, err
		}
		latencies = append(latencies, l)
	}
	return latencies, rows.Err()
}

func (s *TelemetryService) GetServiceDependencies(ctx context.Context) ([]ServiceDependency, error) {
	ds := s.DB.
		From(goqu.T("span").As("s1")).
		Join(goqu.T("span").As("s2"), goqu.On(goqu.I("s1.span_id").Eq(goqu.I("s2.parent_span_id")))).
		Join(goqu.T("scope").As("sc1"), goqu.On(goqu.I("s1.scope_id").Eq(goqu.I("sc1.scope_id")))).
		Join(goqu.T("scope").As("sc2"), goqu.On(goqu.I("s2.scope_id").Eq(goqu.I("sc2.scope_id")))).
		Select(
			goqu.I("sc1.name").As("parent_service"),
			goqu.I("sc2.name").As("child_service"),
			goqu.L("count(*)").As("call_count"),
		).
		Where(goqu.I("sc1.name").Neq(goqu.I("sc2.name"))).
		GroupBy(goqu.I("sc1.name"), goqu.I("sc2.name")).
		Order(goqu.L("call_count").Desc())

	sqlStr, args, err := ds.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := (*s.Ch).Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dependencies []ServiceDependency
	for rows.Next() {
		var d ServiceDependency
		if err := rows.Scan(&d.Source, &d.Target, &d.CallCount); err != nil {
			return nil, err
		}
		dependencies = append(dependencies, d)
	}
	return dependencies, rows.Err()
}

func (s *TelemetryService) GetTraceHeatmap(ctx context.Context) ([]TraceHeatmapPoint, error) {
	ds := s.DB.
		From("span").
		Select(
			goqu.L("toStartOfHour(fromUnixTimestamp64Nano(start_time_unix_nano))").As("hour"),
			goqu.L("count(*)").As("trace_count"),
			goqu.L("avg((end_time_unix_nano - start_time_unix_nano) / 1000000)").As("avg_duration_ms"),
		).
		Where(goqu.I("parent_span_id").Eq("")).
		GroupBy(goqu.L("hour")).
		Order(goqu.L("hour").Desc()).
		Limit(24)

	sqlStr, args, err := ds.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := (*s.Ch).Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var heatmap []TraceHeatmapPoint
	for rows.Next() {
		var h TraceHeatmapPoint
		if err := rows.Scan(&h.Hour, &h.TraceCount, &h.AvgDuration); err != nil {
			return nil, err
		}
		heatmap = append(heatmap, h)
	}
	return heatmap, rows.Err()
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

func (s *TelemetryService) GetSpanDetails(ctx context.Context, spanID string) (*SpanDetail, error) {
	ds := s.DB.
		From(goqu.T("span").As("s1")).
		Join(goqu.T("scope"), goqu.On(goqu.I("s1.scope_id").Eq(goqu.I("scope.scope_id")))).
		Join(
			goqu.T("span").As("s2"),
			goqu.On(goqu.I("s1.name").Eq(goqu.I("s2.name"))),
		).
		Select(
			goqu.I("s1.span_id"),
			goqu.I("s1.trace_id"),
			goqu.I("s1.parent_span_id"),
			goqu.I("s1.name"),
			goqu.I("scope.name").As("service_name"),
			goqu.I("s1.start_time_unix_nano"),
			goqu.I("s1.end_time_unix_nano"),
			goqu.L("s1.duration_ns / 1000000").As("duration_ms"),
			goqu.L("avg(s2.duration_ns / 1000000)").As("avg_duration_ms"),
			goqu.L("quantile(0.5)(s2.duration_ns / 1000000)").As("p50_duration_ms"),
			goqu.L("quantile(0.9)(s2.duration_ns / 1000000)").As("p90_duration_ms"),
			goqu.L("quantile(0.99)(s2.duration_ns / 1000000)").As("p99_duration_ms"),
			goqu.L("(s1.duration_ns / 1000000 - avg(s2.duration_ns / 1000000)) / avg(s2.duration_ns / 1000000) * 100").As("duration_diff_percent"),
			goqu.I("s1.scope_id"),
		).
		Where(goqu.I("s1.span_id").Eq(spanID)).
		GroupBy(
			goqu.I("s1.span_id"),
			goqu.I("s1.trace_id"),
			goqu.I("s1.parent_span_id"),
			goqu.I("s1.name"),
			goqu.I("scope.name"),
			goqu.I("s1.start_time_unix_nano"),
			goqu.I("s1.end_time_unix_nano"),
			goqu.I("s1.duration_ns"),
			goqu.I("s1.scope_id"),
		)

	sqlStr, args, err := ds.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := (*s.Ch).Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("span not found: %s", spanID)
	}

	var detail SpanDetail
	var scopeID string
	if err := rows.Scan(
		&detail.SpanID,
		&detail.TraceID,
		&detail.ParentSpanID,
		&detail.Name,
		&detail.Service,
		&detail.StartTime,
		&detail.EndTime,
		&detail.Duration,
		&detail.AvgDuration,
		&detail.P50Duration,
		&detail.P90Duration,
		&detail.P99Duration,
		&detail.DurationDiff,
		&scopeID,
	); err != nil {
		return nil, err
	}

	// Fetch resource_id for this scope
	var resourceID string
	err = (*s.Ch).QueryRow(ctx, "SELECT resource_id FROM scope WHERE scope_id = ?", scopeID).Scan(&resourceID)
	if err != nil {
		return nil, err
	}

	// Fetch resource attributes for this resource_id
	attrRows, err := (*s.Ch).Query(ctx, "SELECT key, value FROM resource_attributes WHERE resource_id = ?", resourceID)
	if err != nil {
		return nil, err
	}
	defer attrRows.Close()
	attrs := make(map[string]string)
	for attrRows.Next() {
		var k, v string
		if err := attrRows.Scan(&k, &v); err != nil {
			return nil, err
		}
		attrs[k] = v
	}
	detail.ResourceAttributes = attrs

	return &detail, nil
}

func (s *TelemetryService) GetTraceList(ctx context.Context) ([]TraceList, error) {
	ds := s.DB.
		From(goqu.T("span").As("s1")).
		Join(goqu.T("scope"), goqu.On(goqu.I("s1.scope_id").Eq(goqu.I("scope.scope_id")))).
		Select(
			goqu.I("s1.trace_id"),
			goqu.I("s1.name").As("root_span"),
			goqu.L("count(*)").As("total_spans"),
			goqu.L("max(s1.duration_ns / 1000000)").As("duration_ms"),
			goqu.L("min(s1.start_time_unix_nano)").As("timestamp"),
			goqu.L("countIf(s1.duration_ns > avg(s1.duration_ns) * 2)").As("issues"),
		).
		Where(goqu.I("s1.parent_span_id").Eq("")).
		GroupBy(goqu.I("s1.trace_id"), goqu.I("s1.name")).
		Order(goqu.L("timestamp").Desc()).
		Limit(100)

	sqlStr, args, err := ds.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := (*s.Ch).Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var traces []TraceList
	for rows.Next() {
		var t TraceList
		if err := rows.Scan(
			&t.TraceID,
			&t.RootSpan,
			&t.TotalSpans,
			&t.Duration,
			&t.Timestamp,
			&t.Issues,
		); err != nil {
			return nil, err
		}
		traces = append(traces, t)
	}
	return traces, rows.Err()
}

func (s *TelemetryService) SearchTraces(ctx context.Context, dateRange DateRange, query string, page, pageSize int, sort SortOption) (*SearchResponse, error) {
	startNano := dateRange.Start.UnixNano()
	endNano := dateRange.End.UnixNano()
	countDS := s.DB.
		From(goqu.T("span").As("s1")).
		Join(goqu.T("scope"), goqu.On(goqu.I("s1.scope_id").Eq(goqu.I("scope.scope_id")))).
		Join(goqu.T("resource_attributes").As("ra"), goqu.On(goqu.I("scope.resource_id").Eq(goqu.I("ra.resource_id")))).
		Select(goqu.L("count(DISTINCT s1.trace_id, s1.span_id)").As("count")).
		Where(
			goqu.Or(
				goqu.I("s1.name").ILike("%"+query+"%"),
				goqu.I("scope.name").ILike("%"+query+"%"),
				goqu.I("s1.trace_id").ILike("%"+query+"%"),
				goqu.I("s1.span_id").ILike("%"+query+"%"),
				goqu.I("ra.key").ILike("%"+query+"%"),
				goqu.I("ra.value").ILike("%"+query+"%"),
			),
			goqu.I("s1.start_time_unix_nano").Gte(startNano),
			goqu.I("s1.end_time_unix_nano").Lte(endNano),
		)

	countSQL, countArgs, err := countDS.ToSQL()
	if err != nil {
		return nil, err
	}

	var totalCount uint64
	err = (*s.Ch).QueryRow(ctx, countSQL, countArgs...).Scan(&totalCount)
	if err != nil {
		return nil, err
	}

	// Then get paginated results
	offset := (page - 1) * pageSize
	ds := s.DB.
		From(goqu.T("span").As("s1")).
		Join(goqu.T("scope"), goqu.On(goqu.I("s1.scope_id").Eq(goqu.I("scope.scope_id")))).
		Join(goqu.T("resource_attributes").As("ra"), goqu.On(goqu.I("scope.resource_id").Eq(goqu.I("ra.resource_id")))).
		Select(
			goqu.I("s1.trace_id"),
			goqu.I("s1.span_id"),
			goqu.I("s1.name"),
			goqu.I("scope.name").As("service_name"),
			goqu.L("s1.duration_ns / 1000000").As("duration_ms"),
			goqu.I("s1.start_time_unix_nano"),
			goqu.I("s1.end_time_unix_nano"),
			goqu.I("s1.scope_id"),
		).
		Where(
			goqu.Or(
				goqu.I("s1.name").ILike("%"+query+"%"),
				goqu.I("scope.name").ILike("%"+query+"%"),
				goqu.I("s1.trace_id").ILike("%"+query+"%"),
				goqu.I("s1.span_id").ILike("%"+query+"%"),
				goqu.I("ra.key").ILike("%"+query+"%"),
				goqu.I("ra.value").ILike("%"+query+"%"),
			),
			goqu.I("s1.start_time_unix_nano").Gte(startNano),
			goqu.I("s1.end_time_unix_nano").Lte(endNano),
		)

	// Apply sorting
	switch sort.Field {
	case "start_time":
		if sort.Order == "asc" {
			ds = ds.Order(goqu.I("s1.start_time_unix_nano").Asc())
		} else {
			ds = ds.Order(goqu.I("s1.start_time_unix_nano").Desc())
		}
	case "end_time":
		if sort.Order == "asc" {
			ds = ds.Order(goqu.I("s1.end_time_unix_nano").Asc())
		} else {
			ds = ds.Order(goqu.I("s1.end_time_unix_nano").Desc())
		}
	case "duration":
		if sort.Order == "asc" {
			ds = ds.Order(goqu.L("s1.duration_ns").Asc())
		} else {
			ds = ds.Order(goqu.L("s1.duration_ns").Desc())
		}
	default:
		ds = ds.Order(goqu.I("s1.start_time_unix_nano").Desc())
	}

	ds = ds.Limit(uint(pageSize)).Offset(uint(offset))
	sqlStr, args, err := ds.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := (*s.Ch).Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SearchResult
	for rows.Next() {
		var r SearchResult
		var scopeID string
		if err := rows.Scan(
			&r.TraceID,
			&r.SpanID,
			&r.Name,
			&r.Service,
			&r.Duration,
			&r.StartTime,
			&r.EndTime,
			&scopeID,
		); err != nil {
			return nil, err
		}

		// Fetch resource_id for this scope
		var resourceID string
		err = (*s.Ch).QueryRow(ctx, "SELECT resource_id FROM scope WHERE scope_id = ?", scopeID).Scan(&resourceID)
		if err != nil {
			return nil, err
		}

		// Fetch resource attributes
		attrRows, err := (*s.Ch).Query(ctx, "SELECT key, value FROM resource_attributes WHERE resource_id = ?", resourceID)
		if err != nil {
			return nil, err
		}
		attrs := make(map[string]string)
		for attrRows.Next() {
			var k, v string
			if err := attrRows.Scan(&k, &v); err != nil {
				attrRows.Close()
				return nil, err
			}
			attrs[k] = v
		}
		attrRows.Close()
		r.ResourceAttrs = attrs

		results = append(results, r)
	}

	return &SearchResponse{
		Results:    results,
		TotalCount: totalCount,
		Page:       page,
		PageSize:   pageSize,
	}, rows.Err()
}

type TimeCount struct {
	Timestamp time.Time `json:"timestamp"`
	Value     uint64    `json:"value"`
}

func (s *TelemetryService) GetTraceCounts(
	ctx context.Context,
	dateRange DateRange,
) ([]TimeCount, error) {
	const numBuckets = 10

	startNano := dateRange.Start.UnixNano()
	endNano := dateRange.End.UnixNano()
	startStr := strconv.FormatInt(startNano, 10)
	endStr := strconv.FormatInt(endNano, 10)

	timeFilter := fmt.Sprintf(
		"start_time_unix_nano >= %s AND start_time_unix_nano <= %s",
		startStr, endStr,
	)

	intervalSQL := getIntervalFromDateRange(dateRange)

	query := fmt.Sprintf(`
		SELECT
			toStartOfInterval(
				fromUnixTimestamp64Nano(start_time_unix_nano),
				INTERVAL %s
			) AS ts,
			count() AS cnt
		FROM span
		WHERE %s
		GROUP BY ts
		ORDER BY ts ASC
		LIMIT %d
	`, intervalSQL, timeFilter, numBuckets)
	fmt.Println(query)
	rows, err := (*s.Ch).Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	raw := make(map[time.Time]uint64, numBuckets)
	for rows.Next() {
		var ts time.Time
		var cnt uint64
		if err := rows.Scan(&ts, &cnt); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		raw[ts] = cnt
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	result := make([]TimeCount, 0, numBuckets)
	for i, j := range raw {
		result = append(result, TimeCount{
			Timestamp: i,
			Value:     j,
		})
	}
	return result, nil
}

func (s *TelemetryService) GetServiceMetrics(ctx context.Context, timeRange string, start, end *time.Time) ([]ServiceMetrics, error) {
	var timeFilter string

	if start != nil && end != nil {
		timeFilter = fmt.Sprintf("start_time_unix_nano >= toUInt64(toDateTime64('%s', 9)) AND start_time_unix_nano <= toUInt64(toDateTime64('%s', 9))",
			start.UTC().Format("2006-01-02T15:04:05.000000000"),
			end.UTC().Format("2006-01-02T15:04:05.000000000"))
	} else {
		switch timeRange {
		case "1h":
			timeFilter = "start_time_unix_nano >= toUInt64(now64()) - 3600000000000"
		case "24h":
			timeFilter = "start_time_unix_nano >= toUInt64(now64()) - 86400000000000"
		case "7d":
			timeFilter = "start_time_unix_nano >= toUInt64(now64()) - 604800000000000"
		case "30d":
			timeFilter = "start_time_unix_nano >= toUInt64(now64()) - 2592000000000000"
		default:
			timeFilter = "start_time_unix_nano >= toUInt64(now64()) - 86400000000000"
		}
	}

	query := `
		WITH durations AS (
			SELECT 
				scope.name AS service,
				(end_time_unix_nano - start_time_unix_nano) / 1000000 AS duration_ms
			FROM span
			INNER JOIN scope ON span.scope_id = scope.scope_id
			WHERE ` + timeFilter + `
		),
		service_stats AS (
			SELECT 
				service,
				avg(duration_ms) AS avg_duration
			FROM durations
			GROUP BY service
		)
		SELECT 
			d.service,
			count(*) AS count,
			avg(d.duration_ms) AS avg_duration_ms,
			countIf(d.duration_ms > s.avg_duration * 2) / count(*) * 100 AS error_rate
		FROM durations d
		JOIN service_stats s ON d.service = s.service
		GROUP BY d.service
		ORDER BY count DESC`

	rows, err := (*s.Ch).Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metrics []ServiceMetrics
	for rows.Next() {
		var m ServiceMetrics
		if err := rows.Scan(&m.Service, &m.Count, &m.AvgDuration, &m.ErrorRate); err != nil {
			return nil, err
		}
		metrics = append(metrics, m)
	}

	return metrics, rows.Err()
}

func (s *TelemetryService) GetEndpointMetrics(ctx context.Context, dateRange DateRange) ([]EndpointMetrics, error) {
	start := strconv.FormatInt(dateRange.Start.UnixNano(), 10)
	end := strconv.FormatInt(dateRange.End.UnixNano(), 10)
	timeFilter := fmt.Sprintf(
		"start_time_unix_nano >= %s  AND start_time_unix_nano <= %s",
		start, end,
	)

	query := `
		WITH durations AS (
			SELECT 
				name AS endpoint,
				(end_time_unix_nano - start_time_unix_nano) / 1000000 AS duration_ms
			FROM span
			WHERE ` + timeFilter + `
			ORDER BY end_time_unix_nano ASC
		)
		SELECT 
			endpoint,
			count(*) AS count,
			avg(duration_ms) AS avg_duration_ms,
			quantile(0.95)(duration_ms) AS p95_duration_ms
		FROM durations
		GROUP BY endpoint
		--ORDER BY duration_ms DESC
		LIMIT 10`
	rows, err := (*s.Ch).Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metrics []EndpointMetrics
	for rows.Next() {
		var m EndpointMetrics
		if err := rows.Scan(&m.Endpoint, &m.Count, &m.AvgDuration, &m.P95Duration); err != nil {
			return nil, err
		}
		metrics = append(metrics, m)
	}

	return metrics, rows.Err()
}

func (s *TelemetryService) GetSlowestTraces(ctx context.Context, timeRange string) ([]SlowTrace, error) {
	var timeFilter string
	switch timeRange {
	case "1h":
		timeFilter = "start_time_unix_nano >= toUInt64(now64()) - 3600000000000"
	case "24h":
		timeFilter = "start_time_unix_nano >= toUInt64(now64()) - 86400000000000"
	case "7d":
		timeFilter = "start_time_unix_nano >= toUInt64(now64()) - 604800000000000"
	case "30d":
		timeFilter = "start_time_unix_nano >= toUInt64(now64()) - 2592000000000000"
	default:
		timeFilter = "start_time_unix_nano >= toUInt64(now64()) - 86400000000000"
	}

	ds := s.DB.
		From("span").
		Join(goqu.T("scope"), goqu.On(goqu.I("span.scope_id").Eq(goqu.I("scope.scope_id")))).
		Select(
			goqu.I("span.trace_id"),
			goqu.I("span.name"),
			goqu.L("(end_time_unix_nano - start_time_unix_nano) / 1000000").As("duration_ms"),
			goqu.I("scope.name").As("service"),
			goqu.I("span.start_time_unix_nano").As("start_time"),
		).
		Where(goqu.And(
			goqu.I("span.parent_span_id").Eq(""),
			goqu.L(timeFilter),
		)).
		Order(goqu.L("duration_ms").Desc()).
		Limit(10)

	sqlStr, args, err := ds.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := (*s.Ch).Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var traces []SlowTrace
	for rows.Next() {
		var t SlowTrace
		var rawTraceID []byte
		if err := rows.Scan(&rawTraceID, &t.Name, &t.Duration, &t.Service, &t.StartTime); err != nil {
			return nil, err
		}
		t.TraceID = encodeBytes(rawTraceID)
		traces = append(traces, t)
	}

	return traces, rows.Err()
}

type TimePercentile struct {
	Timestamp time.Time `json:"timestamp"` // start of the bucket
	Value     float64   `json:"value"`     // the p-percentile (ms) in this bucket
}

func (s *TelemetryService) GetPercentileSeries(
	ctx context.Context,
	dateRange DateRange,
	percentile int,
	buckets int,
) ([]TimePercentile, error) {
	if percentile < 0 {
		percentile = 0
	}
	if percentile > 100 {
		percentile = 100
	}
	q := float64(percentile) / 100.0

	startNs := dateRange.Start.UnixNano()
	endNs := dateRange.End.UnixNano()
	totalNs := endNs - startNs
	if totalNs <= 0 || buckets <= 0 {
		return nil, fmt.Errorf("invalid range or buckets")
	}

	intervalSQL := getIntervalFromDateRange(dateRange)

	query := fmt.Sprintf(`
        SELECT
 			   	toStartOfInterval(toDateTime(start_time_unix_nano / 1e9), interval %s) time_bucket,
  				quantile(%f)((end_time_unix_nano - start_time_unix_nano)/ 1000000) AS pvalue
				FROM span
				WHERE start_time_unix_nano >= %d  and end_time_unix_nano <= %d
				GROUP BY time_bucket
				ORDER BY time_bucket
  	`, intervalSQL, q, startNs, endNs)

	fmt.Printf("%v\n", query)
	rows, err := (*s.Ch).Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	series := make([]TimePercentile, 0)
	for rows.Next() {
		thisValue := TimePercentile{}
		if err := rows.Scan(&thisValue.Timestamp, &thisValue.Value); err != nil {
			return nil, err
		}
		series = append(series, thisValue)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return series, nil
}

func (s *TelemetryService) GetAvgDuration(
	ctx context.Context,
	dateRange DateRange,
) ([]TimePercentile, error) {
	startNs := dateRange.Start.UnixNano()
	endNs := dateRange.End.UnixNano()

	intervalSQL := getIntervalFromDateRange(dateRange)

	query := fmt.Sprintf(`
        SELECT
 			   	toStartOfInterval(toDateTime(start_time_unix_nano / 1e9), interval %s) time_bucket,
  				avg((end_time_unix_nano - start_time_unix_nano)/ 1000000) AS pvalue
				FROM span
				WHERE start_time_unix_nano >= %d  and end_time_unix_nano <= %d
				GROUP BY time_bucket
				ORDER BY time_bucket
  	`, intervalSQL, startNs, endNs)

	fmt.Printf("%v\n", query)
	rows, err := (*s.Ch).Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	series := make([]TimePercentile, 0)
	for rows.Next() {
		thisValue := TimePercentile{}
		if err := rows.Scan(&thisValue.Timestamp, &thisValue.Value); err != nil {
			return nil, err
		}
		series = append(series, thisValue)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return series, nil
}

func getIntervalFromDateRange(dateRange DateRange) string {
	totalDur := dateRange.End.Sub(dateRange.Start)
	var intervalSQL string
	day := time.Hour * 24
	month := day * 30
	switch {
	case totalDur < time.Minute:
		intervalSQL = "1 second"
	case totalDur >= time.Minute && totalDur <= time.Hour*4:
		intervalSQL = "1 minute"
	case totalDur >= time.Hour && totalDur <= day:
		intervalSQL = "1 hour"
	case totalDur >= day && totalDur <= month:
		intervalSQL = "1 day"
	default:
		intervalSQL = "1 day"
	}
	return intervalSQL
}
