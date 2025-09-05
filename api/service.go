package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"nabatshy/utils"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/doug-martin/goqu/v9"
)

var (
	PadQueryResult  = utils.PadQueryResult
	ParseInterval   = utils.ParseInterval
	AlignToInterval = utils.AlignToInterval
)

var GetIntervalFromDateRange = utils.GetIntervalFromDateRange

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
	Results            []SearchResult   `json:"results"`
	TotalCount         uint64           `json:"totalCount"`
	Page               int              `json:"page"`
	PageSize           int              `json:"pageSize"`
	PercentileResults  []TimePercentile `json:"percentile"`
	TraceCountResults  []TimePercentile `json:"traceCount"`
	AvgDurationResults []TimePercentile `json:"avgDuration"`
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
		From("denormalized_span").
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

func (s *TelemetryService) GetServiceTraces(ctx context.Context, service string) ([]ServiceTrace, error) {
	ds := s.DB.
		From("denormalized_span").
		Select(
			goqu.C("trace_id"),
			goqu.C("name"),
			goqu.L("duration_ns / 1000000").As("duration_ms"),
		).
		Where(goqu.C("scope_name").Eq(service)).
		Order(goqu.C("start_time_unix_nano").Desc()).
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
		From("denormalized_span").
		Select(
			goqu.C("span_id"),
			goqu.C("parent_span_id"),
			goqu.C("name"),
			goqu.C("scope_name").As("service_name"),
			goqu.C("start_time_unix_nano"),
			goqu.C("end_time_unix_nano"),
			goqu.L("duration_ns / 1000000").As("duration_ms"),
		).
		Where(goqu.C("trace_id").Eq(traceID)).
		Order(goqu.C("start_time_unix_nano").Asc())

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
		From("denormalized_span").
		Select(
			goqu.C("name").As("endpoint"),
			goqu.C("scope_name").As("service"),
			goqu.L("avg(duration_ns / 1000000)").As("avg_duration_ms"),
			goqu.L("min(duration_ns / 1000000)").As("min_duration_ms"),
			goqu.L("max(duration_ns / 1000000)").As("max_duration_ms"),
			goqu.L("quantile(0.5)(duration_ns / 1000000)").As("p50_duration_ms"),
			goqu.L("quantile(0.9)(duration_ns / 1000000)").As("p90_duration_ms"),
			goqu.L("quantile(0.99)(duration_ns / 1000000)").As("p99_duration_ms"),
			goqu.L("count(*)").As("request_count"),
		).
		Where(goqu.C("parent_span_id").Eq("")).
		GroupBy(goqu.C("name"), goqu.C("scope_name")).
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
		From("denormalized_span").As("s1").
		Join(goqu.T("denormalized_span").As("s2"), goqu.On(goqu.I("s1.span_id").Eq(goqu.I("s2.parent_span_id")))).
		Select(
			goqu.I("s1.scope_name").As("parent_service"),
			goqu.I("s2.scope_name").As("child_service"),
			goqu.L("count(*)").As("call_count"),
		).
		Where(goqu.I("s1.scope_name").Neq(goqu.I("s2.scope_name"))).
		GroupBy(goqu.I("s1.scope_name"), goqu.I("s2.scope_name")).
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
		From("denormalized_span").
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

func encodeBytes(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func (s *TelemetryService) GetSpanDetails(ctx context.Context, spanID string) (*SpanDetail, error) {
	ds := s.DB.
		From(goqu.T("denormalized_span")).
		Select(
			goqu.I("span_id"),
			goqu.I("trace_id"),
			goqu.I("parent_span_id"),
			goqu.I("name"),
			goqu.I("scope_name").As("service_name"),
			goqu.I("start_time_unix_nano"),
			goqu.I("end_time_unix_nano"),
			goqu.L("duration_ns / 1000000").As("duration_ms"),
			goqu.I("resource_attributes.key").As("resource_keys"),
			goqu.I("resource_attributes.value").As("resource_values"),
			goqu.I("span_attributes.key").As("span_keys"),
			goqu.I("span_attributes.value").As("span_values"),
		).
		Where(goqu.I("span_id").Eq(spanID)).
		GroupBy(
			goqu.I("span_id"),
			goqu.I("trace_id"),
			goqu.I("parent_span_id"),
			goqu.I("name"),
			goqu.I("scope_name"),
			goqu.I("start_time_unix_nano"),
			goqu.I("end_time_unix_nano"),
			goqu.I("duration_ns"),
			goqu.I("resource_attributes.key"),
			goqu.I("resource_attributes.value"),
			goqu.I("span_attributes.key"),
			goqu.I("span_attributes.value"),
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
	var resourceKeys, resourceValues, spanKeys, spanValues []string
	if err := rows.Scan(
		&detail.SpanID,
		&detail.TraceID,
		&detail.ParentSpanID,
		&detail.Name,
		&detail.Service,
		&detail.StartTime,
		&detail.EndTime,
		&detail.Duration,
		&resourceKeys,
		&resourceValues,
		&spanKeys,
		&spanValues,
	); err != nil {
		return nil, err
	}

	// Map resource attributes
	resourceAttrs := make(map[string]string)
	for i := range resourceKeys {
		resourceAttrs[resourceKeys[i]] = resourceValues[i]
	}
	detail.ResourceAttributes = resourceAttrs

	// Map span attributes (this will include db.statement)
	spanAttrs := make(map[string]string)
	for i := range spanKeys {
		spanAttrs[spanKeys[i]] = spanValues[i]
	}
	detail.SpanAttributes = spanAttrs

	// calculate avg durations of spans of the same name
	avgDS := s.DB.
		From(goqu.T("denormalized_span")).
		Select(
			goqu.L("avg(duration_ns / 1000000)").As("avg_duration_ms"),
			goqu.L("quantile(0.5)(duration_ns / 1000000)").As("p50_duration_ms"),
			goqu.L("quantile(0.9)(duration_ns / 1000000)").As("p90_duration_ms"),
			goqu.L("quantile(0.99)(duration_ns / 1000000)").As("p99_duration_ms"),
		).
		Where(goqu.I("name").Eq(detail.Name)).
		GroupBy(goqu.I("name"))
	sqlAvgStr, avgArgs, err := avgDS.ToSQL()
	if err != nil {
		return nil, err
	}
	var avgResult struct {
		AvgDuration float64 `db:"avg_duration_ms"`
		P50Duration float64 `db:"p50_duration_ms"`
		P90Duration float64 `db:"p90_duration_ms"`
		P99Duration float64 `db:"p99_duration_ms"`
	}
	if err := (*s.Ch).QueryRow(ctx, sqlAvgStr, avgArgs...).Scan(
		&avgResult.AvgDuration,
		&avgResult.P50Duration,
		&avgResult.P90Duration,
		&avgResult.P99Duration,
	); err != nil {
		return nil, fmt.Errorf("failed to get avg durations: %w", err)
	}
	detail.AvgDuration = avgResult.AvgDuration
	detail.P50Duration = avgResult.P50Duration
	detail.P90Duration = avgResult.P90Duration
	detail.P99Duration = avgResult.P99Duration
	detail.DurationDiff = (detail.Duration - avgResult.AvgDuration) / avgResult.AvgDuration * 100

	return &detail, nil
}

func (s *TelemetryService) GetTraceList(ctx context.Context) ([]TraceList, error) {
	ds := s.DB.
		From(goqu.T("denormalized_span").As("s1")).
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

// AttributeQuery represents a parsed key=value or key!=value pair
type AttributeQuery struct {
	Key      string
	Value    string
	Operator string // "=" or "!="
}

// parseAttributeQuery parses query string like "attribute1=value1,attribute2!=value2"
// Returns nil if query doesn't match this format (falls back to original search)
func parseAttributeQuery(query string) []AttributeQuery {
	if query == "" {
		return nil
	}
	
	// Check if query contains = or != operators
	if !strings.Contains(query, "=") {
		return nil
	}
	
	pairs := strings.Split(query, ",")
	var attrs []AttributeQuery
	
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		
		// Check for != operator first (longer match)
		if strings.Contains(pair, "!=") {
			parts := strings.SplitN(pair, "!=", 2)
			if len(parts) == 2 {
				attrs = append(attrs, AttributeQuery{
					Key:      strings.TrimSpace(parts[0]),
					Value:    strings.TrimSpace(parts[1]),
					Operator: "!=",
				})
			}
		} else if strings.Contains(pair, "=") {
			// Check for = operator
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) == 2 {
				attrs = append(attrs, AttributeQuery{
					Key:      strings.TrimSpace(parts[0]),
					Value:    strings.TrimSpace(parts[1]),
					Operator: "=",
				})
			}
		}
	}
	
	// Only return parsed attributes if all pairs were valid
	if len(attrs) == len(pairs) {
		return attrs
	}
	
	return nil
}

func (s *TelemetryService) SearchTraces(ctx context.Context, dateRange DateRange, query string, page, pageSize int, sort SortOption, percentile int) (*SearchResponse, error) {
	startNano := dateRange.Start.UnixNano()
	endNano := dateRange.End.UnixNano()

	base := s.DB.From(goqu.T("denormalized_span"))

	conds := []goqu.Expression{
		goqu.I("start_time_unix_nano").Gte(startNano),
		goqu.I("end_time_unix_nano").Lte(endNano),
	}

	if query != "" {
		// Try to parse as attribute query first
		if attrs := parseAttributeQuery(query); attrs != nil {
			// Build AND conditions for each key=value or key!=value pair
			var attrConds []goqu.Expression
			for _, attr := range attrs {
				if attr.Operator == "=" {
					// Equals: match spans that have this exact key=value pair
					attrConds = append(attrConds, goqu.Or(
						goqu.And(
							goqu.L("has(resource_attributes.key, ?)", attr.Key),
							goqu.L("has(resource_attributes.value, ?)", attr.Value),
						),
						goqu.And(
							goqu.L("has(span_attributes.key, ?)", attr.Key),
							goqu.L("has(span_attributes.value, ?)", attr.Value),
						),
					))
				} else if attr.Operator == "!=" {
					// Not equals: match spans that either don't have the key or have a different value
					attrConds = append(attrConds, goqu.Or(
						// Resource attributes: key doesn't exist OR (key exists AND value is different)
						goqu.Or(
							goqu.L("NOT has(resource_attributes.key, ?)", attr.Key),
							goqu.And(
								goqu.L("has(resource_attributes.key, ?)", attr.Key),
								goqu.L("NOT has(resource_attributes.value, ?)", attr.Value),
							),
						),
						// Span attributes: key doesn't exist OR (key exists AND value is different) 
						goqu.Or(
							goqu.L("NOT has(span_attributes.key, ?)", attr.Key),
							goqu.And(
								goqu.L("has(span_attributes.key, ?)", attr.Key),
								goqu.L("NOT has(span_attributes.value, ?)", attr.Value),
							),
						),
					))
				}
			}
			// All attribute conditions must match (AND)
			conds = append(conds, goqu.And(attrConds...))
		} else {
			// Fallback to original broad search
			conds = append(conds, goqu.Or(
				goqu.I("name").Eq(query),
				goqu.I("scope_name").Eq(query),
				goqu.I("trace_id").Eq(query),
				goqu.I("span_id").Eq(query),
				goqu.L("has(resource_attributes.key, ?)", query),
				goqu.L("has(resource_attributes.value, ?)", query),
				goqu.L("has(span_attributes.key, ?)", query),
				goqu.L("has(span_attributes.value, ?)", query),
			))
		}
	}

	countDS := base.
		Select(goqu.L("count(DISTINCT trace_id, span_id)").As("count")).
		Where(conds...)

	countSQL, countArgs, err := countDS.ToSQL()
	if err != nil {
		return nil, err
	}

	var totalCount uint64
	err = (*s.Ch).QueryRow(ctx, countSQL, countArgs...).Scan(&totalCount)
	if err != nil {
		return nil, err
	}

	offset := (page - 1) * pageSize

	ds := base.
		Select(
			goqu.I("trace_id"),
			goqu.I("span_id"),
			goqu.I("name"),
			goqu.I("scope_name").As("service_name"),
			goqu.L("duration_ns / 1000000").As("duration_ms"),
			goqu.I("start_time_unix_nano"),
			goqu.I("end_time_unix_nano"),
			goqu.I("resource_attributes.key").As("resource_keys"),
			goqu.I("resource_attributes.value").As("resource_values"),
		).
		Where(conds...)

	queryString, _, _ := ds.ToSQL()
	intervalSQL := GetIntervalFromDateRange(dateRange)

	pResult, pErr := s.getPercentileForQuery(ctx, queryString, intervalSQL, dateRange, percentile)
	if pErr != nil {
		return nil, pErr
	}
	tcResult, tcErr := s.getTraceCountForQuery(ctx, queryString, intervalSQL, dateRange)
	if tcErr != nil {
		return nil, tcErr
	}
	avgResult, avgErr := s.getAverageDurationForQuery(ctx, queryString, intervalSQL, dateRange)
	if avgErr != nil {
		return nil, avgErr
	}
	switch sort.Field {
	case "start_time":
		if sort.Order == "asc" {
			ds = ds.Order(goqu.I("start_time_unix_nano").Asc())
		} else {
			ds = ds.Order(goqu.I("start_time_unix_nano").Desc())
		}
	case "end_time":
		if sort.Order == "asc" {
			ds = ds.Order(goqu.I("end_time_unix_nano").Asc())
		} else {
			ds = ds.Order(goqu.I("end_time_unix_nano").Desc())
		}
	case "duration":
		if sort.Order == "asc" {
			ds = ds.Order(goqu.I("duration_ns").Asc())
		} else {
			ds = ds.Order(goqu.I("duration_ns").Desc())
		}
	default:
		ds = ds.Order(goqu.I("start_time_unix_nano").Desc())
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
		var resourceKeys, resourceValues []string
		if err := rows.Scan(
			&r.TraceID,
			&r.SpanID,
			&r.Name,
			&r.Service,
			&r.Duration,
			&r.StartTime,
			&r.EndTime,
			&resourceKeys,
			&resourceValues,
		); err != nil {
			return nil, err
		}
		attrs := make(map[string]string)
		for i := range resourceKeys {
			attrs[resourceKeys[i]] = resourceValues[i]
		}
		r.ResourceAttrs = attrs
		results = append(results, r)
	}

	return &SearchResponse{
		Results:            results,
		TotalCount:         totalCount,
		Page:               page,
		PageSize:           pageSize,
		PercentileResults:  pResult,
		TraceCountResults:  tcResult,
		AvgDurationResults: avgResult,
	}, rows.Err()
}

func (s *TelemetryService) getPercentileForQuery(ctx context.Context, queryString string, intervalSQL string, dateRange DateRange, percentile int) ([]TimePercentile, error) {
	pFloat := float64(percentile) / 100.0

	pSeriesQuery := fmt.Sprintf(`
		WITH stats as (
			%s
		)
      SELECT
            toStartOfInterval(
                toDateTime(stats.start_time_unix_nano / 1e9),
                INTERVAL %s
            ) AS ts,
            quantile(%f)(
                (stats.end_time_unix_nano - stats.start_time_unix_nano) / 1000000
            ) AS pvalue
        FROM stats
        GROUP BY ts
        ORDER BY ts		`, queryString, intervalSQL, pFloat)

	pRows, err := (*s.Ch).Query(ctx, pSeriesQuery)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer pRows.Close()
	pResult, pErr := PadQueryResult(pRows, intervalSQL, dateRange)
	if pErr != nil {
		panic(pErr)
	}
	return pResult, nil
}

type TimeCount struct {
	Timestamp time.Time `json:"timestamp"`
	Value     uint64    `json:"value"`
}

func (s *TelemetryService) GetTraceCounts(
	ctx context.Context,
	dateRange DateRange,
) ([]TimeCount, error) {
	startNano := dateRange.Start.UnixNano()
	endNano := dateRange.End.UnixNano()
	timeFilter := fmt.Sprintf(
		"start_time_unix_nano >= %d AND start_time_unix_nano <= %d",
		startNano, endNano,
	)
	intervalSQL := GetIntervalFromDateRange(dateRange)

	query := fmt.Sprintf(`
        SELECT
            toStartOfInterval(
                fromUnixTimestamp64Nano(start_time_unix_nano),
                INTERVAL %s
            ) AS ts,
            count() AS cnt
        FROM denormalized_span
        WHERE %s
        GROUP BY ts
        ORDER BY ts ASC
    `, intervalSQL, timeFilter)

	rows, err := (*s.Ch).Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	counts := make(map[time.Time]uint64)
	for rows.Next() {
		var ts time.Time
		var cnt uint64
		if err := rows.Scan(&ts, &cnt); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		counts[ts] = cnt
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	intervalDur, err := ParseInterval(intervalSQL)
	if err != nil {
		return nil, fmt.Errorf("invalid interval: %w", err)
	}

	alignedStart := AlignToInterval(dateRange.Start, intervalDur)

	var result []TimeCount
	for ts := alignedStart; !ts.After(dateRange.End); ts = ts.Add(intervalDur) {
		result = append(result, TimeCount{
			Timestamp: ts,
			Value:     counts[ts],
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
				scope_name AS service,
				(end_time_unix_nano - start_time_unix_nano) / 1000000 AS duration_ms
			FROM denormalized_span
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
			FROM denormalized_span
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
		From("denormalized_span").
		Select(
			goqu.C("trace_id"),
			goqu.C("name"),
			goqu.L("(end_time_unix_nano - start_time_unix_nano) / 1000000").As("duration_ms"),
			goqu.C("scope_name").As("service"),
			goqu.C("start_time_unix_nano").As("start_time"),
		).
		Where(goqu.And(
			goqu.C("parent_span_id").Eq(""),
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
		if err := rows.Scan(&t.TraceID, &t.Name, &t.Duration, &t.Service, &t.StartTime); err != nil {
			return nil, err
		}
		traces = append(traces, t)
	}

	return traces, rows.Err()
}

func (s *TelemetryService) GetPercentileSeries(
	ctx context.Context,
	dateRange DateRange,
	percentile int,
) ([]TimePercentile, error) {
	// clamp percentile
	if percentile < 0 {
		percentile = 0
	}
	if percentile > 100 {
		percentile = 100
	}
	q := float64(percentile) / 100.0

	startNs := dateRange.Start.UnixNano()
	endNs := dateRange.End.UnixNano()
	if endNs <= startNs {
		return nil, fmt.Errorf("invalid date range")
	}

	intervalSQL := GetIntervalFromDateRange(dateRange)

	query := fmt.Sprintf(`
        SELECT
            toStartOfInterval(
                toDateTime(start_time_unix_nano / 1e9),
                INTERVAL %s
            ) AS ts,
            quantile(%f)(
                (end_time_unix_nano - start_time_unix_nano) / 1000000
            ) AS pvalue
        FROM denormalized_span
        WHERE start_time_unix_nano >= %d
          AND end_time_unix_nano   <= %d
        GROUP BY ts
        ORDER BY ts
    `, intervalSQL, q, startNs, endNs)

	rows, err := (*s.Ch).Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// collect actual values
	return PadQueryResult(rows, intervalSQL, dateRange)
}

func (s *TelemetryService) GetAvgDuration(
	ctx context.Context,
	dateRange DateRange,
) ([]TimePercentile, error) {
	startNs := dateRange.Start.UnixNano()
	endNs := dateRange.End.UnixNano()
	if endNs <= startNs {
		return nil, fmt.Errorf("invalid date range")
	}

	intervalSQL := GetIntervalFromDateRange(dateRange)

	// run ClickHouse query
	query := fmt.Sprintf(`
        SELECT
            toStartOfInterval(
                toDateTime(start_time_unix_nano / 1e9),
                INTERVAL %s
            ) AS ts,
            avg((end_time_unix_nano - start_time_unix_nano) / 1000000) AS pvalue
        FROM denormalized_span
        WHERE start_time_unix_nano >= %d
          AND end_time_unix_nano   <= %d
        GROUP BY ts
        ORDER BY ts
    `, intervalSQL, startNs, endNs)

	rows, err := (*s.Ch).Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// collect actual averages
	vals := make(map[time.Time]float64)
	for rows.Next() {
		var ts time.Time
		var v float64
		if err := rows.Scan(&ts, &v); err != nil {
			return nil, err
		}
		vals[ts] = v
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// determine step duration
	step, err := ParseInterval(intervalSQL)
	if err != nil {
		return nil, err
	}

	// align start to ClickHouse buckets
	aligned := AlignToInterval(dateRange.Start, step)

	// build padded series
	var series []TimePercentile
	for ts := aligned; !ts.After(dateRange.End); ts = ts.Add(step) {
		series = append(series, TimePercentile{
			Timestamp: ts,
			Value:     vals[ts], // zero if missing
		})
	}
	return series, nil
}

// factor out your filtering/joining logic into one helper
func (s *TelemetryService) baseSpanDS(query string, startNs, endNs int64) *goqu.SelectDataset {
	ds := s.DB.
		From(goqu.T("span").As("s1")).
		Join(goqu.T("scope"), goqu.On(
			goqu.I("s1.scope_id").Eq(goqu.I("scope.scope_id")),
		)).
		Join(goqu.T("resource_attributes").As("ra"), goqu.On(
			goqu.I("scope.resource_id").Eq(goqu.I("ra.resource_id")),
		))

	conds := []goqu.Expression{
		goqu.I("s1.start_time_unix_nano").Gte(startNs),
		goqu.I("s1.end_time_unix_nano").Lte(endNs),
	}

	if query != "" {
		conds = append(conds, goqu.Or(
			goqu.I("s1.name").Eq(query),
			goqu.I("scope.name").Eq(query),
			goqu.I("s1.trace_id").Eq(query),
			goqu.I("s1.span_id").Eq(query),
			goqu.I("ra.key").Eq(query),
			goqu.I("ra.value").Eq(query),
		))
	}

	return ds.Where(conds...)
}

// getTraceCountForQuery mirrors getPercentileForQuery but returns counts per interval

func (s *TelemetryService) getTraceCountForQuery(
	ctx context.Context,
	queryString string,
	intervalSQL string,
	dateRange DateRange,
) ([]TimePercentile, error) {
	cSeriesQuery := fmt.Sprintf(`
        WITH stats AS (
            %s
        )
        SELECT
            toStartOfInterval(
                toDateTime(stats.start_time_unix_nano / 1e9),
                INTERVAL %s
            ) AS ts,
            count() / 1.0 AS cnt
        FROM stats
        GROUP BY ts
        ORDER BY ts
    `, queryString, intervalSQL)

	cRows, err := (*s.Ch).Query(ctx, cSeriesQuery)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer cRows.Close()

	// Pad missing intervals with zero counts
	cResult, padErr := PadQueryResult(cRows, intervalSQL, dateRange)
	if padErr != nil {
		return nil, padErr
	}
	return cResult, nil
}

func (s *TelemetryService) getAverageDurationForQuery(
	ctx context.Context,
	queryString string,
	intervalSQL string,
	dateRange DateRange,
) ([]TimePercentile, error) {
	avgSeriesQuery := fmt.Sprintf(`
		WITH stats AS (
			%s
		)
		SELECT
			toStartOfInterval(
				toDateTime(stats.start_time_unix_nano / 1e9),
				INTERVAL %s
			) AS ts,
			avg(
				(stats.end_time_unix_nano - stats.start_time_unix_nano) / 1000000
			) AS pvalue
		FROM stats
		GROUP BY ts
		ORDER BY ts
	`, queryString, intervalSQL)

	rows, err := (*s.Ch).Query(ctx, avgSeriesQuery)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	result, padErr := PadQueryResult(rows, intervalSQL, dateRange)
	if padErr != nil {
		return nil, fmt.Errorf("pad error: %w", padErr)
	}
	return result, nil
}
