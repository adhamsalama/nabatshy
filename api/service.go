package api

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"nabatshy/utils"

	"github.com/doug-martin/goqu/v9"
)

var (
	PadQueryResult  = utils.PadQueryResult
	ParseInterval   = utils.ParseInterval
	AlignToInterval = utils.AlignToInterval
)

var GetIntervalFromDateRange = utils.GetIntervalFromDateRange

type TelemetryService struct {
	Ch *sql.DB
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

type SpanEvent struct {
	TimeUnixNano int64             `json:"timeUnixNano"`
	Name         string            `json:"name"`
	Attributes   map[string]string `json:"attributes,omitempty"`
}

type TraceSpan struct {
	SpanID       string      `db:"span_id"`
	ParentSpanID string      `db:"parent_span_id"`
	Name         string      `db:"name"`
	Service      string      `db:"service_name"`
	StartTimeNS  int64       `db:"start_time_unix_nano"`
	EndTimeNS    int64       `db:"end_time_unix_nano"`
	DurationNS   int64       `db:"duration"`
	Events       []SpanEvent `json:"events"`
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
	Kind               string            `db:"kind"`
	Scope              string            `db:"scope_name"`
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
	Events             []SpanEvent       `json:"events"`
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
	HasError      bool    `db:"has_error" json:"hasError"`
	ResourceAttrs map[string]string
	SpanAttrs     map[string]string
}

type SearchResponse struct {
	Results  []SearchResult `json:"results"`
	Page     int            `json:"page"`
	PageSize int            `json:"pageSize"`
}

type SortOption struct {
	Field string `json:"field"`
	Order string `json:"order"`
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

func (s *TelemetryService) GetTopSlowTraces(ctx context.Context, n uint, service string, dr DateRange) ([]SlowTrace, error) {
	conds := []goqu.Expression{
		goqu.C("parent_span_id").Eq(""),
		goqu.C("start_time_unix_nano").Gte(dr.Start.UnixNano()),
		goqu.C("start_time_unix_nano").Lte(dr.End.UnixNano()),
	}
	if service != "" {
		conds = append(conds, goqu.L("resource_attributes['service.name']::VARCHAR = ?", service))
	}
	ds := s.DB.
		From("denormalized_span").
		Select(
			goqu.C("trace_id"),
			goqu.C("name"),
			goqu.L("duration_ns / 1000000").As("duration_ms"),
			goqu.C("scope_name").As("service"),
			goqu.C("start_time_unix_nano"),
		).
		Where(conds...).
		Order(goqu.C("duration_ms").Desc()).
		Limit(n)
	sqlStr, args, err := ds.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := s.Ch.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SlowTrace
	for rows.Next() {
		var t SlowTrace
		if err := rows.Scan(&t.TraceID, &t.Name, &t.Duration, &t.Service, &t.StartTime); err != nil {
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

	rows, err := s.Ch.QueryContext(ctx, sqlStr, args...)
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
			goqu.L("duration_ns").As("duration"),
			goqu.C("events_time_unix_nano").As("event_times"),
			goqu.C("events_name").As("event_names"),
			goqu.L("list_transform(events_attributes, x -> map_keys(x))").As("event_attr_keys"),
			goqu.L("list_transform(events_attributes, x -> map_values(x)::VARCHAR[])").As("event_attr_values"),
		).
		Where(goqu.C("trace_id").Eq(traceID)).
		Order(goqu.C("start_time_unix_nano").Asc())

	sqlStr, args, err := ds.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := s.Ch.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var spans []TraceSpan
	for rows.Next() {
		var s TraceSpan
		var eventTimes utils.Int64Slice
		var eventNames utils.StringSlice
		var eventAttrKeys utils.StringSliceSlice
		var eventAttrValues utils.StringSliceSlice

		if err := rows.Scan(&s.SpanID, &s.ParentSpanID, &s.Name, &s.Service, &s.StartTimeNS, &s.EndTimeNS, &s.DurationNS, &eventTimes, &eventNames, &eventAttrKeys, &eventAttrValues); err != nil {
			return nil, err
		}

		s.Events = make([]SpanEvent, len(eventTimes))
		for i := range eventTimes {
			event := SpanEvent{
				TimeUnixNano: eventTimes[i],
				Name:         eventNames[i],
			}

			if i < len(eventAttrKeys) && i < len(eventAttrValues) {
				attrs := make(map[string]string)
				for j := range eventAttrKeys[i] {
					if j < len(eventAttrValues[i]) {
						attrs[eventAttrKeys[i][j]] = eventAttrValues[i][j]
					}
				}
				event.Attributes = attrs
			}

			s.Events[i] = event
		}

		spans = append(spans, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return treeOrderSpans(spans), nil
}

func treeOrderSpans(spans []TraceSpan) []TraceSpan {
	children := make(map[string][]TraceSpan, len(spans))
	var root TraceSpan
	found := false
	for _, sp := range spans {
		if sp.ParentSpanID == "" {
			root = sp
			found = true
		} else {
			children[sp.ParentSpanID] = append(children[sp.ParentSpanID], sp)
		}
	}
	if !found {
		return spans
	}
	result := make([]TraceSpan, 0, len(spans))
	queue := []TraceSpan{root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		result = append(result, cur)
		queue = append(queue, children[cur.SpanID]...)
	}
	// append any orphaned spans not reachable from root
	if len(result) < len(spans) {
		inResult := make(map[string]bool, len(result))
		for _, sp := range result {
			inResult[sp.SpanID] = true
		}
		for _, sp := range spans {
			if !inResult[sp.SpanID] {
				result = append(result, sp)
			}
		}
	}
	return result
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
			goqu.L("quantile_cont(duration_ns / 1000000, 0.5)").As("p50_duration_ms"),
			goqu.L("quantile_cont(duration_ns / 1000000, 0.9)").As("p90_duration_ms"),
			goqu.L("quantile_cont(duration_ns / 1000000, 0.99)").As("p99_duration_ms"),
			goqu.L("count(*)").As("request_count"),
		).
		Where(goqu.C("parent_span_id").Eq("")).
		GroupBy(goqu.C("name"), goqu.C("scope_name")).
		Order(goqu.L("avg_duration_ms").Desc())

	sqlStr, args, err := ds.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := s.Ch.QueryContext(ctx, sqlStr, args...)
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
		From(goqu.T("denormalized_span").As("s1")).
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

	rows, err := s.Ch.QueryContext(ctx, sqlStr, args...)
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
			goqu.L("date_trunc('hour', epoch_ns(start_time_unix_nano))").As("hour"),
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

	rows, err := s.Ch.QueryContext(ctx, sqlStr, args...)
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
			goqu.I("kind"),
			goqu.I("scope_name"),
			goqu.I("start_time_unix_nano"),
			goqu.I("end_time_unix_nano"),
			goqu.L("duration_ns / 1000000").As("duration_ms"),
			goqu.L("map_keys(resource_attributes)").As("resource_keys"),
			goqu.L("map_values(resource_attributes)::VARCHAR[]").As("resource_values"),
			goqu.L("map_keys(span_attributes)").As("span_keys"),
			goqu.L("map_values(span_attributes)::VARCHAR[]").As("span_values"),
			goqu.C("events_time_unix_nano").As("event_times"),
			goqu.C("events_name").As("event_names"),
			goqu.L("list_transform(events_attributes, x -> map_keys(x))").As("event_attr_keys"),
			goqu.L("list_transform(events_attributes, x -> map_values(x)::VARCHAR[])").As("event_attr_values"),
		).
		Where(goqu.I("span_id").Eq(spanID))

	sqlStr, args, err := ds.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := s.Ch.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("span not found: %s", spanID)
	}

	var detail SpanDetail
	var resourceKeys, resourceValues, spanKeys, spanValues utils.StringSlice
	var eventTimes utils.Int64Slice
	var eventNames utils.StringSlice
	var eventAttrKeys utils.StringSliceSlice
	var eventAttrValues utils.StringSliceSlice

	if err := rows.Scan(
		&detail.SpanID,
		&detail.TraceID,
		&detail.ParentSpanID,
		&detail.Name,
		&detail.Kind,
		&detail.Scope,
		&detail.StartTime,
		&detail.EndTime,
		&detail.Duration,
		&resourceKeys,
		&resourceValues,
		&spanKeys,
		&spanValues,
		&eventTimes,
		&eventNames,
		&eventAttrKeys,
		&eventAttrValues,
	); err != nil {
		return nil, err
	}

	resourceAttrs := make(map[string]string)
	for i := range resourceKeys {
		resourceAttrs[resourceKeys[i]] = resourceValues[i]
	}
	detail.ResourceAttributes = resourceAttrs

	spanAttrs := make(map[string]string)
	for i := range spanKeys {
		spanAttrs[spanKeys[i]] = spanValues[i]
	}
	detail.SpanAttributes = spanAttrs

	detail.Events = make([]SpanEvent, len(eventTimes))
	for i := range eventTimes {
		event := SpanEvent{
			TimeUnixNano: eventTimes[i],
			Name:         eventNames[i],
		}

		if i < len(eventAttrKeys) && i < len(eventAttrValues) {
			attrs := make(map[string]string)
			for j := range eventAttrKeys[i] {
				if j < len(eventAttrValues[i]) {
					attrs[eventAttrKeys[i][j]] = eventAttrValues[i][j]
				}
			}
			event.Attributes = attrs
		}

		detail.Events[i] = event
	}

	avgDS := s.DB.
		From(goqu.T("denormalized_span")).
		Select(
			goqu.L("avg(duration_ns / 1000000)").As("avg_duration_ms"),
			goqu.L("quantile_cont(duration_ns / 1000000, 0.5)").As("p50_duration_ms"),
			goqu.L("quantile_cont(duration_ns / 1000000, 0.9)").As("p90_duration_ms"),
			goqu.L("quantile_cont(duration_ns / 1000000, 0.99)").As("p99_duration_ms"),
		).
		Where(goqu.I("name").Eq(detail.Name))
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
	if err := s.Ch.QueryRowContext(ctx, sqlAvgStr, avgArgs...).Scan(
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
	rawSQL := `
WITH span_with_avgs AS (
    SELECT *, avg(duration_ns) OVER (PARTITION BY trace_id, name) AS avg_dur
    FROM denormalized_span
    WHERE parent_span_id = ''
)
SELECT
    trace_id,
    name AS root_span,
    count(*) AS total_spans,
    max(duration_ns / 1000000) AS duration_ms,
    min(start_time_unix_nano) AS timestamp,
    count(*) FILTER (WHERE duration_ns > avg_dur * 2) AS issues
FROM span_with_avgs
GROUP BY trace_id, name
ORDER BY timestamp DESC
LIMIT 100`

	rows, err := s.Ch.QueryContext(ctx, rawSQL)
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

type AttributeQuery struct {
	Key      string
	Value    string
	Operator string
}

func parseAttributeQuery(query string) []AttributeQuery {
	if query == "" {
		return nil
	}

	containsEqualSign := strings.Contains(query, "=")
	containsGtOrLt := strings.Contains(query, ">") || strings.Contains(query, "<")
	if !containsEqualSign && !containsGtOrLt {
		fmt.Printf("query does not have valid operators, query: %v", query)
		return nil
	}

	pairs := strings.Split(query, ",")
	var attrs []AttributeQuery

	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if strings.Contains(pair, ">=") {
			parts := strings.SplitN(pair, ">=", 2)
			if len(parts) == 2 {
				attrs = append(attrs, AttributeQuery{
					Key:      strings.TrimSpace(parts[0]),
					Value:    strings.TrimSpace(parts[1]),
					Operator: ">=",
				})
			}
		} else if strings.Contains(pair, "<=") {
			parts := strings.SplitN(pair, "<=", 2)
			if len(parts) == 2 {
				attrs = append(attrs, AttributeQuery{
					Key:      strings.TrimSpace(parts[0]),
					Value:    strings.TrimSpace(parts[1]),
					Operator: "<=",
				})
			}
		} else if strings.Contains(pair, "!=") {
			parts := strings.SplitN(pair, "!=", 2)
			if len(parts) == 2 {
				attrs = append(attrs, AttributeQuery{
					Key:      strings.TrimSpace(parts[0]),
					Value:    strings.TrimSpace(parts[1]),
					Operator: "!=",
				})
			}
		} else if strings.Contains(pair, "=") {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) == 2 {
				attrs = append(attrs, AttributeQuery{
					Key:      strings.TrimSpace(parts[0]),
					Value:    strings.TrimSpace(parts[1]),
					Operator: "=",
				})
			}
		} else if strings.Contains(pair, ">") {
			parts := strings.SplitN(pair, ">", 2)
			if len(parts) == 2 {
				attrs = append(attrs, AttributeQuery{
					Key:      strings.TrimSpace(parts[0]),
					Value:    strings.TrimSpace(parts[1]),
					Operator: ">",
				})
			}
		} else if strings.Contains(pair, "<") {
			parts := strings.SplitN(pair, "<", 2)
			if len(parts) == 2 {
				attrs = append(attrs, AttributeQuery{
					Key:      strings.TrimSpace(parts[0]),
					Value:    strings.TrimSpace(parts[1]),
					Operator: "<",
				})
			}
		} else {
			fmt.Printf("unsupported operator in query pair: %v\n", pair)
		}
	}

	if len(attrs) == len(pairs) {
		return attrs
	}

	return nil
}

func (s *TelemetryService) SearchTraces(ctx context.Context, dateRange DateRange, query string, page, pageSize int, sort SortOption, traceOrSpan string) (*SearchResponse, error) {
	totalStart := time.Now()
	defer func() {
		fmt.Printf("[SearchTraces] Total function time: %v\n", time.Since(totalStart))
	}()

	startNano := dateRange.Start.UnixNano()
	endNano := dateRange.End.UnixNano()

	base := s.DB.From(goqu.T("denormalized_span"))

	conds := []goqu.Expression{
		goqu.I("start_time_unix_nano").Gte(startNano),
		goqu.I("end_time_unix_nano").Lte(endNano),
	}

	if query != "" {
		if attrs := parseAttributeQuery(query); attrs != nil {
			var attrConds []goqu.Expression
			for _, attr := range attrs {
				switch attr.Key {
				case "name":
					switch attr.Operator {
					case "=":
						attrConds = append(attrConds, goqu.I("name").Eq(attr.Value))
					case "!=":
						attrConds = append(attrConds, goqu.I("name").Neq(attr.Value))
					}
				case "scope":
					switch attr.Operator {
					case "=":
						attrConds = append(attrConds, goqu.I("scope_name").Eq(attr.Value))
					case "!=":
						attrConds = append(attrConds, goqu.I("scope_name").Neq(attr.Value))
					}
				case "kind":
					switch attr.Operator {
					case "=":
						attrConds = append(attrConds, goqu.I("kind").Eq(strings.ToUpper(attr.Value)))
					case "!=":
						attrConds = append(attrConds, goqu.I("kind").Neq(strings.ToUpper(attr.Value)))
					}
				default:
					switch attr.Operator {
					case "=":
						attrConds = append(attrConds, goqu.Or(
							goqu.L("resource_attributes[?]::VARCHAR = ?", attr.Key, attr.Value),
							goqu.L("span_attributes[?]::VARCHAR = ?", attr.Key, attr.Value),
						))
					case "!=":
						attrConds = append(attrConds, goqu.And(
							goqu.L("resource_attributes[?] IS NULL OR resource_attributes[?]::VARCHAR != ?", attr.Key, attr.Key, attr.Value),
							goqu.L("span_attributes[?] IS NULL OR span_attributes[?]::VARCHAR != ?", attr.Key, attr.Key, attr.Value),
						))
					case ">=":
						attrConds = append(attrConds, goqu.Or(
							goqu.L("CAST(resource_attributes[?] AS FLOAT) >= ?", attr.Key, attr.Value),
							goqu.L("CAST(span_attributes[?] AS FLOAT) >= ?", attr.Key, attr.Value),
						))
					case "<=":
						attrConds = append(attrConds, goqu.Or(
							goqu.L("CAST(resource_attributes[?] AS FLOAT) <= ?", attr.Key, attr.Value),
							goqu.L("CAST(span_attributes[?] AS FLOAT) <= ?", attr.Key, attr.Value),
						))
					case ">":
						attrConds = append(attrConds, goqu.Or(
							goqu.L("CAST(resource_attributes[?] AS FLOAT) > ?", attr.Key, attr.Value),
							goqu.L("CAST(span_attributes[?] AS FLOAT) > ?", attr.Key, attr.Value),
						))
					case "<":
						attrConds = append(attrConds, goqu.Or(
							goqu.L("CAST(resource_attributes[?] AS FLOAT) < ?", attr.Key, attr.Value),
							goqu.L("CAST(span_attributes[?] AS FLOAT) < ?", attr.Key, attr.Value),
						))
					default:
						fmt.Printf("Unsupported operator in attribute query: %s\n", attr.Operator)
					}

				}
			}
			conds = append(conds, goqu.And(attrConds...))
		} else {
			conds = append(conds, goqu.Or(
				goqu.I("name").Eq(query),
				goqu.I("scope_name").Eq(query),
				goqu.I("trace_id").Eq(query),
				goqu.I("span_id").Eq(query),
				goqu.L("list_contains(map_keys(resource_attributes), ?)", query),
				goqu.L("list_contains(map_values(resource_attributes)::VARCHAR[], ?)", query),
				goqu.L("list_contains(map_keys(span_attributes), ?)", query),
				goqu.L("list_contains(map_values(span_attributes)::VARCHAR[], ?)", query),
			))
		}
	}
	switch traceOrSpan {
	case "trace":
		conds = append(conds, goqu.I("parent_span_id").Eq(""))
	case "span":
		conds = append(conds, goqu.I("parent_span_id").Neq(""))
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
			goqu.L("list_contains(events_name, 'exception')").As("has_error"),
			goqu.L("map_keys(resource_attributes)").As("resource_keys"),
			goqu.L("map_values(resource_attributes)::VARCHAR[]").As("resource_values"),
			goqu.L("map_keys(span_attributes)").As("span_keys"),
			goqu.L("map_values(span_attributes)::VARCHAR[]").As("span_values"),
		).
		Where(conds...)

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

	resultsStart := time.Now()
	rows, err := s.Ch.QueryContext(ctx, sqlStr, args...)
	resultsDuration := time.Since(resultsStart)
	fmt.Printf("[SearchTraces] Results query took: %v\n", resultsDuration)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SearchResult
	for rows.Next() {
		var r SearchResult
		var resourceKeys, resourceValues, spanKeys, spanValues utils.StringSlice
		if err := rows.Scan(
			&r.TraceID,
			&r.SpanID,
			&r.Name,
			&r.Service,
			&r.Duration,
			&r.StartTime,
			&r.EndTime,
			&r.HasError,
			&resourceKeys,
			&resourceValues,
			&spanKeys,
			&spanValues,
		); err != nil {
			return nil, err
		}
		resourceAttrs := make(map[string]string, len(resourceKeys))
		for i := range resourceKeys {
			resourceAttrs[resourceKeys[i]] = resourceValues[i]
		}
		r.ResourceAttrs = resourceAttrs
		spanAttrs := make(map[string]string, len(spanKeys))
		for i := range spanKeys {
			spanAttrs[spanKeys[i]] = spanValues[i]
		}
		r.SpanAttrs = spanAttrs
		results = append(results, r)
	}

	return &SearchResponse{
		Results:  results,
		Page:     page,
		PageSize: pageSize,
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
	startNano := dateRange.Start.UnixNano()
	endNano := dateRange.End.UnixNano()
	timeFilter := fmt.Sprintf(
		"start_time_unix_nano >= %d AND start_time_unix_nano <= %d",
		startNano, endNano,
	)
	intervalSQL := GetIntervalFromDateRange(dateRange)

	query := fmt.Sprintf(`
        SELECT
            time_bucket(INTERVAL '%s', to_timestamp(start_time_unix_nano / 1e9), TIMESTAMPTZ 'epoch') AS ts,
            count(*) AS cnt
        FROM denormalized_span
        WHERE %s
        GROUP BY ts
        ORDER BY ts ASC
    `, intervalSQL, timeFilter)

	rows, err := s.Ch.QueryContext(ctx, query)
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
		counts[ts.UTC()] = cnt
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
		timeFilter = fmt.Sprintf("start_time_unix_nano >= epoch_ns('%s'::TIMESTAMPTZ) AND start_time_unix_nano <= epoch_ns('%s'::TIMESTAMPTZ)",
			start.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano))
	} else {
		switch timeRange {
		case "1h":
			timeFilter = "start_time_unix_nano >= epoch_ns(now()) - 3600000000000"
		case "24h":
			timeFilter = "start_time_unix_nano >= epoch_ns(now()) - 86400000000000"
		case "7d":
			timeFilter = "start_time_unix_nano >= epoch_ns(now()) - 604800000000000"
		case "30d":
			timeFilter = "start_time_unix_nano >= epoch_ns(now()) - 2592000000000000"
		default:
			timeFilter = "start_time_unix_nano >= epoch_ns(now()) - 86400000000000"
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
			count(*) FILTER (WHERE d.duration_ms > s.avg_duration * 2) / count(*) * 100 AS error_rate
		FROM durations d
		JOIN service_stats s ON d.service = s.service
		GROUP BY d.service
		ORDER BY count DESC`

	rows, err := s.Ch.QueryContext(ctx, query)
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
			quantile_cont(duration_ms, 0.95) AS p95_duration_ms
		FROM durations
		GROUP BY endpoint
		LIMIT 10`

	rows, err := s.Ch.QueryContext(ctx, query)
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
		timeFilter = "start_time_unix_nano >= epoch_ns(now()) - 3600000000000"
	case "24h":
		timeFilter = "start_time_unix_nano >= epoch_ns(now()) - 86400000000000"
	case "7d":
		timeFilter = "start_time_unix_nano >= epoch_ns(now()) - 604800000000000"
	case "30d":
		timeFilter = "start_time_unix_nano >= epoch_ns(now()) - 2592000000000000"
	default:
		timeFilter = "start_time_unix_nano >= epoch_ns(now()) - 86400000000000"
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

	rows, err := s.Ch.QueryContext(ctx, sqlStr, args...)
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
            time_bucket(INTERVAL '%s', to_timestamp(start_time_unix_nano / 1e9), TIMESTAMPTZ 'epoch') AS ts,
            quantile_cont((end_time_unix_nano - start_time_unix_nano) / 1000000, %f) AS pvalue
        FROM denormalized_span
        WHERE start_time_unix_nano >= %d
          AND end_time_unix_nano   <= %d
        GROUP BY ts
        ORDER BY ts
    `, intervalSQL, q, startNs, endNs)

	rows, err := s.Ch.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

	query := fmt.Sprintf(`
        SELECT
            time_bucket(INTERVAL '%s', to_timestamp(start_time_unix_nano / 1e9), TIMESTAMPTZ 'epoch') AS ts,
            avg((end_time_unix_nano - start_time_unix_nano) / 1000000) AS pvalue
        FROM denormalized_span
        WHERE start_time_unix_nano >= %d
          AND end_time_unix_nano   <= %d
        GROUP BY ts
        ORDER BY ts
    `, intervalSQL, startNs, endNs)

	rows, err := s.Ch.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	vals := make(map[time.Time]float64)
	for rows.Next() {
		var ts time.Time
		var v float64
		if err := rows.Scan(&ts, &v); err != nil {
			return nil, err
		}
		vals[ts.UTC()] = v
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	step, err := ParseInterval(intervalSQL)
	if err != nil {
		return nil, err
	}

	aligned := AlignToInterval(dateRange.Start, step)

	var series []TimePercentile
	for ts := aligned; !ts.After(dateRange.End); ts = ts.Add(step) {
		series = append(series, TimePercentile{
			Timestamp: ts,
			Value:     vals[ts],
		})
	}
	return series, nil
}

func (s *TelemetryService) GetErrorCounts(
	ctx context.Context,
	dateRange DateRange,
	service string,
) ([]TimeCount, error) {
	startNano := dateRange.Start.UnixNano()
	endNano := dateRange.End.UnixNano()
	intervalSQL := GetIntervalFromDateRange(dateRange)

	serviceFilter := ""
	if service != "" {
		serviceFilter = fmt.Sprintf(` AND resource_attributes['service.name']::VARCHAR = '%s'`, service)
	}

	query := fmt.Sprintf(`
		SELECT
			time_bucket(INTERVAL '%s', to_timestamp(start_time_unix_nano / 1e9), TIMESTAMPTZ 'epoch') AS ts,
			count(*) FILTER (WHERE list_contains(events_name, 'exception')) AS cnt
		FROM denormalized_span
		WHERE start_time_unix_nano >= %d AND start_time_unix_nano <= %d%s
		GROUP BY ts
		ORDER BY ts ASC
	`, intervalSQL, startNano, endNano, serviceFilter)

	rows, err := s.Ch.QueryContext(ctx, query)
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
		counts[ts.UTC()] = cnt
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

type CombinedMetricsResult struct {
	PercentileResults  []TimePercentile
	TraceCountResults  []TimePercentile
	AvgDurationResults []TimePercentile
}

func (s *TelemetryService) getCombinedMetricsForQuery(
	ctx context.Context,
	queryString string,
	queryArgs []interface{},
	intervalSQL string,
	dateRange DateRange,
	percentile int,
) (*CombinedMetricsResult, error) {
	pFloat := float64(percentile) / 100.0

	combinedQuery := fmt.Sprintf(`
		WITH stats AS (
			%s
		)
		SELECT
			time_bucket(INTERVAL '%s', to_timestamp(stats.start_time_unix_nano / 1e9), TIMESTAMPTZ 'epoch') AS ts,
			quantile_cont((stats.end_time_unix_nano - stats.start_time_unix_nano) / 1000000, %f) AS percentile_value,
			count(*) * 1.0 AS trace_count,
			avg((stats.end_time_unix_nano - stats.start_time_unix_nano) / 1000000) AS avg_duration
		FROM stats
		GROUP BY ts
		ORDER BY ts
	`, queryString, intervalSQL, pFloat)

	queryStart := time.Now()
	rows, err := s.Ch.QueryContext(ctx, combinedQuery, queryArgs...)
	queryDuration := time.Since(queryStart)
	fmt.Printf("[getCombinedMetricsForQuery] DuckDB query took: %v\n", queryDuration)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	percentileMap := make(map[time.Time]float64)
	traceCountMap := make(map[time.Time]float64)
	avgDurationMap := make(map[time.Time]float64)

	for rows.Next() {
		var ts time.Time
		var pValue, tcValue, avgValue float64
		if err := rows.Scan(&ts, &pValue, &tcValue, &avgValue); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		percentileMap[ts.UTC()] = pValue
		traceCountMap[ts.UTC()] = tcValue
		avgDurationMap[ts.UTC()] = avgValue
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	intervalDur, err := ParseInterval(intervalSQL)
	if err != nil {
		return nil, fmt.Errorf("invalid interval: %w", err)
	}

	alignedStart := AlignToInterval(dateRange.Start, intervalDur)

	var percentileResult []TimePercentile
	var traceCountResult []TimePercentile
	var avgDurationResult []TimePercentile

	for ts := alignedStart; !ts.After(dateRange.End); ts = ts.Add(intervalDur) {
		percentileResult = append(percentileResult, TimePercentile{
			Timestamp: ts,
			Value:     percentileMap[ts],
		})
		traceCountResult = append(traceCountResult, TimePercentile{
			Timestamp: ts,
			Value:     traceCountMap[ts],
		})
		avgDurationResult = append(avgDurationResult, TimePercentile{
			Timestamp: ts,
			Value:     avgDurationMap[ts],
		})
	}

	return &CombinedMetricsResult{
		PercentileResults:  percentileResult,
		TraceCountResults:  traceCountResult,
		AvgDurationResults: avgDurationResult,
	}, nil
}

func (s *TelemetryService) GetSearchMetrics(ctx context.Context, dateRange DateRange, query string, percentile int, traceOrSpan string) (*CombinedMetricsResult, error) {
	startNano := dateRange.Start.UnixNano()
	endNano := dateRange.End.UnixNano()

	base := s.DB.From(goqu.T("denormalized_span"))

	conds := []goqu.Expression{
		goqu.I("start_time_unix_nano").Gte(startNano),
		goqu.I("end_time_unix_nano").Lte(endNano),
	}

	if query != "" {
		if attrs := parseAttributeQuery(query); attrs != nil {
			var attrConds []goqu.Expression
			for _, attr := range attrs {
				switch attr.Key {
				case "name":
					switch attr.Operator {
					case "=":
						attrConds = append(attrConds, goqu.I("name").Eq(attr.Value))
					case "!=":
						attrConds = append(attrConds, goqu.I("name").Neq(attr.Value))
					}
				case "scope":
					switch attr.Operator {
					case "=":
						attrConds = append(attrConds, goqu.I("scope_name").Eq(attr.Value))
					case "!=":
						attrConds = append(attrConds, goqu.I("scope_name").Neq(attr.Value))
					}
				case "kind":
					switch attr.Operator {
					case "=":
						attrConds = append(attrConds, goqu.I("kind").Eq(strings.ToUpper(attr.Value)))
					case "!=":
						attrConds = append(attrConds, goqu.I("kind").Neq(strings.ToUpper(attr.Value)))
					}
				default:
					switch attr.Operator {
					case "=":
						attrConds = append(attrConds, goqu.Or(
							goqu.L("resource_attributes[?]::VARCHAR = ?", attr.Key, attr.Value),
							goqu.L("span_attributes[?]::VARCHAR = ?", attr.Key, attr.Value),
						))
					case "!=":
						attrConds = append(attrConds, goqu.And(
							goqu.L("resource_attributes[?] IS NULL OR resource_attributes[?]::VARCHAR != ?", attr.Key, attr.Key, attr.Value),
							goqu.L("span_attributes[?] IS NULL OR span_attributes[?]::VARCHAR != ?", attr.Key, attr.Key, attr.Value),
						))
					case ">=":
						attrConds = append(attrConds, goqu.Or(
							goqu.L("CAST(resource_attributes[?] AS FLOAT) >= ?", attr.Key, attr.Value),
							goqu.L("CAST(span_attributes[?] AS FLOAT) >= ?", attr.Key, attr.Value),
						))
					case "<=":
						attrConds = append(attrConds, goqu.Or(
							goqu.L("CAST(resource_attributes[?] AS FLOAT) <= ?", attr.Key, attr.Value),
							goqu.L("CAST(span_attributes[?] AS FLOAT) <= ?", attr.Key, attr.Value),
						))
					case ">":
						attrConds = append(attrConds, goqu.Or(
							goqu.L("CAST(resource_attributes[?] AS FLOAT) > ?", attr.Key, attr.Value),
							goqu.L("CAST(span_attributes[?] AS FLOAT) > ?", attr.Key, attr.Value),
						))
					case "<":
						attrConds = append(attrConds, goqu.Or(
							goqu.L("CAST(resource_attributes[?] AS FLOAT) < ?", attr.Key, attr.Value),
							goqu.L("CAST(span_attributes[?] AS FLOAT) < ?", attr.Key, attr.Value),
						))
					default:
						fmt.Printf("Unsupported operator in attribute query: %s\n", attr.Operator)
					}

				}
			}
			conds = append(conds, goqu.And(attrConds...))
		} else {
			conds = append(conds, goqu.Or(
				goqu.I("name").Eq(query),
				goqu.I("scope_name").Eq(query),
				goqu.I("trace_id").Eq(query),
				goqu.I("span_id").Eq(query),
				goqu.L("list_contains(map_keys(resource_attributes), ?)", query),
				goqu.L("list_contains(map_values(resource_attributes)::VARCHAR[], ?)", query),
				goqu.L("list_contains(map_keys(span_attributes), ?)", query),
				goqu.L("list_contains(map_values(span_attributes)::VARCHAR[], ?)", query),
			))
		}
	}

	switch traceOrSpan {
	case "trace":
		conds = append(conds, goqu.I("parent_span_id").Eq(""))
	case "span":
		conds = append(conds, goqu.I("parent_span_id").Neq(""))
	}

	ds := base.Select(
		goqu.I("start_time_unix_nano"),
		goqu.I("end_time_unix_nano"),
	).Where(conds...)

	queryString, queryArgs, _ := ds.ToSQL()
	intervalSQL := GetIntervalFromDateRange(dateRange)

	return s.getCombinedMetricsForQuery(ctx, queryString, queryArgs, intervalSQL, dateRange, percentile)
}

func (s *TelemetryService) GetUniqueServiceNames(ctx context.Context) ([]string, error) {
	query := `
		SELECT DISTINCT
			resource_attributes['service.name']::VARCHAR AS service_name
		FROM denormalized_span
		WHERE resource_attributes['service.name'] IS NOT NULL
		ORDER BY service_name
	`

	rows, err := s.Ch.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var services []string
	for rows.Next() {
		var serviceName string
		if err := rows.Scan(&serviceName); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		if serviceName != "" {
			services = append(services, serviceName)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return services, nil
}

type OtelMetricRow struct {
	MetricName             string            `json:"metric_name"`
	MetricType             string            `json:"metric_type"`
	MetricUnit             string            `json:"metric_unit"`
	TimeUnixNano           int64             `json:"time_unix_nano"`
	ValueDouble            float64           `json:"value_double"`
	ValueInt               int64             `json:"value_int"`
	AggregationTemporality string            `json:"aggregation_temporality"`
	IsMonotonic            bool              `json:"is_monotonic"`
	HistogramCount         int64             `json:"histogram_count"`
	HistogramSum           float64           `json:"histogram_sum"`
	ScopeName              string            `json:"scope_name"`
	Attributes             map[string]string `json:"attributes"`
	ResourceAttributes     map[string]string `json:"resource_attributes"`
}

func (s *TelemetryService) GetOtelMetrics(ctx context.Context, limit int, metricName string, dr *DateRange) ([]OtelMetricRow, error) {
	var conds []string
	var args []any

	if metricName != "" {
		conds = append(conds, "metric_name = ?")
		args = append(args, metricName)
	}
	if dr != nil {
		conds = append(conds, "time_unix_nano >= ?")
		args = append(args, dr.Start.UnixNano())
		conds = append(conds, "time_unix_nano <= ?")
		args = append(args, dr.End.UnixNano())
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}

	query := fmt.Sprintf(`
		SELECT
			metric_name, metric_type, metric_unit,
			time_unix_nano, value_double, value_int,
			aggregation_temporality, is_monotonic,
			histogram_count, histogram_sum,
			scope_name,
			map_keys(attributes), map_values(attributes)::VARCHAR[],
			map_keys(resource_attributes), map_values(resource_attributes)::VARCHAR[]
		FROM metric_data_point
		%s
		ORDER BY time_unix_nano DESC
		LIMIT %d
	`, where, limit)

	rows, err := s.Ch.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var results []OtelMetricRow
	for rows.Next() {
		var r OtelMetricRow
		var attrKeys, attrValues, resKeys, resValues utils.StringSlice
		if err := rows.Scan(
			&r.MetricName, &r.MetricType, &r.MetricUnit,
			&r.TimeUnixNano, &r.ValueDouble, &r.ValueInt,
			&r.AggregationTemporality, &r.IsMonotonic,
			&r.HistogramCount, &r.HistogramSum,
			&r.ScopeName,
			&attrKeys, &attrValues,
			&resKeys, &resValues,
		); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		r.Attributes = make(map[string]string, len(attrKeys))
		for i := range attrKeys {
			if i < len(attrValues) {
				r.Attributes[attrKeys[i]] = attrValues[i]
			}
		}
		r.ResourceAttributes = make(map[string]string, len(resKeys))
		for i := range resKeys {
			if i < len(resValues) {
				r.ResourceAttributes[resKeys[i]] = resValues[i]
			}
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return results, nil
}

// --- metric names ---

type OtelMetricNameRow struct {
	MetricName    string   `json:"metric_name"`
	MetricType    string   `json:"metric_type"`
	MetricUnit    string   `json:"metric_unit"`
	ScopeName     string   `json:"scope_name"`
	Count         int64    `json:"count"`
	AttributeKeys []string `json:"attribute_keys"`
}

func (s *TelemetryService) GetOtelMetricNames(ctx context.Context) ([]OtelMetricNameRow, error) {
	query := `
		SELECT
			metric_name,
			metric_type,
			metric_unit,
			scope_name,
			COUNT(*) AS count,
			list_distinct(flatten(list(map_keys(attributes)))) AS attribute_keys
		FROM metric_data_point
		GROUP BY metric_name, metric_type, metric_unit, scope_name
		ORDER BY metric_name
	`
	rows, err := s.Ch.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var results []OtelMetricNameRow
	for rows.Next() {
		var r OtelMetricNameRow
		var attrKeys utils.StringSlice
		if err := rows.Scan(&r.MetricName, &r.MetricType, &r.MetricUnit, &r.ScopeName, &r.Count, &attrKeys); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		r.AttributeKeys = []string(attrKeys)
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return results, nil
}

// --- metric time series ---

type OtelSeriesPoint struct {
	Time           int64   `json:"time"` // epoch milliseconds
	Value          float64 `json:"value"`
	HistogramCount int64   `json:"histogram_count,omitempty"`
	HistogramSum   float64 `json:"histogram_sum,omitempty"`
}

type OtelSeries struct {
	Labels map[string]string `json:"labels"`
	Points []OtelSeriesPoint `json:"points"`
}

type OtelMetricSeriesResponse struct {
	Series         []OtelSeries `json:"series"`
	MetricType     string       `json:"metric_type"`
	MetricUnit     string       `json:"metric_unit"`
	BucketInterval string       `json:"bucket_interval"`
}

func (s *TelemetryService) GetOtelMetricSeries(ctx context.Context, metricName string, dr DateRange, groupBy string) (*OtelMetricSeriesResponse, error) {
	intervalSQL := GetIntervalFromDateRange(dr)
	startNano := dr.Start.UnixNano()
	endNano := dr.End.UnixNano()

	args := []any{}
	var groupSelect string
	if groupBy != "" {
		groupSelect = "COALESCE(attributes[?]::VARCHAR, '')"
		args = append(args, groupBy)
	} else {
		groupSelect = "''"
	}
	args = append(args, metricName, startNano, endNano)

	query := fmt.Sprintf(`
		SELECT
			epoch_ms(time_bucket(INTERVAL '%s', to_timestamp(time_unix_nano / 1000000000.0))) AS bucket_ms,
			%s AS group_label,
			AVG(CASE WHEN value_double != 0 THEN value_double ELSE CAST(value_int AS DOUBLE) END) AS avg_value,
			CAST(SUM(histogram_count) AS BIGINT) AS hist_count,
			SUM(histogram_sum) AS hist_sum,
			ANY_VALUE(metric_type) AS metric_type,
			ANY_VALUE(metric_unit) AS metric_unit
		FROM metric_data_point
		WHERE metric_name = ?
		  AND time_unix_nano >= ?
		  AND time_unix_nano <= ?
		GROUP BY bucket_ms, group_label
		ORDER BY bucket_ms, group_label
	`, intervalSQL, groupSelect)

	rows, err := s.Ch.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	seriesMap := make(map[string][]OtelSeriesPoint)
	var labelOrder []string
	var lastMetricType, lastMetricUnit string

	for rows.Next() {
		var bucketMs int64
		var groupLabel string
		var avgValue, histSum float64
		var histCount int64
		var metricType, metricUnit string

		if err := rows.Scan(&bucketMs, &groupLabel, &avgValue, &histCount, &histSum, &metricType, &metricUnit); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		lastMetricType = metricType
		lastMetricUnit = metricUnit

		var value float64
		if metricType == "histogram" {
			if histCount > 0 {
				value = histSum / float64(histCount)
			}
		} else {
			value = avgValue
		}

		if _, seen := seriesMap[groupLabel]; !seen {
			labelOrder = append(labelOrder, groupLabel)
		}
		seriesMap[groupLabel] = append(seriesMap[groupLabel], OtelSeriesPoint{
			Time:           bucketMs,
			Value:          value,
			HistogramCount: histCount,
			HistogramSum:   histSum,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	var series []OtelSeries
	for _, label := range labelOrder {
		labels := map[string]string{}
		if groupBy != "" && label != "" {
			labels[groupBy] = label
		}
		series = append(series, OtelSeries{
			Labels: labels,
			Points: seriesMap[label],
		})
	}
	if series == nil {
		series = []OtelSeries{}
	}

	return &OtelMetricSeriesResponse{
		Series:         series,
		MetricType:     lastMetricType,
		MetricUnit:     lastMetricUnit,
		BucketInterval: intervalSQL,
	}, nil
}

type LogRow struct {
	TimestampUnixNano  int64             `json:"timestamp_unix_nano"`
	SeverityText       string            `json:"severity_text"`
	SeverityNumber     int32             `json:"severity_number"`
	Body               string            `json:"body"`
	TraceID            string            `json:"trace_id"`
	SpanID             string            `json:"span_id"`
	ServiceName        string            `json:"service_name"`
	ScopeName          string            `json:"scope_name"`
	Attributes         map[string]string `json:"attributes"`
	ResourceAttributes map[string]string `json:"resource_attributes"`
}

func (s *TelemetryService) GetLogs(ctx context.Context, dr DateRange, traceID, spanID, service, severity, body, sortField, sortDir string, page, pageSize int) ([]LogRow, error) {
	if pageSize <= 0 {
		pageSize = 50
	}
	if page <= 0 {
		page = 1
	}
	offset := (page - 1) * pageSize

	allowedSortFields := map[string]string{
		"timestamp": "timestamp_unix_nano",
		"severity":  "severity_number",
		"service":   "service_name",
		"body":      "body",
	}
	orderCol := "timestamp_unix_nano"
	if col, ok := allowedSortFields[sortField]; ok {
		orderCol = col
	}
	orderDir := "DESC"
	if sortDir == "asc" {
		orderDir = "ASC"
	}

	var query string
	var args []any

	if body != "" {
		var outerConds []string
		args = append(args, body)
		if spanID != "" {
			outerConds = append(outerConds, "span_id = ?")
			args = append(args, spanID)
		} else if traceID != "" {
			outerConds = append(outerConds, "trace_id = ?")
			args = append(args, traceID)
		} else {
			outerConds = append(outerConds, "timestamp_unix_nano >= ?", "timestamp_unix_nano <= ?")
			args = append(args, dr.Start.UnixNano(), dr.End.UnixNano())
		}
		if service != "" {
			outerConds = append(outerConds, "service_name = ?")
			args = append(args, service)
		}
		if severity != "" {
			outerConds = append(outerConds, "severity_text = ?")
			args = append(args, severity)
		}
		outerConds = append(outerConds, "__score IS NOT NULL")
		args = append(args, pageSize, offset)

		query = fmt.Sprintf(`
			SELECT
				timestamp_unix_nano, severity_text, severity_number,
				body, trace_id, span_id, service_name, scope_name,
				map_keys(attributes), map_values(attributes)::VARCHAR[],
				map_keys(resource_attributes), map_values(resource_attributes)::VARCHAR[]
			FROM (
				SELECT *, fts_main_log_record.match_bm25(rowid, ?) AS __score
				FROM log_record
			)
			WHERE %s
			ORDER BY %s %s
			LIMIT ? OFFSET ?
		`, strings.Join(outerConds, " AND "), orderCol, orderDir)
	} else {
		var conds []string
		if spanID != "" {
			conds = append(conds, "span_id = ?")
			args = append(args, spanID)
		} else if traceID != "" {
			conds = append(conds, "trace_id = ?")
			args = append(args, traceID)
		} else {
			conds = append(conds, "timestamp_unix_nano >= ?", "timestamp_unix_nano <= ?")
			args = append(args, dr.Start.UnixNano(), dr.End.UnixNano())
		}
		if service != "" {
			conds = append(conds, "service_name = ?")
			args = append(args, service)
		}
		if severity != "" {
			conds = append(conds, "severity_text = ?")
			args = append(args, severity)
		}
		args = append(args, pageSize, offset)

		query = fmt.Sprintf(`
			SELECT
				timestamp_unix_nano, severity_text, severity_number,
				body, trace_id, span_id, service_name, scope_name,
				map_keys(attributes), map_values(attributes)::VARCHAR[],
				map_keys(resource_attributes), map_values(resource_attributes)::VARCHAR[]
			FROM log_record
			WHERE %s
			ORDER BY %s %s
			LIMIT ? OFFSET ?
		`, strings.Join(conds, " AND "), orderCol, orderDir)
	}

	rows, err := s.Ch.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var result []LogRow
	for rows.Next() {
		var r LogRow
		var attrKeys, attrValues, resKeys, resValues utils.StringSlice
		if err := rows.Scan(
			&r.TimestampUnixNano, &r.SeverityText, &r.SeverityNumber,
			&r.Body, &r.TraceID, &r.SpanID, &r.ServiceName, &r.ScopeName,
			&attrKeys, &attrValues,
			&resKeys, &resValues,
		); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		r.Attributes = make(map[string]string, len(attrKeys))
		for i := range attrKeys {
			if i < len(attrValues) {
				r.Attributes[attrKeys[i]] = attrValues[i]
			}
		}
		r.ResourceAttributes = make(map[string]string, len(resKeys))
		for i := range resKeys {
			if i < len(resValues) {
				r.ResourceAttributes[resKeys[i]] = resValues[i]
			}
		}
		result = append(result, r)
	}
	if result == nil {
		result = []LogRow{}
	}
	return result, rows.Err()
}

type LogVolumeBucket struct {
	BucketMs int64  `json:"bucket_ms"`
	Severity string `json:"severity"`
	Count    int64  `json:"count"`
}

func (s *TelemetryService) GetLogVolume(ctx context.Context, dr DateRange, service, severity, body string) ([]LogVolumeBucket, error) {
	interval := GetIntervalFromDateRange(dr)

	var query string
	var args []any

	if body != "" {
		var outerConds []string
		args = append(args, body)
		outerConds = append(outerConds, "timestamp_unix_nano >= ?", "timestamp_unix_nano <= ?", "__score IS NOT NULL")
		args = append(args, dr.Start.UnixNano(), dr.End.UnixNano())
		if service != "" {
			outerConds = append(outerConds, "service_name = ?")
			args = append(args, service)
		}
		if severity != "" {
			outerConds = append(outerConds, "severity_text = ?")
			args = append(args, severity)
		}

		query = fmt.Sprintf(`
			SELECT
				epoch_ms(time_bucket(INTERVAL '%s', to_timestamp(timestamp_unix_nano / 1e9), TIMESTAMPTZ 'epoch')) AS bucket_ms,
				UPPER(COALESCE(NULLIF(severity_text, ''), 'UNKNOWN')) AS sev,
				COUNT(*) AS cnt
			FROM (
				SELECT *, fts_main_log_record.match_bm25(rowid, ?) AS __score
				FROM log_record
			)
			WHERE %s
			GROUP BY bucket_ms, sev
			ORDER BY bucket_ms, sev
		`, interval, strings.Join(outerConds, " AND "))
	} else {
		var conds []string
		conds = append(conds, "timestamp_unix_nano >= ?", "timestamp_unix_nano <= ?")
		args = append(args, dr.Start.UnixNano(), dr.End.UnixNano())
		if service != "" {
			conds = append(conds, "service_name = ?")
			args = append(args, service)
		}
		if severity != "" {
			conds = append(conds, "severity_text = ?")
			args = append(args, severity)
		}

		query = fmt.Sprintf(`
			SELECT
				epoch_ms(time_bucket(INTERVAL '%s', to_timestamp(timestamp_unix_nano / 1e9), TIMESTAMPTZ 'epoch')) AS bucket_ms,
				UPPER(COALESCE(NULLIF(severity_text, ''), 'UNKNOWN')) AS sev,
				COUNT(*) AS cnt
			FROM log_record
			WHERE %s
			GROUP BY bucket_ms, sev
			ORDER BY bucket_ms, sev
		`, interval, strings.Join(conds, " AND "))
	}

	rows, err := s.Ch.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var result []LogVolumeBucket
	for rows.Next() {
		var b LogVolumeBucket
		if err := rows.Scan(&b.BucketMs, &b.Severity, &b.Count); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		result = append(result, b)
	}
	if result == nil {
		result = []LogVolumeBucket{}
	}
	return result, rows.Err()
}
