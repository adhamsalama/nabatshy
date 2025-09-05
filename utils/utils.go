package utils

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	clickhouseDriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func PadQueryResult(rows clickhouseDriver.Rows, intervalSQL string, dateRange DateRange) ([]TimePercentile, error) {
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

func ParseInterval(interval string) (time.Duration, error) {
	parts := strings.Fields(interval)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid interval format: %q", interval)
	}

	n, err := strconv.Atoi(parts[0])
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid interval count: %q", parts[0])
	}

	unit := strings.ToLower(parts[1])
	switch unit {
	case "second", "seconds":
		return time.Duration(n) * time.Second, nil
	case "minute", "minutes":
		return time.Duration(n) * time.Minute, nil
	case "hour", "hours":
		return time.Duration(n) * time.Hour, nil
	case "day", "days":
		return time.Duration(n) * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported interval unit: %q", unit)
	}
}

func AlignToInterval(t time.Time, interval time.Duration) time.Time {
	secs := int64(interval.Seconds())
	unix := t.Unix()
	alignedUnix := unix - (unix % secs)
	return time.Unix(alignedUnix, 0).UTC()
}

func GetIntervalFromDateRange(dr DateRange) string {
	numOfBuckets := 15
	secs := max(int(dr.End.Sub(dr.Start).Seconds())/numOfBuckets, 1)
	return fmt.Sprintf("%d second", secs)
}

func ParseDateRange(query url.Values, startField, endField, timeRangeField string) (DateRange, error) {
	startStr := query.Get(startField)
	endStr := query.Get(endField)
	if startStr != "" && endStr != "" {
		startTime, err1 := time.Parse(time.RFC3339, startStr)
		endTime, err2 := time.Parse(time.RFC3339, endStr)
		if err1 == nil && err2 == nil {
			return DateRange{Start: startTime, End: endTime}, nil
		}
		return DateRange{}, fmt.Errorf("invalid start or end time format")
	}

	timeRange := query.Get(timeRangeField)
	return GetDateRangeFromQuery(timeRange), nil
}

func GetDateRangeFromQuery(timeRange string) DateRange {
	end := time.Now()
	if len(timeRange) < 2 {
		return DateRange{Start: end, End: end} // invalid input fallback
	}

	unit := timeRange[len(timeRange)-1:]
	valueStr := timeRange[:len(timeRange)-1]
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return DateRange{Start: end, End: end} // invalid number
	}

	var duration time.Duration
	switch unit {
	case "s":
		duration = time.Duration(value) * time.Second
	case "m":
		duration = time.Duration(value) * time.Minute
	case "h":
		duration = time.Duration(value) * time.Hour
	case "d":
		duration = time.Duration(value) * 24 * time.Hour
	default:
		return DateRange{Start: end, End: end} // unsupported unit
	}

	start := end.Add(-duration)
	dateRange := DateRange{Start: start, End: end}

	fmt.Printf("dateRange: %v\n", dateRange)
	return dateRange
}

func InsertDenormalizedSpans(
	ch *clickhouseDriver.Conn,
	ctx context.Context,
	spans []Span,
) error {
	if len(spans) == 0 {
		return nil
	}

	batch, err := (*ch).PrepareBatch(ctx, "INSERT INTO denormalized_span")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, span := range spans {

		// extarct resource attributes keys and values to its own slices
		resourcesAttrsKyes := make([]string, 0, len(span.ResourceAttributes))
		resourcesAttrsValues := make([]string, 0, len(span.ResourceAttributes))
		for _, attr := range span.ResourceAttributes {
			resourcesAttrsKyes = append(resourcesAttrsKyes, attr.Key)
			resourcesAttrsValues = append(resourcesAttrsValues, attr.Value)
		}

		// extract span attributes keys and values to its own slices
		spanAttrsKeys := make([]string, 0, len(span.SpanAttributes))
		spanAttrsValues := make([]string, 0, len(span.SpanAttributes))
		for _, attr := range span.SpanAttributes {
			spanAttrsKeys = append(spanAttrsKeys, attr.Key)
			spanAttrsValues = append(spanAttrsValues, attr.Value)
		}

		eventKeys := make([]string, 0, len(span.Events))
		eventValues := make([]int64, 0, len(span.Events))
		for _, event := range span.Events {
			eventKeys = append(eventKeys, event.Name)
			eventValues = append(eventValues, event.TimeUnixNano)
		}

		if err := batch.Append(
			span.TraceID,           // trace_id
			span.SpanID,            // span_id
			span.ParentSpanID,      // parent_span_id
			span.Flags,             // flags
			span.Name,              // name
			span.StartTimeUnixNano, // start_time_unix_nano
			span.EndTimeUnixNano,   // end_time_unix_nano
			span.ScopeID,           // scope_id
			span.ScopeName,         // scope_name
			span.ResourceID,        // resource_id
			span.ResourceSchemaURL, // resource_schema_url
			resourcesAttrsKyes,     // resource_attributes_keys
			resourcesAttrsValues,   // resource_attributes_values
			eventValues,            // event_values
			eventKeys,              // event_keys
			spanAttrsKeys,          // span_attributes.key
			spanAttrsValues,        // span_attributes.value
		); err != nil {
			return fmt.Errorf("failed to append span: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}
