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

// DenormalizedSpanRow represents a row in the denormalized_span table
type DenormalizedSpanRow struct {
	TraceID                 string   `ch:"trace_id"`
	SpanID                  string   `ch:"span_id"`
	ParentSpanID            string   `ch:"parent_span_id"`
	Flags                   int32    `ch:"flags"`
	Name                    string   `ch:"name"`
	StartTimeUnixNano       int64    `ch:"start_time_unix_nano"`
	EndTimeUnixNano         int64    `ch:"end_time_unix_nano"`
	ScopeID                 string   `ch:"scope_id"`
	ScopeName               string   `ch:"scope_name"`
	ResourceID              string   `ch:"resource_id"`
	ResourceSchemaURL       string   `ch:"resource_schema_url"`
	ResourceAttributesKey      []string   `ch:"resource_attributes.key"`
	ResourceAttributesValue    []string   `ch:"resource_attributes.value"`
	SpanAttributesKey          []string   `ch:"span_attributes.key"`
	SpanAttributesValue        []string   `ch:"span_attributes.value"`
	EventsTimeUnixNano         []int64    `ch:"events.time_unix_nano"`
	EventsName                 []string   `ch:"events.name"`
	EventsAttributesKey        [][]string `ch:"events.attributes.key"`
	EventsAttributesValue      [][]string `ch:"events.attributes.value"`
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
		// Extract resource attribute keys and values
		resourceKeys := make([]string, len(span.ResourceAttributes))
		resourceValues := make([]string, len(span.ResourceAttributes))
		for i, attr := range span.ResourceAttributes {
			resourceKeys[i] = attr.Key
			resourceValues[i] = attr.Value
		}

		// Extract span attribute keys and values
		spanKeys := make([]string, len(span.SpanAttributes))
		spanValues := make([]string, len(span.SpanAttributes))
		for i, attr := range span.SpanAttributes {
			spanKeys[i] = attr.Key
			spanValues[i] = attr.Value
		}

		// Extract event data
		eventTimes := make([]int64, len(span.Events))
		eventNames := make([]string, len(span.Events))
		eventAttrKeys := make([][]string, len(span.Events))
		eventAttrValues := make([][]string, len(span.Events))

		for i, event := range span.Events {
			eventTimes[i] = event.TimeUnixNano
			eventNames[i] = event.Name

			// Extract event attributes
			keys := make([]string, len(event.Attributes))
			values := make([]string, len(event.Attributes))
			for j, attr := range event.Attributes {
				keys[j] = attr.Key
				values[j] = attr.Value
			}
			eventAttrKeys[i] = keys
			eventAttrValues[i] = values
		}

		row := DenormalizedSpanRow{
			TraceID:                 span.TraceID,
			SpanID:                  span.SpanID,
			ParentSpanID:            span.ParentSpanID,
			Flags:                   span.Flags,
			Name:                    span.Name,
			StartTimeUnixNano:       span.StartTimeUnixNano,
			EndTimeUnixNano:         span.EndTimeUnixNano,
			ScopeID:                 span.ScopeID.String(),
			ScopeName:               span.ScopeName,
			ResourceID:              span.ResourceID.String(),
			ResourceSchemaURL:       span.ResourceSchemaURL,
			ResourceAttributesKey:   resourceKeys,
			ResourceAttributesValue: resourceValues,
			SpanAttributesKey:       spanKeys,
			SpanAttributesValue:     spanValues,
			EventsTimeUnixNano:      eventTimes,
			EventsName:              eventNames,
			EventsAttributesKey:     eventAttrKeys,
			EventsAttributesValue:   eventAttrValues,
		}

		if err := batch.AppendStruct(&row); err != nil {
			return fmt.Errorf("failed to append span: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}
