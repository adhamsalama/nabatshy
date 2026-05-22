package utils

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

func PadQueryResult(rows *sql.Rows, intervalSQL string, dateRange DateRange) ([]TimePercentile, error) {
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
	return fmt.Sprintf("%d seconds", secs)
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
		return DateRange{Start: end, End: end}
	}

	unit := timeRange[len(timeRange)-1:]
	valueStr := timeRange[:len(timeRange)-1]
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return DateRange{Start: end, End: end}
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
		return DateRange{Start: end, End: end}
	}

	start := end.Add(-duration)
	dateRange := DateRange{Start: start, End: end}

	fmt.Printf("dateRange: %v\n", dateRange)
	return dateRange
}

func InsertDenormalizedSpans(db *sql.DB, ctx context.Context, spans []Span) error {
	if len(spans) == 0 {
		return nil
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("getting conn: %w", err)
	}
	defer conn.Close()

	return conn.Raw(func(c any) error {
		driverConn, ok := c.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected connection type %T", c)
		}

		appender, err := duckdb.NewAppenderFromConn(driverConn, "", "denormalized_span")
		if err != nil {
			return fmt.Errorf("creating appender: %w", err)
		}

		for _, span := range spans {
			resourceKeys := make([]string, len(span.ResourceAttributes))
			resourceValues := make([]string, len(span.ResourceAttributes))
			for i, attr := range span.ResourceAttributes {
				resourceKeys[i] = attr.Key
				resourceValues[i] = attr.Value
			}

			spanKeys := make([]string, len(span.SpanAttributes))
			spanValues := make([]string, len(span.SpanAttributes))
			for i, attr := range span.SpanAttributes {
				spanKeys[i] = attr.Key
				spanValues[i] = attr.Value
			}

			eventTimes := make([]int64, len(span.Events))
			eventNames := make([]string, len(span.Events))
			eventAttrKeys := make([][]string, len(span.Events))
			eventAttrValues := make([][]string, len(span.Events))

			for i, event := range span.Events {
				eventTimes[i] = event.TimeUnixNano
				eventNames[i] = event.Name

				keys := make([]string, len(event.Attributes))
				values := make([]string, len(event.Attributes))
				for j, attr := range event.Attributes {
					keys[j] = attr.Key
					values[j] = attr.Value
				}
				eventAttrKeys[i] = keys
				eventAttrValues[i] = values
			}

			if err := appender.AppendRow(
				span.TraceID, span.SpanID, span.ParentSpanID, span.Flags, span.Name,
				span.StartTimeUnixNano, span.EndTimeUnixNano,
				span.EndTimeUnixNano-span.StartTimeUnixNano,
				span.ScopeID.String(), span.ScopeName, span.ResourceID.String(), span.ResourceSchemaURL,
				resourceKeys, resourceValues,
				spanKeys, spanValues,
				eventTimes, eventNames, eventAttrKeys, eventAttrValues,
			); err != nil {
				appender.Close()
				return fmt.Errorf("appending row: %w", err)
			}
		}
		return appender.Close()
	})
}
