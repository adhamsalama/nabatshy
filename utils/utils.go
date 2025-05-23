package utils

import (
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
