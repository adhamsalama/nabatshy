package utils

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Int64Slice is a []int64 that can be scanned from DuckDB's []interface{} array.
type Int64Slice []int64

func (s *Int64Slice) Scan(src any) error {
	if src == nil {
		*s = nil
		return nil
	}
	v, ok := src.([]interface{})
	if !ok {
		return fmt.Errorf("Int64Slice: expected []interface{}, got %T", src)
	}
	result := make([]int64, len(v))
	for i, item := range v {
		switch n := item.(type) {
		case int64:
			result[i] = n
		case int32:
			result[i] = int64(n)
		case float64:
			result[i] = int64(n)
		case nil:
		default:
			return fmt.Errorf("Int64Slice: unsupported element type %T", item)
		}
	}
	*s = result
	return nil
}

// StringSlice is a []string that can be scanned from DuckDB's []interface{} array.
type StringSlice []string

func (s *StringSlice) Scan(src any) error {
	if src == nil {
		*s = nil
		return nil
	}
	v, ok := src.([]interface{})
	if !ok {
		return fmt.Errorf("StringSlice: expected []interface{}, got %T", src)
	}
	result := make([]string, len(v))
	for i, item := range v {
		if item == nil {
			result[i] = ""
		} else if str, ok := item.(string); ok {
			result[i] = str
		} else {
			result[i] = fmt.Sprintf("%v", item)
		}
	}
	*s = result
	return nil
}

// StringSliceSlice is a [][]string that can be scanned from DuckDB's []interface{} nested array.
type StringSliceSlice [][]string

func (s *StringSliceSlice) Scan(src any) error {
	if src == nil {
		*s = nil
		return nil
	}
	v, ok := src.([]interface{})
	if !ok {
		return fmt.Errorf("StringSliceSlice: expected []interface{}, got %T", src)
	}
	result := make([][]string, len(v))
	for i, item := range v {
		if item == nil {
			result[i] = nil
			continue
		}
		inner, ok := item.([]interface{})
		if !ok {
			return fmt.Errorf("StringSliceSlice: expected inner []interface{}, got %T", item)
		}
		row := make([]string, len(inner))
		for j, el := range inner {
			if el == nil {
				row[j] = ""
			} else if str, ok := el.(string); ok {
				row[j] = str
			} else {
				row[j] = fmt.Sprintf("%v", el)
			}
		}
		result[i] = row
	}
	*s = result
	return nil
}

type TimePercentile struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

type DateRange struct {
	Start time.Time
	End   time.Time
}

type ResourceAttribute struct {
	Key   string
	Value string
}

type EventAttribute struct {
	Key   string
	Value string
}

type Event struct {
	TimeUnixNano int64
	Name         string
	Attributes   []EventAttribute
}

type Span struct {
	TraceID            string
	SpanID             string
	ParentSpanID       string
	Flags              int32
	Kind               string
	Name               string
	StartTimeUnixNano  int64
	EndTimeUnixNano    int64
	DurationNs         int64
	ScopeID            uuid.UUID
	ScopeName          string
	ResourceID         uuid.UUID
	ResourceSchemaURL  string
	ResourceAttributes []ResourceAttribute
	SpanAttributes     []ResourceAttribute
	Events             []Event
}
