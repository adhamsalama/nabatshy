package utils

import (
	"time"

	"github.com/google/uuid"
)

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

type Event struct {
	TimeUnixNano int64
	Name         string
}

type Span struct {
	TraceID            string
	SpanID             string
	ParentSpanID       string
	Flags              int32
	Name               string
	StartTimeUnixNano  int64
	EndTimeUnixNano    int64
	DurationNs         int64
	ScopeID            uuid.UUID
	ScopeName          string
	ResourceID         uuid.UUID
	ResourceSchemaURL  string
	ResourceAttributes []ResourceAttribute
	Events             []Event
}
