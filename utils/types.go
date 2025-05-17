package utils

import "time"

type TimePercentile struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

type DateRange struct {
	Start time.Time
	End   time.Time
}
