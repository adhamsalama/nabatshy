package collector

import (
	"context"
	"database/sql"
	"fmt"

	"nabatshy/utils"
)

type MetricWriter struct {
	ch chan []utils.MetricDataPoint
}

func NewMetricWriter(db *sql.DB) *MetricWriter {
	w := &MetricWriter{ch: make(chan []utils.MetricDataPoint, 100)}
	go func() {
		for points := range w.ch {
			if err := utils.InsertMetricDataPoints(db, context.Background(), points); err != nil {
				fmt.Printf("metric insert error: %v\n", err)
			}
		}
	}()
	return w
}

func (w *MetricWriter) Write(points []utils.MetricDataPoint) {
	w.ch <- points
}
