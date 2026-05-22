package collector

import (
	"context"
	"database/sql"
	"fmt"

	"nabatshy/utils"
)

// SpanWriter serializes all DuckDB writes through a single goroutine,
// avoiding concurrent-write conflicts.
type SpanWriter struct {
	ch chan []utils.Span
}

func NewSpanWriter(db *sql.DB) *SpanWriter {
	w := &SpanWriter{ch: make(chan []utils.Span, 100)}
	go func() {
		for spans := range w.ch {
			if err := utils.InsertDenormalizedSpans(db, context.Background(), spans); err != nil {
				fmt.Printf("span insert error: %v\n", err)
			}
		}
	}()
	return w
}

func (w *SpanWriter) Write(spans []utils.Span) {
	w.ch <- spans
}
