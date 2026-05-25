package collector

import (
	"context"
	"database/sql"
	"fmt"

	"nabatshy/utils"
)

type LogWriter struct {
	ch chan []utils.LogRecord
}

func NewLogWriter(db *sql.DB) *LogWriter {
	w := &LogWriter{ch: make(chan []utils.LogRecord, 100)}
	go func() {
		for records := range w.ch {
			if err := utils.InsertLogRecords(db, context.Background(), records); err != nil {
				fmt.Printf("log insert error: %v\n", err)
			}
		}
	}()
	return w
}

func (w *LogWriter) Write(records []utils.LogRecord) {
	w.ch <- records
}
