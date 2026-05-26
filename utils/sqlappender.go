package utils

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// Variant wraps a value to be written as a DuckDB VARIANT column.
// The inner value must be one of: nil, bool, int32, int64, float64, string.
type Variant struct{ V any }

// SQLAppender buffers rows and flushes them to DuckDB as batched INSERT
// statements. Unlike the duckdb-go binary appender, it supports VARIANT columns.
//
// Flushing is triggered by whichever comes first:
//   - the buffer reaching maxRows
//   - maxWait duration elapsing since the last flush
type SQLAppender struct {
	db    *sql.DB
	table string
	rowC  chan []any
	stopC chan struct{}
	doneC chan error
}

// NewSQLAppender creates an appender and starts its background flush goroutine.
func NewSQLAppender(db *sql.DB, table string, maxRows int, maxWait time.Duration) *SQLAppender {
	if maxRows <= 0 {
		maxRows = 2048
	}
	if maxWait <= 0 {
		maxWait = 5 * time.Second
	}
	a := &SQLAppender{
		db:    db,
		table: table,
		rowC:  make(chan []any, maxRows),
		stopC: make(chan struct{}),
		doneC: make(chan error, 1),
	}
	go a.run(maxRows, maxWait)
	return a
}

// AppendRow queues a row for insertion. Returns an error if the appender is closed.
func (a *SQLAppender) AppendRow(vals ...any) error {
	select {
	case a.rowC <- vals:
		return nil
	case <-a.stopC:
		return errors.New("sqlappender: closed")
	}
}

// Close flushes any buffered rows and shuts down the background goroutine.
// Returns any error that occurred during the final flush.
func (a *SQLAppender) Close() error {
	close(a.stopC)
	return <-a.doneC
}

func (a *SQLAppender) run(maxRows int, maxWait time.Duration) {
	ticker := time.NewTicker(maxWait)
	defer ticker.Stop()

	buf := make([][]any, 0, maxRows)
	var lastErr error

	flush := func() {
		if len(buf) == 0 {
			return
		}
		if err := sqlInsert(a.db, a.table, buf); err != nil {
			lastErr = err
		}
		buf = buf[:0]
	}

	for {
		select {
		case row := <-a.rowC:
			buf = append(buf, row)
			if len(buf) >= maxRows {
				flush()
				ticker.Reset(maxWait)
			}
		case <-ticker.C:
			flush()
		case <-a.stopC:
			// Drain any rows already in the channel before stopping.
			for {
				select {
				case row := <-a.rowC:
					buf = append(buf, row)
				default:
					goto done
				}
			}
		done:
			flush()
			a.doneC <- lastErr
			return
		}
	}
}

func sqlInsert(db *sql.DB, table string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(table)
	sb.WriteString(" VALUES ")
	for i, row := range rows {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('(')
		for j, val := range row {
			if j > 0 {
				sb.WriteString(", ")
			}
			lit, err := toLiteral(val)
			if err != nil {
				return fmt.Errorf("row %d col %d: %w", i, j, err)
			}
			sb.WriteString(lit)
		}
		sb.WriteByte(')')
	}
	_, err := db.ExecContext(context.Background(), sb.String())
	return err
}

func toLiteral(v any) (string, error) {
	switch val := v.(type) {
	case nil:
		return "NULL", nil
	case bool:
		if val {
			return "true", nil
		}
		return "false", nil
	case int:
		return fmt.Sprintf("%d", val), nil
	case int8:
		return fmt.Sprintf("%d", val), nil
	case int16:
		return fmt.Sprintf("%d", val), nil
	case int32:
		return fmt.Sprintf("%d", val), nil
	case int64:
		return fmt.Sprintf("%d", val), nil
	case uint:
		return fmt.Sprintf("%d", val), nil
	case uint32:
		return fmt.Sprintf("%d", val), nil
	case uint64:
		return fmt.Sprintf("%d", val), nil
	case float32:
		return floatLiteral(float64(val)), nil
	case float64:
		return floatLiteral(val), nil

	case string:
		return strLiteral(val), nil
	case []string:
		return strSliceLiteral(val), nil
	case []int64:
		return int64SliceLiteral(val), nil
	case []float64:
		return float64SliceLiteral(val), nil
	case [][]string:
		return strSliceSliceLiteral(val), nil
	case Variant:
		return variantLiteral(val.V)
	case map[string]any:
		return mapVariantLiteral(val)
	default:
		return "", fmt.Errorf("unsupported type %T", v)
	}
}

func strLiteral(s string) string {
	// DuckDB's C parser treats null bytes as string terminators.
	// Strip them to prevent parse errors; null bytes have no meaningful
	// representation in OTLP attribute values anyway.
	s = strings.ReplaceAll(s, "\x00", "")
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func floatLiteral(f float64) string {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "NULL"
	}
	return fmt.Sprintf("%g", f)
}

func strSliceLiteral(ss []string) string {
	parts := make([]string, len(ss))
	for i, s := range ss {
		parts[i] = strLiteral(s)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func int64SliceLiteral(ns []int64) string {
	parts := make([]string, len(ns))
	for i, n := range ns {
		parts[i] = fmt.Sprintf("%d", n)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func float64SliceLiteral(fs []float64) string {
	parts := make([]string, len(fs))
	for i, f := range fs {
		parts[i] = floatLiteral(f)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func strSliceSliceLiteral(ss [][]string) string {
	parts := make([]string, len(ss))
	for i, s := range ss {
		parts[i] = strSliceLiteral(s)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func variantLiteral(v any) (string, error) {
	switch val := v.(type) {
	case nil:
		return "NULL::VARIANT", nil
	case bool:
		if val {
			return "true::VARIANT", nil
		}
		return "false::VARIANT", nil
	case int32:
		return fmt.Sprintf("%d::INTEGER::VARIANT", val), nil
	case int64:
		return fmt.Sprintf("%d::BIGINT::VARIANT", val), nil
	case float64:
		lit := floatLiteral(val)
		if lit == "NULL" {
			return "NULL::VARIANT", nil
		}
		return lit + "::DOUBLE::VARIANT", nil
	case string:
		return strLiteral(val) + "::VARIANT", nil
	default:
		return "", fmt.Errorf("unsupported VARIANT value type %T", v)
	}
}

func mapVariantLiteral(m map[string]any) (string, error) {
	if len(m) == 0 {
		return "MAP {}::MAP(VARCHAR, VARIANT)", nil
	}
	pairs := make([]string, 0, len(m))
	for k, v := range m {
		vlit, err := variantLiteral(v)
		if err != nil {
			return "", fmt.Errorf("map key %q: %w", k, err)
		}
		pairs = append(pairs, strLiteral(k)+": "+vlit)
	}
	return "MAP {" + strings.Join(pairs, ", ") + "}", nil
}
