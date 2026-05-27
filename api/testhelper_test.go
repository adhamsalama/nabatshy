package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	_ "github.com/duckdb/duckdb-go/v2"
	nabdb "nabatshy/db"
)

const (
	testTraceID1 = "trace-aaaa-0001"
	testTraceID2 = "trace-bbbb-0002"
	testSpanID1  = "span-1111-root"
	testSpanID2  = "span-2222-child"
	testSpanID3  = "span-3333-root"
)

// testNow is a fixed reference point so all fixture timestamps are consistent.
var testNow = time.Now().UTC()

// setupTestDB initialises an in-memory DuckDB, seeds all fixture data, and
// registers a cleanup that closes the DB when the test finishes.
func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db := nabdb.InitDuckDB(true)
	t.Cleanup(func() { db.Close() })
	seedSpans(t, db)
	seedLogs(t, db)
	seedMetrics(t, db)
	return db
}

// newTestRouter builds the full chi router wired to the given DB.
func newTestRouter(db *sql.DB) http.Handler {
	dialect := goqu.Dialect("default")
	svc := TelemetryService{Ch: db, DB: &dialect}
	ctrl := TelemetryController{service: svc}
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	ctrl.RegisterRoutes(r)
	NewCronController(db).RegisterRoutes(r)
	return r
}

// get fires a GET request against the router and returns the recorder.
func get(t *testing.T, r http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

// assertOK fails the test if the response is not HTTP 200.
func assertOK(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d — body: %s", w.Code, w.Body.String())
	}
}

// mustDecode decodes the recorder body into T, failing the test on error.
func mustDecode[T any](t *testing.T, w *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.NewDecoder(w.Body).Decode(&v); err != nil {
		t.Fatalf("decode: %v — body: %s", err, w.Body.String())
	}
	return v
}

// timeParam formats a time.Time as an RFC3339 query-param value.
func timeParam(t time.Time) string {
	return t.Format(time.RFC3339)
}

// rangeParams returns ?start=...&end=... covering a window around testNow.
func rangeParams(before, after time.Duration) string {
	return fmt.Sprintf("?start=%s&end=%s",
		timeParam(testNow.Add(-before)),
		timeParam(testNow.Add(after)),
	)
}

// ── fixture helpers ───────────────────────────────────────────────────────────

func spanMapLiteral(m map[string]string) string {
	if len(m) == 0 {
		return "MAP {}::MAP(VARCHAR, VARIANT)"
	}
	parts := make([]string, 0, len(m))
	for k, v := range m {
		parts = append(parts, "'"+strings.ReplaceAll(k, "'", "''")+"': '"+strings.ReplaceAll(v, "'", "''")+"'::VARIANT")
	}
	return "MAP {" + strings.Join(parts, ", ") + "}"
}

func strArr(ss []string) string {
	if len(ss) == 0 {
		return "[]::VARCHAR[]"
	}
	parts := make([]string, len(ss))
	for i, s := range ss {
		parts[i] = "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func int64Arr(ns []int64) string {
	if len(ns) == 0 {
		return "[]::BIGINT[]"
	}
	parts := make([]string, len(ns))
	for i, n := range ns {
		parts[i] = fmt.Sprintf("%d", n)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func mapArrLiteral(ms []map[string]string) string {
	if len(ms) == 0 {
		return "[]::MAP(VARCHAR, VARIANT)[]"
	}
	parts := make([]string, len(ms))
	for i, m := range ms {
		parts[i] = spanMapLiteral(m)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func seedSpans(t *testing.T, db *sql.DB) {
	t.Helper()

	now := testNow

	type span struct {
		traceID, spanID, parentSpanID string
		kind, name                    string
		start, end                    time.Time
		scopeName                     string
		resAttrs                      map[string]string
		spanAttrs                     map[string]string
		evtTimes                      []int64
		evtNames                      []string
		evtAttrs                      []map[string]string
	}

	spans := []span{
		{
			// S1: root span, trace T1, frontend
			traceID: testTraceID1, spanID: testSpanID1, parentSpanID: "",
			kind: "SPAN_KIND_SERVER", name: "GET /users",
			start: now.Add(-10 * time.Minute), end: now.Add(-9 * time.Minute),
			scopeName: "frontend",
			resAttrs:  map[string]string{"service.name": "frontend"},
			spanAttrs: map[string]string{"http.method": "GET", "http.status_code": "200"},
			evtTimes:  []int64{now.Add(-9*time.Minute + 30*time.Second).UnixNano()},
			evtNames:  []string{"log"},
			evtAttrs:  []map[string]string{{"level": "info"}},
		},
		{
			// S2: child span, trace T1, db
			traceID: testTraceID1, spanID: testSpanID2, parentSpanID: testSpanID1,
			kind: "SPAN_KIND_CLIENT", name: "db.query",
			start: now.Add(-10 * time.Minute), end: now.Add(-9*time.Minute + 30*time.Second),
			scopeName: "db",
			resAttrs:  map[string]string{"service.name": "db"},
			spanAttrs: map[string]string{"db.statement": "SELECT * FROM users"},
		},
		{
			// S3: root span, trace T2, frontend (slower — 2 minutes)
			traceID: testTraceID2, spanID: testSpanID3, parentSpanID: "",
			kind: "SPAN_KIND_SERVER", name: "POST /orders",
			start: now.Add(-5 * time.Minute), end: now.Add(-3 * time.Minute),
			scopeName: "frontend",
			resAttrs:  map[string]string{"service.name": "frontend"},
			spanAttrs: map[string]string{"http.method": "POST", "http.status_code": "201"},
			evtTimes:  []int64{now.Add(-4 * time.Minute).UnixNano()},
			evtNames:  []string{"exception"},
			evtAttrs:  []map[string]string{{"exception.type": "ValueError"}},
		},
	}

	for _, s := range spans {
		startNs := s.start.UnixNano()
		endNs := s.end.UnixNano()
		query := fmt.Sprintf(`INSERT INTO denormalized_span VALUES (
			'%s', '%s', '%s', 0, '%s', '%s',
			%d, %d, %d,
			'scope-%s', '%s', 'res-%s', '',
			%s,
			%s,
			%s, %s, %s
		)`,
			s.traceID, s.spanID, s.parentSpanID,
			s.kind, s.name,
			startNs, endNs, endNs-startNs,
			s.scopeName, s.scopeName, s.scopeName,
			spanMapLiteral(s.resAttrs),
			spanMapLiteral(s.spanAttrs),
			int64Arr(s.evtTimes), strArr(s.evtNames),
			mapArrLiteral(s.evtAttrs),
		)
		if _, err := db.Exec(query); err != nil {
			t.Fatalf("seed span %s: %v", s.spanID, err)
		}
	}
}

func seedLogs(t *testing.T, db *sql.DB) {
	t.Helper()
	now := testNow

	logs := []struct {
		ts       int64
		severity string
		sevNum   int
		body     string
		traceID  string
		spanID   string
		service  string
	}{
		{now.Add(-8 * time.Minute).UnixNano(), "INFO", 9, "user login successful", testTraceID1, testSpanID1, "frontend"},
		{now.Add(-6 * time.Minute).UnixNano(), "ERROR", 17, "database connection failed", testTraceID1, testSpanID2, "db"},
		{now.Add(-4 * time.Minute).UnixNano(), "WARN", 13, "slow query detected", testTraceID2, testSpanID3, "frontend"},
	}

	for _, l := range logs {
		query := fmt.Sprintf(`INSERT INTO log_record VALUES (
			%d, %d, '%s', %d, '%s',
			'%s', '%s', '%s',
			%s,
			%s,
			'%s'
		)`,
			l.ts, l.ts, l.severity, l.sevNum, l.body,
			l.traceID, l.spanID, l.service,
			spanMapLiteral(map[string]string{"app": "nabatshy"}),
			spanMapLiteral(map[string]string{"service.name": l.service}),
			l.service,
		)
		if _, err := db.Exec(query); err != nil {
			t.Fatalf("seed log: %v", err)
		}
	}

	// Rebuild FTS index after seeding logs (same as logs_writer.go does).
	db.Exec("PRAGMA drop_fts_index(log_record)")
	db.Exec("PRAGMA create_fts_index('log_record', 'rowid', 'body', overwrite=1)")
}

func seedMetrics(t *testing.T, db *sql.DB) {
	t.Helper()
	now := testNow

	metrics := []struct {
		name, mtype, unit string
		ts                int64
		valDouble         float64
		valInt            int64
	}{
		{"http.request.duration", "gauge", "ms", now.Add(-9 * time.Minute).UnixNano(), 123.4, 0},
		{"http.request.duration", "gauge", "ms", now.Add(-7 * time.Minute).UnixNano(), 98.7, 0},
		{"http.request.count", "sum", "1", now.Add(-8 * time.Minute).UnixNano(), 0, 42},
	}

	for _, m := range metrics {
		query := fmt.Sprintf(`INSERT INTO metric_data_point VALUES (
			'%s', '', '%s', '%s',
			%d, %d,
			%f, %d,
			'AGGREGATION_TEMPORALITY_CUMULATIVE', false,
			0, 0, 0, 0,
			[]::BIGINT[], []::DOUBLE[],
			%s,
			%s,
			'frontend'
		)`,
			m.name, m.unit, m.mtype,
			m.ts, m.ts,
			m.valDouble, m.valInt,
			spanMapLiteral(map[string]string{"service": "frontend"}),
			spanMapLiteral(map[string]string{"service.name": "frontend"}),
		)
		if _, err := db.Exec(query); err != nil {
			t.Fatalf("seed metric: %v", err)
		}
	}
}

