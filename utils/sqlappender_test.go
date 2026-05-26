package utils

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func countRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}

func TestSQLAppender_FlushOnRowCount(t *testing.T) {
	db := newTestDB(t)
	db.Exec("CREATE TABLE t (id INTEGER)")

	a := NewSQLAppender(db, "t", 3, 10*time.Second)
	a.AppendRow(int32(1))
	a.AppendRow(int32(2))
	// Not yet flushed — below maxRows.
	time.Sleep(50 * time.Millisecond)
	if n := countRows(t, db, "t"); n != 0 {
		t.Fatalf("expected 0 rows before flush, got %d", n)
	}
	a.AppendRow(int32(3)) // triggers flush at maxRows=3
	time.Sleep(50 * time.Millisecond)
	if n := countRows(t, db, "t"); n != 3 {
		t.Fatalf("expected 3 rows after row-count flush, got %d", n)
	}
	a.Close()
}

func TestSQLAppender_FlushOnTimer(t *testing.T) {
	db := newTestDB(t)
	db.Exec("CREATE TABLE t (id INTEGER)")

	a := NewSQLAppender(db, "t", 100, 100*time.Millisecond)
	a.AppendRow(int32(1))
	a.AppendRow(int32(2))

	time.Sleep(200 * time.Millisecond)
	if n := countRows(t, db, "t"); n != 2 {
		t.Fatalf("expected 2 rows after timer flush, got %d", n)
	}
	a.Close()
}

func TestSQLAppender_CloseFlushesRemaining(t *testing.T) {
	db := newTestDB(t)
	db.Exec("CREATE TABLE t (id INTEGER)")

	a := NewSQLAppender(db, "t", 100, 10*time.Second)
	a.AppendRow(int32(42))
	if err := a.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if n := countRows(t, db, "t"); n != 1 {
		t.Fatalf("expected 1 row after close, got %d", n)
	}
}

func TestSQLAppender_SQLInjection(t *testing.T) {
	db := newTestDB(t)
	db.Exec("CREATE TABLE t (v VARCHAR)")

	a := NewSQLAppender(db, "t", 1, time.Second)
	a.AppendRow("it's a test'); DROP TABLE t; --")
	if err := a.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var got string
	db.QueryRow("SELECT v FROM t").Scan(&got)
	if got != "it's a test'); DROP TABLE t; --" {
		t.Fatalf("unexpected value: %q", got)
	}
	if n := countRows(t, db, "t"); n != 1 {
		t.Fatalf("table was dropped or row missing, count=%d", n)
	}
}

func TestSQLAppender_AllTypes(t *testing.T) {
	db := newTestDB(t)
	db.Exec(`CREATE TABLE t (
		b   BOOLEAN,
		i32 INTEGER,
		i64 BIGINT,
		f64 DOUBLE,
		s   VARCHAR,
		ss  VARCHAR[],
		ns  BIGINT[],
		sss VARCHAR[][]
	)`)

	a := NewSQLAppender(db, "t", 1, time.Second)
	err := a.AppendRow(
		true,
		int32(7),
		int64(1234567890),
		float64(3.14),
		"hello",
		[]string{"a", "b"},
		[]int64{10, 20},
		[][]string{{"x", "y"}, {"z"}},
	)
	if err != nil {
		t.Fatalf("AppendRow: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if n := countRows(t, db, "t"); n != 1 {
		t.Fatalf("expected 1 row, got %d", n)
	}
}

func TestSQLAppender_Variant(t *testing.T) {
	db := newTestDB(t)
	db.Exec("CREATE TABLE t (v VARIANT)")

	cases := []struct {
		v    Variant
		want string
	}{
		{Variant{int32(200)}, "INT32"},
		{Variant{int64(999)}, "INT64"},
		{Variant{true}, "BOOL_TRUE"},
		{Variant{"hello"}, "VARCHAR"},
		{Variant{float64(3.14)}, "DOUBLE"},
	}

	a := NewSQLAppender(db, "t", 10, time.Second)
	for _, c := range cases {
		if err := a.AppendRow(c.v); err != nil {
			t.Fatalf("AppendRow(%v): %v", c.v, err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	rows, _ := db.Query("SELECT variant_typeof(v) FROM t")
	defer rows.Close()
	i := 0
	for rows.Next() {
		var typ string
		rows.Scan(&typ)
		if typ != cases[i].want {
			t.Errorf("row %d: got type %q, want %q", i, typ, cases[i].want)
		}
		i++
	}
	if i != len(cases) {
		t.Fatalf("expected %d rows, got %d", len(cases), i)
	}
}

func TestSQLAppender_MapVariant(t *testing.T) {
	db := newTestDB(t)
	db.Exec("CREATE TABLE t (attrs MAP(VARCHAR, VARIANT))")

	a := NewSQLAppender(db, "t", 1, time.Second)
	a.AppendRow(map[string]any{
		"service.name":     "frontend",
		"http.status_code": int32(200),
		"error":            false,
	})
	if err := a.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var service string
	var status int32
	db.QueryRow(`
		SELECT attrs['service.name']::VARCHAR, attrs['http.status_code']::INTEGER
		FROM t
	`).Scan(&service, &status)

	if service != "frontend" {
		t.Errorf("service: got %q, want %q", service, "frontend")
	}
	if status != 200 {
		t.Errorf("status: got %d, want 200", status)
	}
}
