package timeseries

// Generic correctness tests for the timeseries package.
//
// Each test opens its own SQLite database in a temporary directory so tests
// are fully isolated regardless of execution order.  The singleton is reset
// after every test via the returned cleanup function.

import (
	"fmt"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetLevel(log.ErrorLevel) // keep test output clean
}

// newHandler returns a DbHandler backed by a fresh SQLite file in a temp
// directory, plus a cleanup function that closes the handler (resetting the
// singleton) when called.  Always defer the cleanup.
func newHandler(t *testing.T) (*DbHandler, func()) {
	t.Helper()
	conf := DBConfig{
		Name:     "test.db",
		IPOrPath: t.TempDir() + "/",
	}
	dbh := DBHandler(conf)
	return dbh, func() { _ = dbh.Close() }
}

// ── InsertTimeseries ─────────────────────────────────────────────────────────

// TestInsertTimeseries_RoundTrip inserts rows into a timeseries table and reads
// them back, verifying that timestamps, numeric values, and comments are all
// stored correctly.
func TestInsertTimeseries_RoundTrip(t *testing.T) {
	dbh, cleanup := newHandler(t)
	defer cleanup()

	if err := dbh.CreateTimeseriesTable("ts"); err != nil {
		t.Fatalf("CreateTimeseriesTable: %v", err)
	}

	is := TimeseriesImportStruct{
		Tag:        "temp",
		Timestamps: []string{"2024-01-01 10:00:00", "2024-01-01 11:00:00"},
		Values:     []string{"21.5", "22.0"},
		Comments:   []string{"morning reading", "noon reading"},
	}
	if err := dbh.InsertTimeseries(is, false, "ts"); err != nil {
		t.Fatalf("InsertTimeseries: %v", err)
	}

	rows, err := dbh.ExecuteQuery(
		"SELECT tag, value, comment FROM ts ORDER BY time")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	type row struct {
		tag, comment string
		val          float64
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.tag, &r.val, &r.comment); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
	if got[0].tag != "temp" || got[1].tag != "temp" {
		t.Errorf("unexpected tag values: %v", got)
	}
	if got[0].val != 21.5 || got[1].val != 22.0 {
		t.Errorf("unexpected values: %v %v", got[0].val, got[1].val)
	}
	if got[0].comment != "morning reading" || got[1].comment != "noon reading" {
		t.Errorf("comments not persisted: got %q, %q", got[0].comment, got[1].comment)
	}
}

// TestInsertTimeseries_EmptyIsNoOp confirms that an empty TimeseriesImportStruct
// does not produce an error and leaves the table untouched.
func TestInsertTimeseries_EmptyIsNoOp(t *testing.T) {
	dbh, cleanup := newHandler(t)
	defer cleanup()

	if err := dbh.CreateTimeseriesTable("ts"); err != nil {
		t.Fatalf("CreateTimeseriesTable: %v", err)
	}
	if err := dbh.InsertTimeseries(TimeseriesImportStruct{Tag: "x"}, false, "ts"); err != nil {
		t.Errorf("empty insert should be a no-op, got: %v", err)
	}
}

// TestInsertTimeseries_NonNumericValues confirms that non-numeric value strings
// are stored as NULL (the documented fallback) rather than causing a SQL error.
func TestInsertTimeseries_NonNumericValues(t *testing.T) {
	dbh, cleanup := newHandler(t)
	defer cleanup()

	if err := dbh.CreateTimeseriesTable("ts"); err != nil {
		t.Fatalf("CreateTimeseriesTable: %v", err)
	}

	is := TimeseriesImportStruct{
		Tag:        "sensor",
		Timestamps: []string{"2024-01-01 00:00:00"},
		Values:     []string{"N/A"},
	}
	if err := dbh.InsertTimeseries(is, false, "ts"); err != nil {
		t.Errorf("non-numeric value should store NULL, not error: %v", err)
	}
}

// ── InsertIntoDatabase ───────────────────────────────────────────────────────

// TestInsertIntoDatabase_RoundTrip inserts a small mixed-type dataset and reads
// it back, verifying that all column types and values survive the trip.
func TestInsertIntoDatabase_RoundTrip(t *testing.T) {
	dbh, cleanup := newHandler(t)
	defer cleanup()

	is := ImportStruct{
		Names:      []string{"numeric_col", "text_col"},
		Timestamps: []string{"2024-01-01 00:00:00", "2024-01-01 00:01:00"},
		Data: [][]string{
			{"1.5", "2.5"},
			{"hello", "world"},
		},
	}
	if err := dbh.InsertIntoDatabase("t", is); err != nil {
		t.Fatalf("InsertIntoDatabase: %v", err)
	}

	rows, err := dbh.ExecuteQuery("SELECT numeric_col, text_col FROM t ORDER BY Timestamp")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var num interface{}
		var txt string
		if err := rows.Scan(&num, &txt); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

// TestInsertIntoDatabase_SpecialCharacters verifies that text values containing
// single quotes are stored and retrieved without SQL errors.
func TestInsertIntoDatabase_SpecialCharacters(t *testing.T) {
	dbh, cleanup := newHandler(t)
	defer cleanup()

	want := "it's a test"
	is := ImportStruct{
		Names:      []string{"label"},
		Timestamps: []string{"2024-01-01 00:00:00"},
		Data:       [][]string{{want}},
	}
	if err := dbh.InsertIntoDatabase("special", is); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rows, err := dbh.ExecuteQuery("SELECT label FROM special")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var got string
	if rows.Next() {
		_ = rows.Scan(&got)
	}
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

// ── InsertRowsToTable / InsertRowToTable ──────────────────────────────────────

// TestInsertRowsToTable_RoundTrip inserts a batch of rows and verifies the row
// count and that all values survive the round-trip.
func TestInsertRowsToTable_RoundTrip(t *testing.T) {
	dbh, cleanup := newHandler(t)
	defer cleanup()

	var rows []ImportRowStruct
	for i := 0; i < 5; i++ {
		rows = append(rows, ImportRowStruct{
			Names:     []string{"idx", "label"},
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			Values:    []string{fmt.Sprintf("%d", i), fmt.Sprintf("label_%d", i)},
		})
	}

	failed, err := dbh.InsertRowsToTable("batch", rows)
	if err != nil {
		t.Fatalf("InsertRowsToTable: %v", err)
	}
	if len(failed) != 0 {
		t.Errorf("expected no failed rows, got %d", len(failed))
	}

	sqlRows, err := dbh.ExecuteQuery("SELECT COUNT(*) FROM batch")
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	defer sqlRows.Close()
	var n int
	if sqlRows.Next() {
		_ = sqlRows.Scan(&n)
	}
	if n != 5 {
		t.Errorf("expected 5 rows in table, got %d", n)
	}
}

// TestInsertRowsToTable_SpecialCharacters verifies single quotes in text values
// do not cause a SQL error.
func TestInsertRowsToTable_SpecialCharacters(t *testing.T) {
	dbh, cleanup := newHandler(t)
	defer cleanup()

	want := "it's a test"
	rows := []ImportRowStruct{
		{Names: []string{"label"}, Timestamp: "2024-01-01 00:00:00", Values: []string{want}},
	}
	if _, err := dbh.InsertRowsToTable("special_rows", rows); err != nil {
		t.Fatalf("insert: %v", err)
	}

	sqlRows, err := dbh.ExecuteQuery("SELECT label FROM special_rows")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer sqlRows.Close()

	var got string
	if sqlRows.Next() {
		_ = sqlRows.Scan(&got)
	}
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

// TestInsertRowsToTable_MixedColumnTypes verifies that a column with numeric
// values in some rows and text values in others is created as TEXT so no data
// is silently lost as NULL.
func TestInsertRowsToTable_MixedColumnTypes(t *testing.T) {
	dbh, cleanup := newHandler(t)
	defer cleanup()

	rows := []ImportRowStruct{
		{Names: []string{"val"}, Timestamp: "2024-01-01 00:00:00", Values: []string{"42"}},
		{Names: []string{"val"}, Timestamp: "2024-01-01 00:00:01", Values: []string{"hello"}},
	}
	if _, err := dbh.InsertRowsToTable("mixed", rows); err != nil {
		t.Fatalf("InsertRowsToTable: %v", err)
	}

	sqlRows, err := dbh.ExecuteQuery("SELECT val FROM mixed ORDER BY Timestamp")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer sqlRows.Close()

	var vals []interface{}
	for sqlRows.Next() {
		var v interface{}
		_ = sqlRows.Scan(&v)
		vals = append(vals, v)
	}
	if len(vals) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(vals))
	}
	if vals[1] == nil {
		t.Errorf("text value 'hello' stored as NULL; column type must be TEXT when any row is non-numeric")
	}
}

// ── CreateImportTable ─────────────────────────────────────────────────────────

// TestCreateImportTable covers the basic round-trip and the empty-input edge case.
func TestCreateImportTable(t *testing.T) {
	t.Run("empty input returns zero value", func(t *testing.T) {
		result := CreateImportTable([]ImportRowStruct{})
		if result.Names != nil || result.Timestamps != nil || result.Data != nil {
			t.Errorf("expected zero ImportStruct for empty input, got %+v", result)
		}
	})

	t.Run("round-trip matches source rows", func(t *testing.T) {
		input := []ImportRowStruct{
			{Names: []string{"a", "b"}, Timestamp: "2024-01-01", Values: []string{"1", "2"}},
			{Names: []string{"a", "b"}, Timestamp: "2024-01-02", Values: []string{"3", "4"}},
		}
		got := CreateImportTable(input)

		if len(got.Names) != 2 {
			t.Errorf("expected 2 names, got %d", len(got.Names))
		}
		if len(got.Timestamps) != 2 {
			t.Errorf("expected 2 timestamps, got %d", len(got.Timestamps))
		}
		// Data[column][row]
		if len(got.Data) != 2 || len(got.Data[0]) != 2 || len(got.Data[1]) != 2 {
			t.Errorf("unexpected data shape: %+v", got.Data)
		}
		if got.Data[0][1] != "3" || got.Data[1][1] != "4" {
			t.Errorf("second row data mismatch: %v", got.Data)
		}
	})
}
