package timeseries

import (
	"testing"
	"time"
)

func countRows(t *testing.T, dbh *DbHandler, table, tag string) int {
	t.Helper()
	var n int
	if err := dbh.DB.QueryRow(dbh.rebindTest("SELECT COUNT(*) FROM "+table+" WHERE tag = ?"), tag).Scan(&n); err != nil {
		t.Fatalf("count rows in %s: %v", table, err)
	}
	return n
}

func TestInsertTimeseriesStoresMaliciousTagLiterally(t *testing.T) {
	dbh := newTestHandler(t)
	if err := dbh.CreateTimeseriesTable("measurements"); err != nil {
		t.Fatal(err)
	}
	tag := "evil'); DROP TABLE measurements; --"
	is := TimeseriesImportStruct{
		Tag:        tag,
		Timestamps: []string{"2026-10-22T19:22:08.499Z"},
		Values:     []string{"21.5"},
	}
	if err := dbh.InsertTimeseries(is, true, "measurements"); err != nil {
		t.Fatalf("InsertTimeseries: %v", err)
	}
	if got := countRows(t, dbh, "measurements", tag); got != 1 {
		t.Fatalf("rows with malicious tag = %d, want 1", got)
	}
}

func TestInsertTimeseriesRejectsInvalidTimestamp(t *testing.T) {
	dbh := newTestHandler(t)
	if err := dbh.CreateTimeseriesTable("measurements"); err != nil {
		t.Fatal(err)
	}
	for _, ts := range []string{
		"2026-10-22', 'x', 1); DROP TABLE measurements; --",
		"yesterday",
		"",
	} {
		is := TimeseriesImportStruct{Tag: "t", Timestamps: []string{ts}, Values: []string{"1"}}
		if err := dbh.InsertTimeseries(is, true, "measurements"); err == nil {
			t.Errorf("InsertTimeseries accepted timestamp %q", ts)
		}
	}
	if got := countRows(t, dbh, "measurements", "t"); got != 0 {
		t.Fatalf("rows = %d, want 0", got)
	}
}

func TestInsertTimeseriesAcceptsKnownTimestampFormats(t *testing.T) {
	dbh := newTestHandler(t)
	if err := dbh.CreateTimeseriesTable("measurements"); err != nil {
		t.Fatal(err)
	}
	// Distinct instants: on Postgres one instant in two formats is the same
	// (tag, time), and the second would be skipped as a duplicate.
	is := TimeseriesImportStruct{
		Tag: "formats",
		Timestamps: []string{
			"2026-10-22T19:22:08.499Z",       // firmware CTimeHelper::getTimestamp
			"2026-10-22 19:22:09.499",        // go-iotedge UpdateSensorHandler / MQTT buffer
			"2026-10-22 19:22:10",            // SQLite datetime()
			"2026-10-22T21:22:11.1+02:00",    // RFC3339 with offset
			"2026-10-22T19:22:12.123456789Z", // RFC3339Nano
		},
		Values: []string{"1", "2", "3", "4", "5"},
	}
	if err := dbh.InsertTimeseries(is, true, "measurements"); err != nil {
		t.Fatalf("InsertTimeseries: %v", err)
	}
	if got := countRows(t, dbh, "measurements", "formats"); got != 5 {
		t.Fatalf("rows = %d, want 5", got)
	}
}

func TestInsertTimeseriesStoresUnparsableValueAsNull(t *testing.T) {
	dbh := newTestHandler(t)
	if err := dbh.CreateTimeseriesTable("measurements"); err != nil {
		t.Fatal(err)
	}
	is := TimeseriesImportStruct{
		Tag:        "nullable",
		Timestamps: []string{"2026-10-22 19:22:08.000", "2026-10-22 19:22:09.000", "2026-10-22 19:22:10.000"},
		Values:     []string{"1); DROP TABLE measurements; --", " 42.5 ", "0x10"},
	}
	if err := dbh.InsertTimeseries(is, true, "measurements"); err != nil {
		t.Fatalf("InsertTimeseries: %v", err)
	}
	rows, err := dbh.DB.Query(dbh.rebindTest("SELECT value FROM measurements WHERE tag = ? ORDER BY time"), "nullable")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []*float64
	for rows.Next() {
		var v *float64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if len(got) != 3 || got[0] != nil || got[1] == nil || *got[1] != 42.5 || got[2] == nil || *got[2] != 16 {
		t.Fatalf("values = %v, want [nil 42.5 16]", got)
	}
}

func TestInsertTimeseriesRejectsMismatchedLengths(t *testing.T) {
	dbh := newTestHandler(t)
	if err := dbh.CreateTimeseriesTable("measurements"); err != nil {
		t.Fatal(err)
	}
	is := TimeseriesImportStruct{Tag: "t", Timestamps: []string{"2026-10-22 19:22:08"}, Values: nil}
	if err := dbh.InsertTimeseries(is, true, "measurements"); err == nil {
		t.Fatal("InsertTimeseries accepted more timestamps than values")
	}
}

func TestInsertTimeseriesRejectsInvalidTableName(t *testing.T) {
	dbh := newTestHandler(t)
	is := TimeseriesImportStruct{Tag: "t", Timestamps: []string{"2026-10-22 19:22:08"}, Values: []string{"1"}}
	if err := dbh.InsertTimeseries(is, true, "measurements; DROP TABLE x"); err == nil {
		t.Fatal("InsertTimeseries accepted an invalid table name")
	}
}

// T1: large batches used to switch to the hard-coded "measurements" table.
func TestInsertTimeseriesLargeBatchUsesGivenTable(t *testing.T) {
	dbh := newTestHandler(t)
	if err := dbh.CreateTimeseriesTable("custom_ts"); err != nil {
		t.Fatal(err)
	}
	const n = 100_005
	is := TimeseriesImportStruct{Tag: "bulk", Timestamps: make([]string, n), Values: make([]string, n)}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		is.Timestamps[i] = start.Add(time.Duration(i) * time.Second).Format("2006-01-02 15:04:05.000")
		is.Values[i] = "1"
	}
	if err := dbh.InsertTimeseries(is, true, "custom_ts"); err != nil {
		t.Fatalf("InsertTimeseries: %v", err)
	}
	if got := countRows(t, dbh, "custom_ts", "bulk"); got != n {
		t.Fatalf("rows in custom_ts = %d, want %d", got, n)
	}
}
