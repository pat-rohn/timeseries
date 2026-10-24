package timeseries

import "testing"

// T2: a table from an older release holds duplicate (tag, time) rows, so
// "ON CONFLICT DO NOTHING" never fired. CreateTimeseriesTable removes them
// once (keeping the first row inserted) and adds a unique index.
func TestCreateTimeseriesTableRemovesDuplicatesAndMakesTagTimeUnique(t *testing.T) {
	dbh := newTestHandler(t)
	timestampType := "DATETIME"
	if dbh.conf.UsePostgres {
		timestampType = "TIMESTAMP"
	}
	legacy := []string{
		`CREATE TABLE measurements (time ` + timestampType + `, tag TEXT NOT NULL,
			value DOUBLE PRECISION NULL, comment TEXT DEFAULT '')`,
		`INSERT INTO measurements (time, tag, value) VALUES ('2026-10-24 10:00:00', 'a', 1)`,
		`INSERT INTO measurements (time, tag, value) VALUES ('2026-10-24 10:00:00', 'a', 2)`,
		`INSERT INTO measurements (time, tag, value) VALUES ('2026-10-24 10:00:00', 'a', 3)`,
		`INSERT INTO measurements (time, tag, value) VALUES ('2026-10-24 10:01:00', 'a', 4)`,
		`INSERT INTO measurements (time, tag, value) VALUES ('2026-10-24 10:00:00', 'b', 5)`,
		`INSERT INTO measurements (time, tag, value) VALUES (NULL, 'c', 6)`,
		`INSERT INTO measurements (time, tag, value) VALUES (NULL, 'c', 7)`,
	}
	for _, stmt := range legacy {
		if _, err := dbh.DB.Exec(stmt); err != nil {
			t.Fatalf("legacy data: %v", err)
		}
	}

	for i := 0; i < 2; i++ { // the second run must find nothing to do
		if err := dbh.CreateTimeseriesTable("measurements"); err != nil {
			t.Fatalf("CreateTimeseriesTable (run %d): %v", i+1, err)
		}
	}

	for tag, want := range map[string]int{"a": 2, "b": 1, "c": 2} {
		if got := countRows(t, dbh, "measurements", tag); got != want {
			t.Errorf("rows for %s = %d, want %d", tag, got, want)
		}
	}
	var kept float64
	if err := dbh.DB.QueryRow(dbh.rebindTest(`SELECT value FROM measurements WHERE tag = ? AND time = ?`),
		"a", "2026-10-24 10:00:00").Scan(&kept); err != nil || kept != 1 {
		t.Errorf("kept value for the duplicate = %v (%v), want the first inserted (1)", kept, err)
	}

	again := TimeseriesImportStruct{Tag: "a", Timestamps: []string{"2026-10-24 10:00:00"}, Values: []string{"9"}}
	if err := dbh.InsertTimeseries(again, true, "measurements"); err != nil {
		t.Fatalf("InsertTimeseries of an existing (tag, time): %v", err)
	}
	if got := countRows(t, dbh, "measurements", "a"); got != 2 {
		t.Errorf("rows for a after re-insert = %d, want 2: ON CONFLICT DO NOTHING must skip it", got)
	}
	if _, err := dbh.DB.Exec(`INSERT INTO measurements (time, tag, value) VALUES ('2026-10-24 10:01:00', 'a', 8)`); err == nil {
		t.Error("a duplicate (tag, time) was accepted: no unique index")
	}
}
