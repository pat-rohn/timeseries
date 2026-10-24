package timeseries

import (
	"strings"
	"testing"
)

// T3: Grafana queries filter by tag and time range; without an index every
// panel refresh scans the whole table.
func TestCreateTimeseriesTableIndexesTagAndTime(t *testing.T) {
	dbh := newTestHandler(t)
	for i := 0; i < 2; i++ { // idempotent
		if err := dbh.CreateTimeseriesTable("measurements"); err != nil {
			t.Fatalf("CreateTimeseriesTable (run %d): %v", i+1, err)
		}
	}
	rows, err := dbh.DB.Query(`EXPLAIN QUERY PLAN SELECT time, value FROM measurements
		WHERE tag = ? AND time BETWEEN ? AND ? ORDER BY time`, "t", "2026-01-01", "2026-02-01")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	joined := strings.Join(plan, "; ")
	if !strings.Contains(joined, "USING INDEX") || strings.Contains(joined, "TEMP B-TREE") {
		t.Fatalf("query plan does not use a (tag, time) index: %s", joined)
	}
}
