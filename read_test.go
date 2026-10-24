package timeseries

import (
	"testing"
	"time"
)

func TestTagsAndSeriesAcrossTimestampFormats(t *testing.T) {
	dbh := newTestHandler(t)
	if err := dbh.CreateTimeseriesTable("measurements"); err != nil {
		t.Fatal(err)
	}
	insert := func(tag string, ts []string, vs []string) {
		t.Helper()
		if err := dbh.InsertTimeseries(TimeseriesImportStruct{Tag: tag, Timestamps: ts, Values: vs}, true, "measurements"); err != nil {
			t.Fatal(err)
		}
	}
	// Firmware sends RFC3339 with Z, the MQTT path "YYYY-MM-DD HH:MM:SS.fff".
	insert("temp", []string{"2026-10-24T10:00:00.000Z", "2026-10-24 11:00:00.000", "2026-10-24T12:00:00.000Z"}, []string{"20.5", "21", "x"})
	insert("co2", []string{"2026-10-24 10:30:00.000"}, []string{"600"})

	tags, err := dbh.Tags("measurements")
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != 2 || tags[0] != "co2" || tags[1] != "temp" {
		t.Fatalf("Tags = %v, want [co2 temp]", tags)
	}

	from := time.Date(2026, 10, 24, 10, 30, 0, 0, time.UTC)
	to := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	points, err := dbh.Series("measurements", "temp", from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 2 {
		t.Fatalf("Series = %d points, want 2 (11:00 and 12:00)", len(points))
	}
	if !points[0].Time.Equal(time.Date(2026, 10, 24, 11, 0, 0, 0, time.UTC)) || points[0].Value == nil || *points[0].Value != 21 {
		t.Errorf("first point = %+v", points[0])
	}
	if !points[1].Time.Equal(to) || points[1].Value != nil {
		t.Errorf("second point = %+v, want 12:00 with a null value", points[1])
	}

	if _, err := dbh.Tags("measurements; DROP TABLE x"); err == nil {
		t.Error("Tags accepted an invalid table name")
	}
}
