package timeseries

import (
	"path/filepath"
	"testing"
)

// Open hands out independent handlers: closing one leaves the others (and
// the DBHandler singleton) alone.
func TestOpenReturnsIndependentHandlers(t *testing.T) {
	conf := DBConfig{Name: "open.db", IPOrPath: filepath.Join(t.TempDir()) + "/"}
	first, err := Open(conf)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	second, err := Open(conf)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer second.Close()
	if first == second {
		t.Fatal("Open returned the same handler twice")
	}
	if err := first.CreateTimeseriesTable("measurements"); err != nil {
		t.Fatal(err)
	}
	first.Close()
	if err := second.DB.Ping(); err != nil {
		t.Fatalf("closing one handler closed the other: %v", err)
	}
	is := TimeseriesImportStruct{Tag: "t", Timestamps: []string{"2026-10-24 12:00:00"}, Values: []string{"1"}}
	if err := second.InsertTimeseries(is, true, "measurements"); err != nil {
		t.Fatalf("second handler cannot write: %v", err)
	}
}
