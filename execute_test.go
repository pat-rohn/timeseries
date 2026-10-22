package timeseries

import "testing"

func TestExecuteReturnsError(t *testing.T) {
	dbh := newTestHandler(t)
	if err := dbh.Execute("INSERT INTO no_such_table (x) VALUES (?)", 1); err == nil {
		t.Fatal("Execute reported success for a failing statement")
	}
}
