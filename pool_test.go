package timeseries

import "testing"

// Postgres refuses connections beyond max_connections (100 by default); an
// unbounded pool hit that under MQTT load ("sorry, too many clients already").
func TestOpenDatabaseBoundsTheConnectionPool(t *testing.T) {
	dbh := newTestHandler(t)
	got := dbh.DB.Stats().MaxOpenConnections
	if got <= 0 || got > maxOpenConns {
		t.Fatalf("MaxOpenConnections = %d, want 1..%d", got, maxOpenConns)
	}
}
