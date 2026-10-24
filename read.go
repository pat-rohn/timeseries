package timeseries

import (
	"fmt"
	"time"
)

// Point is one stored reading; Value is nil for a reading that was not a
// number.
type Point struct {
	Time  time.Time
	Value *float64
}

// Tags lists the distinct tags of a timeseries table, sorted.
func (dbh *DbHandler) Tags(table string) ([]string, error) {
	if !tableNamePattern.MatchString(table) {
		return nil, fmt.Errorf("invalid table name %q", table)
	}
	rows, err := dbh.DB.Query(`SELECT DISTINCT tag FROM ` + table + ` ORDER BY tag`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tags []string
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, err
		}
		tags = append(tags, tag)
	}
	return tags, rows.Err()
}

// Series returns a tag's readings with from <= time <= to, oldest first.
// SQLite stores the time as text in the format it was sent (RFC3339 from the
// firmware, "YYYY-MM-DD HH:MM:SS.fff" from MQTT), so it compares datetime(time).
func (dbh *DbHandler) Series(table, tag string, from, to time.Time) ([]Point, error) {
	if !tableNamePattern.MatchString(table) {
		return nil, fmt.Errorf("invalid table name %q", table)
	}
	query := `SELECT time, value FROM ` + table + ` WHERE tag = ?
		AND datetime(time) BETWEEN datetime(?) AND datetime(?) ORDER BY datetime(time)`
	args := []any{tag, from.UTC().Format("2006-01-02 15:04:05"), to.UTC().Format("2006-01-02 15:04:05")}
	if dbh.conf.UsePostgres {
		query = `SELECT time, value FROM ` + table + ` WHERE tag = $1 AND time BETWEEN $2 AND $3 ORDER BY time`
		args = []any{tag, from.UTC().Format("2006-01-02 15:04:05.999999"), to.UTC().Format("2006-01-02 15:04:05.999999")}
	}
	rows, err := dbh.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var points []Point
	for rows.Next() {
		var raw any
		var p Point
		if err := rows.Scan(&raw, &p.Value); err != nil {
			return nil, err
		}
		if p.Time, err = parseStoredTime(raw); err != nil {
			return nil, err
		}
		points = append(points, p)
	}
	return points, rows.Err()
}

// parseStoredTime reads the time column, which drivers return as time.Time
// or as the text it was stored with.
func parseStoredTime(raw any) (time.Time, error) {
	switch v := raw.(type) {
	case time.Time:
		return v.UTC(), nil
	case string:
		return parseTimeText(v)
	case []byte:
		return parseTimeText(string(v))
	}
	return time.Time{}, fmt.Errorf("unexpected time value %T", raw)
}

func parseTimeText(s string) (time.Time, error) {
	for _, layout := range timestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unreadable time %q", s)
}
