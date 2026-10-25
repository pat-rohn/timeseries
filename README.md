# timeseries

Go library that stores timeseries readings in SQLite (pure Go, modernc) or
PostgreSQL. Used by [go-iotedge](https://github.com/pat-rohn/go-iotedge).

## Usage

```go
db, err := timeseries.Open(timeseries.DBConfig{Name: "iot.db", IPOrPath: "./"})
// Postgres: DBConfig{UsePostgres: true, IPOrPath: host, Port: 5432, Name, User, Password}
defer db.Close()

db.CreateTimeseriesTable("measurements")
db.InsertTimeseries(timeseries.TimeseriesImportStruct{
    Tag:        "livingTemperature",
    Timestamps: []string{"2026-10-24T10:00:00.000Z"},
    Values:     []string{"21.5"},
}, true, "measurements") // true: skip a (tag, time) that is already stored

tags, _ := db.Tags("measurements")
points, _ := db.Series("measurements", "livingTemperature", from, to)
```

- Table: `time`, `tag`, `value` (NULL if not a number), `comment`, with a
  unique index on `(tag, time)`.
- **On first start `CreateTimeseriesTable` deletes duplicate `(tag, time)`
  rows** (keeps the first) before adding that index. Back up older databases.
- Timestamps: RFC3339 or `YYYY-MM-DD HH:MM:SS[.fff]`, in UTC.
- `DBHandler(conf)` is the older process-wide singleton; prefer `Open`.
- The connection pool is capped at 20 connections.

Tests: `go test ./...`; against an embedded Postgres:
`TIMESERIES_TEST_POSTGRES=1 go test ./...` (downloads Postgres once).

## PostgreSQL setup

```sql
-- sudo -u postgres psql
CREATE USER grafanawriteuser WITH PASSWORD '<write password>';
CREATE DATABASE plottydb OWNER grafanawriteuser;

CREATE USER grafanaread WITH PASSWORD '<read password>';
GRANT CONNECT ON DATABASE plottydb TO grafanaread;
\c plottydb
GRANT USAGE ON SCHEMA public TO grafanaread;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafanaread;
ALTER DEFAULT PRIVILEGES FOR ROLE grafanawriteuser IN SCHEMA public GRANT SELECT ON TABLES TO grafanaread;

ALTER DATABASE plottydb SET statement_timeout = '60s';
```

To accept connections from other hosts, set `listen_addresses = '*'` in
`postgresql.conf` and allow only your network in `pg_hba.conf`:

```
host  plottydb  all  192.168.1.0/24  scram-sha-256
```

Then `sudo systemctl restart postgresql`.

For TimescaleDB (hypertable, compression, hourly aggregates) see
[go-iotedge/doc/TIMESCALEDB.md](https://github.com/pat-rohn/go-iotedge/blob/main/doc/TIMESCALEDB.md).
Let the library create the table first.

## Maintenance

```sh
# backup / restore
pg_dump -h 127.0.0.1 -U grafanawriteuser -d plottydb -Fc -f plottydb.dump
pg_restore -h 127.0.0.1 -U grafanawriteuser -d plottydb plottydb.dump
```

```sql
-- latest readings
SELECT * FROM measurements ORDER BY time DESC LIMIT 20;

-- correct a time shift
UPDATE measurements SET time = time - INTERVAL '2 hour' WHERE time > '2020-10-24 20:00:00';
```

Major Postgres upgrade: `pg_dumpall > all.sql` on the old version, install the
new one, restore with `psql -f all.sql postgres` (or use `pg_upgradecluster`
on Debian).
