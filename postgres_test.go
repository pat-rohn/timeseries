package timeseries

// postgres_test.go — with TIMESERIES_TEST_POSTGRES set, newTestHandler gives
// every test a fresh database on an embedded Postgres instead of SQLite:
//
//   TIMESERIES_TEST_POSTGRES=1 go test ./...
//
// The first run downloads the Postgres binaries (~30 MB).

import (
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

var testPostgres *DBConfig
var testDatabaseCounter atomic.Int64

func TestMain(m *testing.M) {
	var stop func() error
	if os.Getenv("TIMESERIES_TEST_POSTGRES") != "" {
		var err error
		stop, err = startTestPostgres()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	code := m.Run()
	if stop != nil {
		stop()
	}
	os.Exit(code)
}

func startTestPostgres() (func() error, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	runtime, err := os.MkdirTemp("", "timeseries-pg-")
	if err != nil {
		return nil, err
	}
	db := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Port(uint32(port)).Database("postgres").Username("ts").Password("ts").
		RuntimePath(runtime).DataPath(filepath.Join(runtime, "data")).Logger(nil))
	if err := db.Start(); err != nil {
		return nil, fmt.Errorf("start embedded postgres: %w", err)
	}
	testPostgres = &DBConfig{UsePostgres: true, IPOrPath: "127.0.0.1", Port: port, User: "ts", Password: "ts"}
	fmt.Printf("tests run against embedded Postgres on port %d\n", port)
	return func() error {
		defer os.RemoveAll(runtime)
		return db.Stop()
	}, nil
}

// newTestHandler opens a private database (SQLite file, or a fresh Postgres
// database), bypassing the process-wide singleton.
func newTestHandler(t *testing.T) *DbHandler {
	t.Helper()
	conf := DBConfig{Name: "test.db", IPOrPath: t.TempDir() + "/"}
	if testPostgres != nil {
		conf = *testPostgres
		conf.Name = fmt.Sprintf("test_%d_%d", os.Getpid(), testDatabaseCounter.Add(1))
		admin, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
			conf.IPOrPath, conf.Port, conf.User, conf.Password))
		if err != nil {
			t.Fatal(err)
		}
		defer admin.Close()
		if _, err := admin.Exec(`CREATE DATABASE ` + conf.Name); err != nil {
			t.Fatalf("create test database: %v", err)
		}
	}
	dbh := &DbHandler{conf: conf, timeout: 10 * time.Second, semaphore: make(chan struct{}, 10)}
	if err := dbh.openDatabase(); err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { dbh.DB.Close() })
	return dbh
}

func skipOnPostgres(t *testing.T, why string) {
	t.Helper()
	if testPostgres != nil {
		t.Skip(why)
	}
}

// rebindTest turns ? placeholders into $n on Postgres.
func (dbh *DbHandler) rebindTest(query string) string {
	if !dbh.conf.UsePostgres {
		return query
	}
	out, n := "", 0
	for _, r := range query {
		if r == '?' {
			n++
			out += fmt.Sprintf("$%d", n)
			continue
		}
		out += string(r)
	}
	return out
}
