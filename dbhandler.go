package timeseries

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	_ "github.com/lib/pq"
	_ "modernc.org/sqlite"
)

const (
	logPkg                 string = "dbhandler"
	columnIntegerType      int    = 0
	columnFloatType        int    = 1
	columnTextType         int    = 2
	DefaultTimeseriesTable string = "measurements"
)

type DBConfig struct {
	Name        string `json:"Name"`
	IPOrPath    string `json:"IPOrPath"`
	UsePostgres bool   `json:"UsePostgres"`
	User        string `json:"User"`
	Password    string `json:"Password"`
	Port        int    `json:"Port"`
}

type DbHandler struct {
	conf      DBConfig
	DB        *sql.DB
	semaphore chan struct{} // serializes operations with timeout support
	timeout   time.Duration
}

var dbhandler *DbHandler
var dbMu sync.Mutex

// Singleton for dbhandler
func DBHandler(conf DBConfig) *DbHandler {
	dbMu.Lock()
	defer dbMu.Unlock()
	if dbhandler == nil {
		dbhandler = &DbHandler{
			conf:      conf,
			timeout:   time.Second * 10,
			semaphore: make(chan struct{}, 1),
		}
		if err := dbhandler.openDatabase(); err != nil {
			log.WithField("package", logPkg).Fatalf(
				"Failed to create database: %v", err)
		}
		log.Infof("%+v", dbhandler.conf)
	}
	if dbhandler.conf != conf {
		log.WithField("package", logPkg).Warnf(
			"DBHandler already initialized with a different config; new config ignored")
	}
	return dbhandler
}

// OpenDatabase creates a sqlite or postgres db
func (dbh *DbHandler) openDatabase() error {
	logFields := log.Fields{"package": logPkg, "func": "CreateDatabase"}
	log.WithFields(logFields).Infof("Create/Open database with path/ip:%s with name %s",
		dbh.conf.IPOrPath, dbh.conf.Name)
	if dbh.conf.UsePostgres {
		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
			"password=%s dbname=%s sslmode=disable",
			dbh.conf.IPOrPath, dbh.conf.Port, dbh.conf.User, dbh.conf.Password, dbh.conf.Name)
		log.WithFields(logFields).Tracef(
			"Open database: %v", psqlInfo)
		database, err := sql.Open("postgres", psqlInfo)
		if err != nil {
			log.WithField("package", logPkg).Errorf(
				"Failed to open db %v", err)
			return fmt.Errorf("failed to open db %v", err)
		}
		dbh.DB = database
	} else {
		if len(dbh.conf.IPOrPath) > 0 {
			log.WithFields(logFields).Tracef("Create Folder: %v", dbh.conf.IPOrPath)
			if _, err := os.Stat(dbh.conf.IPOrPath); err != nil {
				if os.IsNotExist(err) {
					err := os.MkdirAll(dbh.conf.IPOrPath, 0755)
					if err != nil {
						log.WithFields(logFields).Errorf("Failed to create path %v", err)
					}
				}
			}
		}

		database, err := sql.Open("sqlite", dbh.conf.IPOrPath+dbh.conf.Name)
		if err != nil {
			log.WithFields(logFields).Errorf("Failed to open db %v", err)
			return fmt.Errorf("failed to open db %v", err)
		}
		dbh.DB = database
	}
	log.WithFields(logFields).Infof("Opened database with name %s ",
		dbh.conf.Name)

	return nil
}

func (dbh *DbHandler) Close() error {
	err := dbh.DB.Close()
	dbMu.Lock()
	dbhandler = nil
	dbMu.Unlock()
	log.WithField("package", logPkg).Infof("Closed database %s", dbh.conf.Name)
	if err != nil {
		log.WithField("package", logPkg).Warnf("Closing %s failed %v",
			dbh.conf.Name, err)
		return err
	}
	return nil
}

// InsertIntoDatabase stores values into database
func (dbh *DbHandler) InsertIntoDatabase(tableName string, is ImportStruct) error {
	var str strings.Builder
	log.WithField("package", logPkg).Tracef(
		"Columns: %v", is.Names)
	log.WithField("package", logPkg).Tracef(
		"Columns: %v", len(is.Names))
	log.WithField("package", logPkg).Tracef(
		"Entries: %v", len(is.Data))
	log.WithField("package", logPkg).Tracef(
		"Values: %v", len(is.Data[0]))

	log.WithField("package", logPkg).Tracef(
		"Entries: %v", is.Data)
	timeStampStr := "DATETIME"
	if dbh.conf.UsePostgres {
		timeStampStr = "TIMESTAMP"
	}
	str.WriteString("CREATE TABLE IF NOT EXISTS " + tableName + " (Timestamp " + timeStampStr + ", ")
	columnsOfText := make(map[int]bool)
	for columnNr, name := range is.Names {
		isNumeric := true
		for _, val := range is.Data[columnNr] {
			temp := strings.TrimSpace(val)
			if temp == "float" {
				continue
			}
			_, errInt := strconv.ParseInt(temp, 0, 64)
			_, errFloat := strconv.ParseFloat(temp, 64)
			if errInt != nil && errFloat != nil {
				isNumeric = false
				break
			}
		}
		if isNumeric {
			str.WriteString(name + " REAL DEFAULT NULL, ")
			log.WithField("package", logPkg).Tracef("Is numeric column: %v", name)
			columnsOfText[columnNr] = true
		} else {
			str.WriteString(name + " TEXT DEFAULT NULL, ")
			log.WithField("package", logPkg).Tracef("Is text column: %v", name)
			columnsOfText[columnNr] = false
		}
	}

	sqlStr := str.String()[0 : len(str.String())-2]
	sqlStr += ");"
	log.WithField("package", logPkg).Tracef("create query: %s", sqlStr)
	err := dbh.execute(func() error {
		_, err := dbh.DB.Exec(sqlStr)
		if err != nil {
			log.WithField("package", logPkg).Errorf("Failed to create db %v", err)
			return fmt.Errorf("failed to execute sql-statement: %v", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	str.Reset()

	str.WriteString("INSERT INTO " + tableName + " (Timestamp, ")
	isFirst := true
	for _, name := range is.Names {
		if isFirst {
			isFirst = false
			str.WriteString(name)
		} else {
			str.WriteString(", " + name)
		}
	}

	log.WithField("package", logPkg).Infof("Insert string: %v", str.String())
	str.WriteString(") VALUES \n")

	for entryIndex, ts := range is.Timestamps {
		str.WriteString("('" + ts + "', ")
		isFirst := true
		for dataIndex, columnName := range is.Names {
			val := strings.TrimSpace(is.Data[dataIndex][entryIndex])
			if columnsOfText[dataIndex] {
				_, errFloat := strconv.ParseFloat(val, 64)
				_, errInt := strconv.ParseInt(val, 0, 64)
				if errFloat != nil && errInt != nil {
					// it can be float or integer, db-type is set to real
					log.WithField("package", logPkg).Warnf(
						"Skip number in %s because parsing failed: %s", columnName, errFloat)
					val = "null" // this can be imported in column of type real
				}
			}
			if !columnsOfText[dataIndex] {
				val = "'" + strings.ReplaceAll(val, "'", "''") + "'"
			}
			if isFirst {
				str.WriteString(val)
				isFirst = false

			} else {
				str.WriteString(", " + val)
			}
		}
		str.WriteString("),\n")
	}
	log.WithField("package", logPkg).Traceln("Finished creating string")
	sqlStr = str.String()

	sqlStr = sqlStr[0 : len(sqlStr)-2]
	if err := dbh.writeToDB(sqlStr); err != nil {
		log.WithField("package", logPkg).Errorf("Failed to execute sql-statement: %v\n", err)
		return fmt.Errorf("failed to execute sql-statement: %v", err)
	}
	log.WithField("package", logPkg).Infof("Succesfully imported values into table: %v", tableName)
	return nil
}

// buildTableAndColumnTypes builds a CREATE TABLE IF NOT EXISTS statement for the
// given table and returns it together with a map of column index -> column type
// constant (columnFloatType / columnTextType).
func (dbh *DbHandler) buildTableAndColumnTypes(tableName string, names []string, values []string) (string, map[int]int) {
	var str strings.Builder
	timeStampStr := "DATETIME"
	if dbh.conf.UsePostgres {
		timeStampStr = "TIMESTAMP"
	}
	str.WriteString("CREATE TABLE IF NOT EXISTS " + tableName + " (Timestamp " + timeStampStr + ", ")
	colTypes := make(map[int]int)
	for columnNr, name := range names {
		temp := strings.TrimSpace(values[columnNr])
		_, errInt := strconv.ParseInt(temp, 0, 64)
		_, errFloat := strconv.ParseFloat(temp, 64)
		if errInt == nil || errFloat == nil || values[columnNr] == "float" {
			str.WriteString(name + " REAL DEFAULT NULL, ")
			colTypes[columnNr] = columnFloatType
		} else {
			str.WriteString(name + " TEXT DEFAULT NULL, ")
			colTypes[columnNr] = columnTextType
		}
	}
	sqlStr := str.String()[0 : len(str.String())-2]
	sqlStr += ", Fetched INTEGER DEFAULT 0);"
	return sqlStr, colTypes
}

// buildInsertRowSQL builds a single-row INSERT statement using the column type
// map returned by buildTableAndColumnTypes.
func buildInsertRowSQL(tableName string, is ImportRowStruct, colTypes map[int]int) string {
	var str strings.Builder
	str.WriteString("INSERT INTO " + tableName + " (Timestamp, ")
	for i, name := range is.Names {
		if i == 0 {
			str.WriteString(name)
		} else {
			str.WriteString(", " + name)
		}
	}
	str.WriteString(") VALUES ('" + is.Timestamp + "', ")
	for dataIndex := range is.Names {
		val := strings.TrimSpace(is.Values[dataIndex])
		if colTypes[dataIndex] == columnFloatType || colTypes[dataIndex] == columnIntegerType {
			_, errFloat := strconv.ParseFloat(val, 64)
			_, errInt := strconv.ParseInt(val, 0, 64)
			if errFloat != nil && errInt != nil {
				log.WithField("package", logPkg).Warnf(
					"Skip number because parsing failed: %s", errFloat)
				val = "null"
			}
		}
		if colTypes[dataIndex] == columnTextType {
			val = "'" + strings.ReplaceAll(val, "'", "''") + "'"
		}
		if dataIndex == 0 {
			str.WriteString(val)
		} else {
			str.WriteString(", " + val)
		}
	}
	str.WriteString(")")
	return str.String()
}

// InsertRowsToTable imports all rows into the table inside a single transaction.
// The table schema is inferred by scanning all rows. On any failure the entire
// batch is rolled back and all rows are returned as failed.
func (dbh *DbHandler) InsertRowsToTable(tableName string, importStructs []ImportRowStruct) ([]ImportRowStruct, error) {
	logFields := log.Fields{"package": logPkg, "func": "InsertRowsToTable"}
	if len(importStructs) == 0 {
		return nil, nil
	}

	// Ensure the table exists (idempotent DDL, done outside the transaction).
	// Determine column types by scanning every row so that a column is only
	// declared REAL when ALL of its values are numeric.
	allNumeric := make([]bool, len(importStructs[0].Names))
	for i := range allNumeric {
		allNumeric[i] = true
	}
	for _, row := range importStructs {
		for i, val := range row.Values {
			if allNumeric[i] {
				temp := strings.TrimSpace(val)
				_, errInt := strconv.ParseInt(temp, 0, 64)
				_, errFloat := strconv.ParseFloat(temp, 64)
				if errInt != nil && errFloat != nil {
					allNumeric[i] = false
				}
			}
		}
	}
	typeHints := make([]string, len(importStructs[0].Names))
	for i, isNum := range allNumeric {
		if isNum {
			typeHints[i] = "0" // numeric sentinel → REAL column
		} else {
			typeHints[i] = "text" // non-numeric sentinel → TEXT column
		}
	}
	createSQL, colTypes := dbh.buildTableAndColumnTypes(tableName, importStructs[0].Names, typeHints)
	log.WithFields(logFields).Tracef("create query: %s", createSQL)
	if err := dbh.execute(func() error {
		_, err := dbh.DB.Exec(createSQL)
		return err
	}); err != nil {
		log.WithFields(logFields).Errorf("Failed to create table: %v", err)
		return importStructs, fmt.Errorf("failed to create table: %v", err)
	}

	// Insert all rows atomically.
	err := dbh.executeTransaction(func(tx *sql.Tx) error {
		for _, is := range importStructs {
			insertSQL := buildInsertRowSQL(tableName, is, colTypes)
			if _, err := tx.Exec(insertSQL); err != nil {
				log.WithFields(logFields).Errorf("Failed to import row: %v", err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.WithFields(logFields).Errorf("Transaction failed, all %d rows rolled back: %v", len(importStructs), err)
		return importStructs, fmt.Errorf("transaction failed, all rows rolled back: %v", err)
	}
	log.WithFields(logFields).Tracef("Successfully imported %d rows", len(importStructs))
	return nil, nil
}

// InsertRowToTable inserts one row into database
func (dbh *DbHandler) InsertRowToTable(tableName string, is ImportRowStruct) error {
	log.WithField("package", logPkg).Tracef("Columns: %v (%d), Values: %d",
		is.Names, len(is.Names), len(is.Values))

	createSQL, colTypes := dbh.buildTableAndColumnTypes(tableName, is.Names, is.Values)
	log.WithField("package", logPkg).Tracef("create query: %s", createSQL)
	if err := dbh.execute(func() error {
		_, err := dbh.DB.Exec(createSQL)
		if err != nil {
			log.WithField("package", logPkg).Errorf("Failed to create table: %v", err)
			return fmt.Errorf("failed to create table: %v", err)
		}
		return nil
	}); err != nil {
		return err
	}

	insertSQL := buildInsertRowSQL(tableName, is, colTypes)
	log.WithField("package", logPkg).Traceln("Finished creating string")
	if err := dbh.writeToDB(insertSQL); err != nil {
		log.WithField("package", logPkg).Errorf("Failed to execute sql-statement: %v", err)
		return fmt.Errorf("failed to execute sql-statement: %v", err)
	}
	return nil
}

func (dbh *DbHandler) ReadTPH() ImportStruct {
	logFields := log.Fields{"package": logPkg, "fnct": "readTPH"}

	names := []string{"Temperature", "Pressure", "Humidity"}
	sqlstr := `SELECT TIMESTAMP, Temperature, Pressure, Humidity FROM sensor_data WHERE Fetched = 0 ORDER BY Timestamp;`
	log.WithFields(logFields).Tracef("Select statement: %v", sqlstr)

	rows, err := dbh.DB.Query(sqlstr)
	if err != nil {
		log.WithFields(logFields).Errorf("Failed to query db: %v", err)
		return ImportStruct{}
	}
	defer rows.Close()

	var timestamps []string
	var Temperatures []string
	var Pressures []string
	var Humiditys []string
	counter := 0
	for rows.Next() {
		var timestamp time.Time
		var temperature float32
		var pressure float32
		var humidity float32
		err = rows.Scan(&timestamp, &temperature, &pressure, &humidity)
		if err != nil {
			log.WithFields(logFields).Warn(err)
			continue
		}
		timestamps = append(timestamps, timestamp.Format("2006-01-02 15:04:05.000"))
		Temperatures = append(Temperatures, fmt.Sprintf("%f", temperature))
		Pressures = append(Pressures, fmt.Sprintf("%f", pressure))
		Humiditys = append(Humiditys, fmt.Sprintf("%f", humidity))
		if counter > 1000 {
			log.WithFields(logFields).Warnln("1000 reached")
			break
		}
		counter++
	}
	var data [][]string
	data = append(data, Temperatures)
	data = append(data, Pressures)
	data = append(data, Humiditys)

	return ImportStruct{
		Names:      names,
		Timestamps: timestamps,
		Data:       data,
	}
}

func (dbh *DbHandler) ReadAllTPH() ImportStruct {
	logFields := log.Fields{"package": logPkg, "fnct": "readTPH"}

	names := []string{"Temperature", "Pressure", "Humidity"}
	sqlstr := `SELECT TIMESTAMP, Temperature, Pressure, Humidity FROM living;`
	log.WithFields(logFields).Tracef("Select statement: %v", sqlstr)
	var rows *sql.Rows
	err := dbh.execute(func() error {
		var err error
		rows, err = dbh.DB.Query(sqlstr)
		if err != nil {
			log.Fatal(err)
		}
		return nil
	})
	if err != nil {
		log.WithFields(logFields).Errorf("Failed to read from db: %v", err)
		return ImportStruct{}
	}
	defer rows.Close()
	var timestamps []string
	var Temperatures []string
	var Pressures []string
	var Humiditys []string
	counter := 0
	for rows.Next() {
		var timestamp time.Time
		var temperature float32
		var pressure float32
		var humidity float32
		err = rows.Scan(&timestamp, &temperature, &pressure, &humidity)
		if err != nil {
			log.WithFields(logFields).Warn(err)
			continue
		}
		timestamps = append(timestamps, timestamp.Format("2006-01-02 15:04:05.000"))
		Temperatures = append(Temperatures, fmt.Sprintf("%f", temperature))
		Pressures = append(Pressures, fmt.Sprintf("%f", pressure))
		Humiditys = append(Humiditys, fmt.Sprintf("%f", humidity))
		if counter%1000 == 0 {
			log.WithFields(logFields).Warnf("%vk", counter/1000)
		}
		counter++
	}
	var data [][]string
	data = append(data, Temperatures)
	data = append(data, Pressures)
	data = append(data, Humiditys)

	return ImportStruct{
		Names:      names,
		Timestamps: timestamps,
		Data:       data,
	}
}

func (dbh *DbHandler) SetFetched(firstTimestamp string, lastTimestamp string) error {
	logFields := log.Fields{"package": logPkg, "fnct": "SetFetched"}

	var statement string
	if dbh.conf.UsePostgres {
		statement = "UPDATE sensor_data SET Fetched=$1 WHERE Timestamp<=$2 AND Timestamp>=$3"
	} else {
		statement = "UPDATE sensor_data SET Fetched=? WHERE Timestamp<=? AND Timestamp>=?"
	}
	err := dbh.execute(func() error {
		res, err := dbh.DB.Exec(statement, 1, lastTimestamp, firstTimestamp)
		if err != nil {
			log.WithFields(logFields).Errorf("Failed to get affected rows ... :  %v, %v", err, statement)
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			log.WithFields(logFields).Errorf("Failed to get affected rows ... :  %v, %v", err, statement)
			return err
		}
		log.WithFields(logFields).Infof("Rows Affected: %v", affected)
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// AddColumnToTable adds a column with type number into table (real default null))
func (dbh *DbHandler) AddColumnToTable(tableName string, columnName string) error {
	logFields := log.Fields{"package": logPkg, "func": "AddColumnToTable"}

	err := dbh.execute(func() error {
		log.WithFields(logFields).Infof("Add %v to %v", columnName, tableName)

		_, err := dbh.DB.Exec(`ALTER TABLE ` + tableName +
			` ADD COLUMN IF NOT EXISTS "` + columnName + `" REAL DEFAULT NULL;`)
		if err != nil {
			log.WithFields(logFields).Errorf("Failed to add column to table %v: %v", tableName, err)
			return err
		}

		return nil
	})
	return err
}

func GetDefaultDBConfig() DBConfig {
	return DBConfig{
		Name:        "data.db",
		IPOrPath:    "",
		UsePostgres: false,
		User:        "webuser",
		Password:    "PlottyPW",
		Port:        5432,
	}
}

// CreateTimeseriesTable creates a table for timeseries values.
// Consider adding timescaledb features for postgres.
func (dbh *DbHandler) CreateTimeseriesTable(tableName string) error {
	timeStampStr := "DATETIME"
	if dbh.conf.UsePostgres {
		timeStampStr = "TIMESTAMP"
	}

	sqlStr := `CREATE TABLE IF NOT EXISTS ` + tableName + ` (
		time ` + timeStampStr + `,
		tag        TEXT                NOT NULL,
		value      DOUBLE PRECISION    NULL,
		comment    TEXT                DEFAULT ''
	   );
	 `
	return dbh.writeToDB(sqlStr)
}

// InsertTimeseries stores values into timeseries table
func (dbh *DbHandler) InsertTimeseries(is TimeseriesImportStruct, onConflictDoNothing bool, table string) error {
	log.WithField("package", logPkg).Tracef("Entries: %v", is.Values)
	log.WithField("package", logPkg).Infof("Tag: %v", is.Tag)

	if len(is.Timestamps) == 0 {
		return nil
	}

	// Build all INSERT chunks up front so they can be committed atomically.
	var chunks []string
	includeComment := len(is.Comments) > 0
	var insertHeader string
	if includeComment {
		insertHeader = "INSERT INTO " + table + " (time, tag, value, comment) VALUES \n"
	} else {
		insertHeader = "INSERT INTO " + table + " (time, tag, value) VALUES \n"
	}
	var str strings.Builder
	str.WriteString(insertHeader)

	for entryIndex, ts := range is.Timestamps {
		str.WriteString("('" + ts + "', '" + is.Tag + "',")
		val := strings.TrimSpace(is.Values[entryIndex])
		_, errFloat := strconv.ParseFloat(val, 64)
		_, errInt := strconv.ParseInt(val, 0, 64)
		if errFloat != nil && errInt != nil {
			log.WithField("package", logPkg).Infof(
				"Skip number in %s because parsing failed: %s", is.Values[entryIndex], errFloat)
			val = "null"
		}
		comment := ""
		if includeComment && entryIndex < len(is.Comments) {
			comment = strings.ReplaceAll(is.Comments[entryIndex], "'", "''")
		}
		if includeComment {
			str.WriteString(val + ", '" + comment + "'),\n")
		} else {
			str.WriteString(val + "),\n")
		}

		if entryIndex%100000 == 0 && entryIndex != 0 {
			chunk := str.String()
			chunk = chunk[0 : len(chunk)-2]
			if onConflictDoNothing {
				chunk += " on conflict do nothing"
			}
			chunks = append(chunks, chunk)
			str.Reset()
			str.WriteString(insertHeader)
		}
	}
	log.WithField("package", logPkg).Traceln("Finished creating string")
	// Only flush the last partial chunk if it actually contains row data.
	// When a chunk-boundary flush happened on the very last iteration, str
	// was reset to just the INSERT header and there is nothing left to append.
	if str.Len() > 0 {
		// The header alone ends with "VALUES \n" (8+ chars); a real chunk has
		// at least one row appended after the header. We detect "header only"
		// by checking whether str still ends with exactly "VALUES \n".
		raw := str.String()
		const valueSuffix = "VALUES \n"
		if !strings.HasSuffix(raw, valueSuffix) {
			chunk := raw[0 : len(raw)-2]
			if onConflictDoNothing {
				chunk += " on conflict do nothing"
			}
			chunks = append(chunks, chunk)
		}
	}

	// Execute all chunks inside a single transaction so the whole import is
	// atomic: a failure on any chunk rolls back everything written so far.
	return dbh.executeTransaction(func(tx *sql.Tx) error {
		for _, c := range chunks {
			if _, err := tx.Exec(c); err != nil {
				log.WithField("package", logPkg).Errorf("%v", err)
				return err
			}
		}
		return nil
	})
}

func (dbh *DbHandler) writeToDB(sqlStr string) error {

	if len(sqlStr) > 2000 {
		log.WithField("package", logPkg).Tracef(
			"start from query: %s\n", sqlStr[0:500])
		log.WithField("package", logPkg).Tracef(
			"end from query: %v\n", sqlStr[len(sqlStr)-500:])
	} else {
		log.WithField("package", logPkg).Tracef(
			"full query: %s\n", sqlStr)
	}
	err := dbh.execute(func() error {
		_, err := dbh.DB.Exec(sqlStr)
		if err != nil {
			log.WithField("package", logPkg).Error(err)
			return err
		}
		return nil
	})
	return err
}

// Do not forget to close rows
func (db *DbHandler) ExecuteQuery(sqlStr string, args ...any) (*sql.Rows, error) {
	logFields := log.Fields{"fnct": "executeQuery"}
	log.WithFields(logFields).Infof("Execute query")

	if len(sqlStr) > 2000 {
		log.WithFields(logFields).Tracef(
			"start from query: %s\n", sqlStr[0:500])
		log.WithFields(logFields).Tracef(
			"end from query: %v\n", sqlStr[len(sqlStr)-500:])
	} else {
		log.WithFields(logFields).Tracef(
			"full query: %s\n", sqlStr)
	}
	var rows *sql.Rows
	err := db.execute(func() error {
		var err error
		rows, err = db.DB.Query(sqlStr, args...)
		if err != nil {
			log.WithField("package", logPkg).Error(err)
			return err
		}
		return nil
	})
	if err != nil {
		log.WithField("package", logPkg).Error(err)
		return nil, err
	}

	log.WithFields(logFields).Infof("Query executed")

	return rows, nil
}

func (db *DbHandler) Execute(sqlStr string, args ...any) error {
	logFields := log.Fields{"fnct": "executeQuery"}
	log.WithFields(logFields).Infof("Execute query")

	if len(sqlStr) > 2000 {
		log.WithFields(logFields).Tracef(
			"start from query: %s\n", sqlStr[0:500])
		log.WithFields(logFields).Tracef(
			"end from query: %v\n", sqlStr[len(sqlStr)-500:])
	} else {
		log.WithFields(logFields).Tracef(
			"full query: %s\n", sqlStr)
	}

	err := db.execute(func() error {
		var err error
		_, err = db.DB.Exec(sqlStr, args...)
		if err != nil {
			log.WithField("package", logPkg).Error(err)
			return err
		}
		return nil
	})
	if err != nil {
		log.WithField("package", logPkg).Error(err)
		return err
	}

	log.WithFields(logFields).Infof("Query executed")

	return nil
}

func (db *DbHandler) execute(operation func() error) error {
	select {
	case db.semaphore <- struct{}{}:
		defer func() { <-db.semaphore }()
		return operation()
	case <-time.After(db.timeout):
		return errors.New("operation timed out waiting for semaphore")
	}
}

// executeTransaction runs all operations inside a single DB transaction.
// If any operation returns an error the transaction is rolled back.
func (db *DbHandler) executeTransaction(operations func(*sql.Tx) error) error {
	select {
	case db.semaphore <- struct{}{}:
		defer func() { <-db.semaphore }()
		tx, err := db.DB.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		if err := operations(tx); err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				return fmt.Errorf("operation failed: %v; rollback also failed: %v", err, rbErr)
			}
			return err
		}
		return tx.Commit()
	case <-time.After(db.timeout):
		return errors.New("operation timed out waiting for semaphore")
	}
}
