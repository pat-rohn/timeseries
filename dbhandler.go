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
	logPkg            string = "dbhandler"
	columnIntegerType int    = 0
	columnFloatType   int    = 1
	columnTextType    int    = 2
)

type DBConfig struct {
	Name        string `json:"Name"`
	IPOrPath    string `json:"IPOrPath"`
	UsePostgres bool   `json:"UsePostgres"`
	User        string `json:"User"`
	Password    string `json:"Password"`
	Port        int    `json:"Port"`
	TableName   string `json:"TableName"`
}

type DbHandler struct {
	conf      DBConfig
	DB        *sql.DB
	mutex     sync.Mutex
	semaphore chan struct{} // limit number of concurrent operations
	timeout   time.Duration
}

var dbhandler *DbHandler
var once sync.Once

// Singleton for dbhandler
func DBHandler(conf DBConfig) *DbHandler {
	once.Do(func() {
		if dbhandler != nil {
			log.Fatalf("DbHandler already created with config: %+v", dbhandler.conf)
		}
		dbhandler = &DbHandler{
			conf:      conf,
			mutex:     sync.Mutex{},
			timeout:   time.Second * 10,
			semaphore: make(chan struct{}, 10),
		}
		if err := dbhandler.openDatabase(); err != nil {
			log.WithField("package", logPkg).Fatalf(
				"Failed to create database: %v", err)
		}
		log.Infof("%+v", dbhandler.conf)
	})
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
					err := os.MkdirAll(dbh.conf.IPOrPath, 0644)
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
	log.WithField("package", logPkg).Infof("Closed database %s", dbh.conf.Name)
	if err != nil {
		log.WithField("package", logPkg).Warnf("Closing %s failed %f",
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
		temp := strings.TrimSpace(is.Data[columnNr][0])
		_, errInt := strconv.ParseInt(temp, 0, 64)
		_, errFloat := strconv.ParseFloat(temp, 64)
		if errInt == nil || errFloat == nil || is.Data[columnNr][0] == "float" {
			str.WriteString(name + " REAL DEFAULT NULL, ")
			log.WithField("package", logPkg).Tracef(
				"Is number: %v", is.Data[columnNr][0])
			columnsOfText[columnNr] = true
		} else {
			str.WriteString(name + " TEXT DEFAULT NULL, ")
			columnsOfText[columnNr] = false
			log.WithField("package", logPkg).Tracef(
				"Is no number: %v", is.Data[columnNr][0])
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
				val = "'" + val + "'"
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

// InsertRowsToTable imports importStructs into table and returns failed rows
func (dbh *DbHandler) InsertRowsToTable(tableName string, importStructs []ImportRowStruct) ([]ImportRowStruct, error) {
	logFields := log.Fields{"package": logPkg, "func": "InsertRowsToTable"}
	var failedImports []ImportRowStruct

	for _, is := range importStructs {
		retryCounter := 3
		for retryCounter > 0 {
			retryCounter--
			err := dbh.InsertRowToTable(tableName, is)
			if err != nil {
				log.WithFields(logFields).Errorf("Failed to import row: %v", err)
				time.Sleep(time.Millisecond * 500)
			} else {
				log.WithFields(logFields).Traceln("succesfully imported row")
				break
			}
			if retryCounter == 0 {
				log.WithFields(logFields).Errorln("Unsuccesful rows. ")
				failedImports = append(failedImports, is)
			}
		}
	}
	if len(failedImports) > 0 {
		log.WithFields(logFields).Errorf("Failed to imports: %v", len(failedImports))
		return failedImports, fmt.Errorf("failed to imports: %v", len(failedImports))
	}
	return failedImports, nil
}

// InsertRowToTable inserts one row into database
func (dbh *DbHandler) InsertRowToTable(tableName string, is ImportRowStruct) error {

	var str strings.Builder
	log.WithField("package", logPkg).Tracef(
		"Columns: %v", is.Names)
	log.WithField("package", logPkg).Tracef(
		"Columns: %v", len(is.Names))
	log.WithField("package", logPkg).Tracef(
		"Entries: %v", len(is.Values))
	timeStampStr := "DATETIME"
	if dbh.conf.UsePostgres {
		timeStampStr = "TIMESTAMP"
	}
	str.WriteString("CREATE TABLE IF NOT EXISTS " + tableName + " (Timestamp " + timeStampStr + ", ")
	columnsOfText := make(map[int]int)
	for columnNr, name := range is.Names {
		temp := strings.TrimSpace(is.Values[columnNr])
		_, errInt := strconv.ParseInt(temp, 0, 64)
		_, errFloat := strconv.ParseFloat(temp, 64)
		if errInt == nil || errFloat == nil || is.Values[columnNr] == "float" {
			str.WriteString(name + " REAL DEFAULT NULL, ")
			columnsOfText[columnNr] = columnFloatType
		} else {
			str.WriteString(name + " TEXT DEFAULT NULL, ")
			columnsOfText[columnNr] = columnTextType
			log.WithField("package", logPkg).Tracef(
				"Is no number: %v", is.Values[columnNr])
		}
	}

	sqlStr := str.String()[0 : len(str.String())-2]
	sqlStr += ", Fetched INTEGER DEFAULT 0);"
	log.WithField("package", logPkg).Tracef("create query: %s", sqlStr)

	err := dbh.execute(func() error {
		_, err := dbh.DB.Query(sqlStr)
		if err != nil {
			log.WithField("package", logPkg).Errorf("Failed to create db %v", err)
			return fmt.Errorf("failed to execute sql string: %v", err)
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
	str.WriteString(") VALUES \n")

	str.WriteString("('" + is.Timestamp + "', ")
	isFirst = true
	for dataIndex := range is.Names {
		val := strings.TrimSpace(is.Values[dataIndex])
		if columnsOfText[dataIndex] == columnFloatType || columnsOfText[dataIndex] == columnIntegerType {
			_, errFloat := strconv.ParseFloat(val, 64)
			_, errInt := strconv.ParseInt(val, 0, 64)
			if errFloat != nil && errInt != nil {
				// we hope the db doesn't mind and accepts float for int and vice versa
				log.WithField("package", logPkg).Warnf(
					"Skip number because parsing failed: %s", errFloat)
				val = "null"

			}
		}
		if columnsOfText[dataIndex] == columnTextType {
			val = "'" + val + "'"
		}
		if isFirst {
			str.WriteString(val)
			isFirst = false

		} else {
			str.WriteString(", " + val)
		}
	}
	str.WriteString("),\n")
	log.WithField("package", logPkg).Traceln("Finished creating string")
	sqlStr = str.String()

	sqlStr = sqlStr[0 : len(sqlStr)-2]

	if err := dbh.writeToDB(sqlStr); err != nil {
		log.WithField("package", logPkg).Errorf("Failed to execute sql-statement: %v\n", err)
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
		log.Fatal(err)
	}

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
	rows.Close()

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
	rows.Close()

	return ImportStruct{
		Names:      names,
		Timestamps: timestamps,
		Data:       data,
	}
}

func (dbh *DbHandler) SetFetched(firstTimestamp string, lastTimestamp string) error {
	logFields := log.Fields{"package": logPkg, "fnct": "SetFetched"}

	statement := "UPDATE sensor_data SET Fetched=? WHERE Timestamp<=? AND Timestamp>=?"
	err := dbh.execute(func() error {
		res, err := dbh.DB.Exec(statement)
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
		TableName:   "measurements",
	}
}

// CreateTimeseriesTable creates a table for timeseries values.
// Consider adding timescaledb features for postgres.
func (dbh *DbHandler) CreateTimeseriesTable() error {
	timeStampStr := "DATETIME"
	if dbh.conf.UsePostgres {
		timeStampStr = "TIMESTAMP"
	}

	sqlStr := `CREATE TABLE IF NOT EXISTS ` + dbh.conf.TableName + ` (
		time ` + timeStampStr + `,
		tag        TEXT                NOT NULL,
		value      DOUBLE PRECISION    NULL,
		comment    TEXT                DEFAULT ''
	   );
	 `
	return dbh.writeToDB(sqlStr)
}

// InsertTimeseries stores values into timeseries table
func (dbh *DbHandler) InsertTimeseries(is TimeseriesImportStruct, onClonflictDoNothing bool) error {
	var str strings.Builder
	log.WithField("package", logPkg).Tracef(
		"Entries: %v", is.Values)
	log.WithField("package", logPkg).Infof(
		"Tag: %v", is.Tag)
	str.Reset()

	if len(dbh.conf.TableName) == 0 {
		dbh.conf.TableName = "measurements"
	}
	str.WriteString("INSERT INTO " + dbh.conf.TableName + " (time, tag, value)")
	log.WithField("package", logPkg).Infof("Insert string: %v", str.String())
	str.WriteString(" VALUES \n")

	for entryIndex, ts := range is.Timestamps {
		str.WriteString("('" + ts + "', '" + is.Tag + "',")

		val := strings.TrimSpace(is.Values[entryIndex])
		_, errFloat := strconv.ParseFloat(val, 64)
		_, errInt := strconv.ParseInt(val, 0, 64)
		if errFloat != nil && errInt != nil {
			// it can be float or integer, db-type is set to real
			log.WithField("package", logPkg).Infof(
				"Skip number in %s because parsing failed: %s", is.Values[entryIndex], errFloat)
			val = "null" // this can be imported in column of type real

		}
		str.WriteString(val + "),\n")
		if entryIndex%100000 == 0 && entryIndex != 0 {
			sqlStr := str.String()
			sqlStr = sqlStr[0 : len(sqlStr)-2]
			if onClonflictDoNothing {
				sqlStr += " on conflict do nothing"
			}
			err := dbh.writeToDB(sqlStr)
			if err != nil {
				log.WithField("package", logPkg).Errorf("%v", err)
				return err
			}
			str.Reset()
			str.WriteString("INSERT INTO measurements (time, tag, value)")
			log.WithField("package", logPkg).Infof("Insert string: %v", str.String())
			str.WriteString(" VALUES \n")
		}
	}
	log.WithField("package", logPkg).Traceln("Finished creating string")
	sqlStr := str.String()
	sqlStr = sqlStr[0 : len(sqlStr)-2]
	if onClonflictDoNothing {
		sqlStr += " on conflict  do nothing"
	}
	err := dbh.writeToDB(sqlStr)
	if err != nil {
		log.WithField("package", logPkg).Errorf("%v", err)
		return err
	}

	return nil
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

func (db *DbHandler) execute(operation func() error) error {
	select {
	case db.semaphore <- struct{}{}:
		defer func() { <-db.semaphore }()
		db.mutex.Lock()
		defer db.mutex.Unlock()
		return operation()
	case <-time.After(db.timeout):
		return errors.New("operation timed out waiting for semaphore")
	}
}
