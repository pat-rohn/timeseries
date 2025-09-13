package timeseries

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

func TestDb(t *testing.T) {
	log.SetLevel(log.WarnLevel)
	names := []string{"randFloat", "randFloat2Mix", "randInt", "randText"}
	var timestamps []string
	var randFloat []string
	var randFloat2Mix []string
	var randInt []string
	var randText []string
	for i := 0; i < 10000; i++ {
		timestamps = append(timestamps, time.Now().Format("2006-01-02 15:04:05.000"))
		randomNmr := rand.Float32()*10000000.0 - 1000
		randFloat = append(randFloat, fmt.Sprintf("%f", randomNmr))
		randomNmr = rand.Float32()*0.0001 - 0.00005
		if i%10 == 3 {
			randFloat2Mix = append(randFloat2Mix, fmt.Sprintf("IamText%d", rand.Int()*1500))
		} else {
			randFloat2Mix = append(randFloat2Mix, fmt.Sprintf("%f", randomNmr))
		}

		randInt = append(randInt, fmt.Sprintf("%d", rand.Int()*2000-1000))
		randText = append(randText, fmt.Sprintf("IamText%d", rand.Int()*1500))
	}

	var data [][]string
	data = append(data, randFloat)
	data = append(data, randFloat2Mix)
	data = append(data, randInt)
	data = append(data, randText)
	is := ImportStruct{
		Timestamps: timestamps,
		Names:      names,
		Data:       data,
	}

	fmt.Printf("data:%+v", is)
	dbh := DBHandler(GetDefaultDBConfig())
	defer dbh.Close()
	err := dbh.InsertIntoDatabase("randomtest", is)
	if err != nil {
		t.Fatalf("Failed to insert data:%v", err)
	}
	err = os.Remove("data.db")
	if err != nil {
		t.Errorf("Failed to remove data:%v", err)
	}
}

func TestDBStructs(t *testing.T) {
	var importRows []ImportRowStruct
	for i := 0; i < 10; i++ {
		names := []string{"first", "second", "third"}
		values := []string{fmt.Sprintf("%f", float64(i)/1.0), fmt.Sprintf("%f", float64(i)/1.0), fmt.Sprintf("%f", float64(i)*1.0)}
		is := ImportRowStruct{
			Names:     names,
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			Values:    values,
		}
		importRows = append(importRows, is)
	}
	dbh := DBHandler(GetDefaultDBConfig())
	defer dbh.Close()
	_, err := dbh.InsertRowsToTable("migrateTest", importRows)
	if err != nil {
		t.Fatalf("Failed to insert data:%v", err)
	}
	err = dbh.InsertIntoDatabase("migrateTest", CreateImportTable(importRows))
	if err != nil {
		t.Errorf("Failed to insert data after CreateImportTable:%v", err)
	}
	err = os.Remove("data.db")
	if err != nil {
		t.Errorf("Failed to remove data:%v", err)
	}
}
