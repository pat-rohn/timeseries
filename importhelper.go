package timeseries

import "fmt"

// ImportStruct contains all data which are needed to add to a database.
// One Timestamp can have multiple values
type ImportStruct struct {
	Names      []string
	Timestamps []string
	Data       [][]string
}

// TimeseriesImportStruct is to insert into a timeseries table
type TimeseriesImportStruct struct {
	Tag        string
	Timestamps []string
	Values     []string
	Comments   []string
}

// ImportRowStruct contains data for one row
type ImportRowStruct struct {
	Names     []string
	Timestamp string
	Values    []string
}

func CreateImportTable(importRows []ImportRowStruct) ImportStruct {
	var timestamps []string
	var data [][]string
	for _ = range importRows[0].Names {
		data = append(data, []string{})
	}
	for _, row := range importRows {
		timestamps = append(timestamps, row.Timestamp)

		for i, _ := range importRows[0].Names {
			data[i] = append(data[i], row.Values[i])
		}
	}
	is := ImportStruct{
		Names:      importRows[0].Names,
		Timestamps: timestamps,
		Data:       data,
	}
	fmt.Printf("data:%+v", is)
	return is
}
