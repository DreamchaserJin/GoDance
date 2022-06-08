package utils

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
)

type CsvTable struct {
	FileName string
	Records  []CsvRecord
}

type CsvRecord struct {
	Record map[string]string
}

func LoadCsvFile(filename string, row int) *CsvTable {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		panic("文件打开失败")
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)
	reader := csv.NewReader(file)
	if reader == nil {
		panic("reader is nil")
	}
	reader.LazyQuotes = true
	records, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}
	if len(records) < row {
		panic(fmt.Sprintf("%s is empty", filename))
	}

	colNum := len(records[0])
	recordNum := len(records)

	var allRecords []CsvRecord

	for i := row; i < recordNum; i++ {
		record := &CsvRecord{make(map[string]string)}
		for k := 0; k < colNum; k++ {
			record.Record[records[0][k]] = records[i][k]
		}
		allRecords = append(allRecords, *record)
	}

	return &CsvTable{FileName: filename, Records: allRecords}
}

func (c *CsvRecord) GetInt(fieldName string) int {
	r, err := strconv.Atoi(c.Record[fieldName])
	if err != nil {
		panic(err)
	}
	return r
}

func (c *CsvRecord) GetString(fieldName string) string {
	if data, ok := c.Record[fieldName]; ok {
		return data
	} else {
		panic("fieldName is not exist")
		return ""
	}
}
