package test

import (
	"GoDance/utils"
	"fmt"
	"testing"
)

func TestLoadCsvFile(t *testing.T) {
	table := utils.LoadCsvFile("/home/iceberg/Downloads/高考作文.csv", 1)
	for i, val := range table.Records {
		fmt.Println(i)
		fmt.Println(val)
	}
}
