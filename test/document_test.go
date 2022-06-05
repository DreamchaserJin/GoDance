package test

import (
	gdindex "GoDance/index"
	"GoDance/utils"
	"fmt"
	"testing"
)

func TestUpdateDocument(t *testing.T) {
	logger, err := utils.New("GoDanceTest")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	content := make(map[string]string)
	index := gdindex.NewIndexFromLocalFile("gk", utils.IDX_ROOT_PATH, logger)
	content["id"] = "6"
	content["year"] = "1977"
	content["region"] = "辽宁"
	content["title"] = "伟大的胜利"
	index.UpdateDocument(content)
	index.SyncMemorySegment()
}
