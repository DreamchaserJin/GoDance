package test

import (
	"fmt"
	"gdindex"
	"gdindex/segment"
	"testing"
	"utils"
)

func TestCreateIndex(t *testing.T) {
	logger, err := utils.New("GoDanceEngine")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}

	idx := gdindex.NewEmptyIndex("wechat", utils.IDX_ROOT_PATH, logger)

	field1 := segment.SimpleFieldInfo{
		FieldName: "id",
		FieldType: utils.IDX_TYPE_PK,
	}
	field2 := segment.SimpleFieldInfo{
		FieldName: "content",
		FieldType: utils.IDX_TYPE_STRING_SEG,
	}
	field3 := segment.SimpleFieldInfo{
		FieldName: "text",
		FieldType: utils.IDX_TYPE_NUMBER,
	}

	err = idx.AddField(field1)
	err = idx.AddField(field2)
	err = idx.AddField(field3)
}

func TestAddDocument(t *testing.T) {
	utils.GSegmenter = utils.NewSegmenter("/home/hz/GoProject/GoDanceEngine/GoDance/utils/dict/dict.txt")
	logger, err := utils.New("GoDance")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := gdindex.NewIndexFromLocalFile("wechat", utils.IDX_ROOT_PATH, logger)
	//field := utils.SimpleFieldInfo{
	//	FieldName: "test1",
	//	FieldType: utils.IDX_TYPE_STRING_SEG,
	//	PflOffset: -1,
	//	PflLen:    -1,
	//}
	//index.AddField(field)

	content := make(map[string]string, 0)
	content["content"] = "123"
	content["id"] = "4"
	content["text"] = "456"
	index.AddDocument(content)
	content["content"] = "234"
	content["id"] = "5"
	content["text"] = "567"
	index.AddDocument(content)
	content["content"] = "345"
	content["id"] = "6"
	content["text"] = "678"
	index.AddDocument(content)

	err = index.SyncMemorySegment()
	if err != nil {
		fmt.Printf("err happen : %v", err)
	}
}

func TestAddField(t *testing.T) {
	utils.GSegmenter = utils.NewSegmenter("./GoDance/data/dict.txt")

	logger, err := utils.New("FalconSearcher")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := gdindex.NewIndexFromLocalFile("wechat", "./index/", logger)

	if err != nil {
		return
	}

	field := segment.SimpleFieldInfo{
		FieldName: "text",
		FieldType: utils.IDX_TYPE_STRING_SEG,
	}

	err = index.AddField(field)

	updateType := utils.UPDATE_TYPE_ADD
	content := make(map[string]string, 0)
	content["content"] = "Leetcode上号"
	content["id"] = "4"
	content["text"] = "testText"
	index.UpdateDocument(content, updateType)

	err = index.SyncMemorySegment()
	if err != nil {
		fmt.Printf("err happen : %v", err)
	}
}

func TestDeleteField(t *testing.T) {
	logger, err := utils.New("FalconSearcher")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := gdindex.NewIndexFromLocalFile("wechat", utils.IDX_ROOT_PATH, logger)

	err = index.DeleteField("time")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
}

func TestPKSearch(t *testing.T) {
	utils.GSegmenter = utils.NewSegmenter("/home/itcast/hz/GoProject/FalconEngine/src/data/dict.txt")
	logger, err := utils.New("FalconSearcher")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := FalconIndex.NewIndexWithLocalFile("wechat", "./index/", logger)

	detail, b := index.FindPKDetail("1")
	if b {
		fmt.Println(detail)
	}
}

func TestSearch(t *testing.T) {
	utils.GSegmenter = utils.NewSegmenter("/home/itcast/hz/GoProject/FalconEngine/src/data/dict.txt")
	logger, err := utils.New("FalconSearcher")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := FalconIndex.NewIndexWithLocalFile("wechat", "./index/", logger)

	index.Close()
}

func TestMergeSegment(t *testing.T) {
	logger, err := utils.New("FalconSearcher")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := FalconIndex.NewIndexWithLocalFile("wechat", "./index/", logger)

	index.MergeSegments(-1)

}
