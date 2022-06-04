package test

import (
	gdindex "GoDance/index"
	"GoDance/index/segment"
	"GoDance/utils"
	"fmt"
	"testing"
	"time"
)

func TestCreateIndex(t *testing.T) {
	logger, err := utils.New("FalconSearcher")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := gdindex.NewEmptyIndex("gk", utils.IDX_ROOT_PATH, logger)

	field1 := segment.SimpleFieldInfo{
		FieldName: "year",
		FieldType: utils.IDX_TYPE_NUMBER,
	}
	field2 := segment.SimpleFieldInfo{
		FieldName: "region",
		FieldType: utils.IDX_TYPE_STRING,
	}
	field3 := segment.SimpleFieldInfo{
		FieldName: "title",
		FieldType: utils.IDX_TYPE_STRING_SEG,
	}

	// 添加索引
	err = index.AddField(field1)
	err = index.AddField(field2)
	err = index.AddField(field3)
	// 删除索引
	err = index.DeleteField("year")
}

func TestDeleteField(t *testing.T) {
	logger, err := utils.New("FalconSearcher")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := gdindex.NewIndexFromLocalFile("gk", utils.IDX_ROOT_PATH, logger)

	err = index.DeleteField("year")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
}

func TestAddDocument(t *testing.T) {
	utils.GSegmenter = utils.NewSegmenter("/home/hz/GoProject/GoDanceEngine/GoDance/test/dictionary/dict.txt")
	logger, err := utils.New("GoDanceTest")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := gdindex.NewIndexFromLocalFile("wechat", utils.IDX_ROOT_PATH, logger)

	content := make(map[string]string, 0)
	content["content"] = "南昌大学信息工程学院"
	content["id"] = "1"
	content["text"] = "111"
	index.AddDocument(content)
	content["content"] = "北京大学和清华大学"
	content["id"] = "2"
	content["text"] = "222"
	index.AddDocument(content)
	content["content"] = "字节跳动抖音部门"
	content["id"] = "3"
	content["text"] = "333"
	index.AddDocument(content)

	err = index.SyncMemorySegment()
	if err != nil {
		fmt.Printf("err happen : %v", err)
	}
}

func TestPKSearch(t *testing.T) {
	fmt.Println("test start")
}

func TestSearch(t *testing.T) {
	utils.GetDocIDsChan, utils.GiveDocIDsChan = utils.DocIdsMaker()
	utils.GSegmenter = utils.NewSegmenter("/home/hz/GoProject/GoDanceEngine/GoDance/test/dictionary/dict.txt")
	logger, err := utils.New("GD Engine")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := gdindex.NewIndexFromLocalFile("wechat", utils.IDX_ROOT_PATH, logger)

	q1 := utils.SearchQuery{
		FieldName: "content",
		Value:     "南昌",
		Type:      utils.IDX_TYPE_STRING_SEG,
	}

	//q2 := utils.SearchQuery{
	//	FieldName: "content",
	//	Value:     "大学",
	//	Type:      utils.IDX_TYPE_STRING_SEG,
	//}
	//
	//q3 := utils.SearchQuery{
	//	FieldName: "content",
	//	Value:     "学院",
	//	Type:      utils.IDX_TYPE_STRING_SEG,
	//}

	ids, ok := index.SearchKeyDocIds(q1)
	if ok {
		fmt.Println(ids)
	} else {
		fmt.Println("null")
	}

	for _, id := range ids {
		document, ok := index.GetDocument(id.Docid)
		if ok {
			fmt.Println(document)
		} else {
			fmt.Println("nil")
		}
	}

	time.Sleep(1 * time.Hour)

}
func TestMergeSegment(t *testing.T) {
	logger, err := utils.New("FalconSearcher")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := gdindex.NewIndexFromLocalFile("wechat", utils.IDX_ROOT_PATH, logger)

	index.MergeSegments(-1)

}

func TestAddField(t *testing.T) {
	utils.GSegmenter = utils.NewSegmenter("/home/hz/GoProject/GoDanceEngine/GoDance/test/dictionary/dict.txt")

	logger, err := utils.New("FalconSearcher")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := gdindex.NewIndexFromLocalFile("wechat", utils.IDX_ROOT_PATH, logger)

	if err != nil {
		return
	}

	field := segment.SimpleFieldInfo{
		FieldName: "text",
		FieldType: utils.IDX_TYPE_STRING_SEG,
	}

	err = index.AddField(field)

	content := make(map[string]string, 0)
	content["content"] = "Leetcode上号"
	content["id"] = "4"
	content["text"] = "testText"
	index.AddDocument(content)

	err = index.SyncMemorySegment()
	if err != nil {
		fmt.Printf("err happen : %v", err)
	}
}

func TestGetDoc(t *testing.T) {

	ids := make([]utils.DocIdNode, 0)
	ids = append(ids, utils.DocIdNode{
		Docid: 0,
	})
	ids = append(ids, utils.DocIdNode{
		Docid: 1,
	})

	logger, err := utils.New("GoDanceEngine")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := gdindex.NewIndexFromLocalFile("wechat", utils.IDX_ROOT_PATH, logger)

	for _, id := range ids {
		document, ok := index.GetDocument(id.Docid)
		if ok {
			fmt.Println(document)
		} else {
			fmt.Println("nil")
		}
	}
}
