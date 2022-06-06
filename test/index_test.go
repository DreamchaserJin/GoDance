package test

import (
	gdindex "GoDance/index"
	"GoDance/index/segment"
	"GoDance/utils"
	"fmt"
	"github.com/blevesearch/vellum"
	"strconv"
	"testing"
)

func TestCreateIndex(t *testing.T) {
	logger, err := utils.NewLogger("FalconSearcher")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := gdindex.NewEmptyIndex("gk", utils.IDX_ROOT_PATH, logger)

	field0 := segment.SimpleFieldInfo{
		FieldName: "id",
		FieldType: utils.IDX_TYPE_PK,
	}

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
	err = index.AddField(field0)
	err = index.AddField(field1)
	err = index.AddField(field2)
	err = index.AddField(field3)

	// 添加字段
	content := make(map[string]string, 0)
	csvTable := utils.LoadCsvFile("/home/iceberg/桌面/高考作文1.csv", 1)
	for i, val := range csvTable.Records {
		content["id"] = strconv.Itoa(i)
		content["year"] = val.GetString("year")
		content["region"] = val.GetString("region")
		content["title"] = val.GetString("title")
		index.AddDocument(content)
	}

	err = index.SyncMemorySegment()
	if err != nil {
		fmt.Printf("err happen : %v", err)
	}

}

func TestDeleteField(t *testing.T) {
	logger, err := utils.NewLogger("FalconSearcher")
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
	//utils.GSegmenter = utils.NewSegmenter("/home/hz/GoProject/GoDanceEngine/GoDance/test/dictionary/dict.txt")
	logger, err := utils.NewLogger("GoDanceTest")
	//if err != nil {
	//	fmt.Printf("err happen: %v", err)
	//}
	index := gdindex.NewIndexFromLocalFile("gk", utils.IDX_ROOT_PATH, logger)

	content := make(map[string]string, 0)
	csvTable := utils.LoadCsvFile("/home/iceberg/桌面/高考作文.csv", 1)
	for i, val := range csvTable.Records {
		content["id"] = strconv.Itoa(i)
		content["year"] = val.GetString("year")
		content["region"] = val.GetString("region")
		content["title"] = val.GetString("title")
		index.AddDocument(content)
	}

	err = index.SyncMemorySegment()
	if err != nil {
		fmt.Printf("err happen : %v", err)
	}
}

func TestFst(t *testing.T) {
	fst, err := vellum.Open("./data/gk_1001/title_invert.fst")
	if err != nil {
		panic(err)
	}
	minKey, _ := fst.GetMinKey()
	maxKey, _ := fst.GetMaxKey()
	iterator, _ := fst.Iterator(minKey, append(maxKey, []byte("#")...))
	idxMmap, err := utils.NewMmap("./data/gk_1001/title_invert.idx", utils.MODE_APPEND)
	if err != nil {
		panic(err)
	}
	idxMmap.SetFileEnd(0)
	for err == nil {
		key, offset := iterator.Current()
		fmt.Printf("key: %v, offset: %v\n", string(key), offset)
		lens := idxMmap.ReadInt64(int64(offset))
		res := idxMmap.ReadDocIdsArry(uint64(offset)+8, uint64(lens))
		fmt.Println(res)
		err = iterator.Next()
	}
}

func TestSearch(t *testing.T) {
	utils.GetDocIDsChan, utils.GiveDocIDsChan = utils.DocIdsMaker()
	//utils.GSegmenter = utils.NewSegmenter("/home/hz/GoProject/GoDanceEngine/GoDance/test/dictionary/dict.txt")
	logger, err := utils.NewLogger("GDEngine")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := gdindex.NewIndexFromLocalFile("gk", utils.IDX_ROOT_PATH, logger)

	q1 := utils.SearchQuery{
		FieldName: "title",
		Value:     "伟大",
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
		fmt.Println(len(ids), ids)
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

}

func TestSearchFilterDocIds(t *testing.T) {
	logger, err := utils.NewLogger("GDEngine")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := gdindex.NewIndexFromLocalFile("gk", utils.IDX_ROOT_PATH, logger)

	filters := utils.SearchFilters{FieldName: "year", Start: 1977, End: 2000, Type: utils.FILT_RANGE}
	ids, b := index.SearchFilterDocIds(filters)
	if !b {
		fmt.Println("查询失败")
	}
	for _, id := range ids {
		document, ok := index.GetDocument(id)
		if ok {
			fmt.Println(document)
		} else {
			fmt.Println("nil")
		}
	}
}

func TestMergeSegment(t *testing.T) {
	logger, err := utils.NewLogger("FalconSearcher")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}
	index := gdindex.NewIndexFromLocalFile("gk", utils.IDX_ROOT_PATH, logger)

	index.MergeSegments()

}
