package utils

import (
	"math"
	"os"
	"time"
)

// IDX_ROOT_PATH 默认索引存放位置
const IDX_ROOT_PATH string = "./data/"
const TRIE_PATH string = "./data/trieTree.tr"

// FALCONENGINENAME base名称
const GODANCEENGINE string = "GoDanceEngine"
const MAX_SEGMENT_SIZE uint64 = 100000

// 停用词文件
// const STOP_WORD_FILE_PATH = "utils/stopWords.txt"

const STOP_WORD_FILE_PATH = "utils/stopWord-CN.txt"

var DocIdChan chan []DocIdNode

type DocIdNode struct {
	Docid  uint64
	WordTF float64
}

type DocIdSort []DocIdNode

func (a DocIdSort) Len() int      { return len(a) }
func (a DocIdSort) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a DocIdSort) Less(i, j int) bool {
	if a[i] == a[j] {
		return a[i].Docid < a[j].Docid
	}
	return a[i].Docid < a[j].Docid
}

type DocWeightSort []DocIdNode

func (a DocWeightSort) Len() int      { return len(a) }
func (a DocWeightSort) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a DocWeightSort) Less(i, j int) bool {
	if a[i] == a[j] {
		return CompareFloat64(a[i].WordTF, a[j].WordTF) < 0
	}
	return CompareFloat64(a[i].WordTF, a[j].WordTF) < 0
}

// 协调因子
type CoordWeight struct {
	DocId  uint64
	Weight float64
}
type CoordWeightSort []CoordWeight

func (cw CoordWeightSort) Len() int { return len(cw) }
func (cw CoordWeightSort) Less(i, j int) bool {
	if cw[i].Weight > cw[j].Weight {
		return true
	} else if cw[i].Weight == cw[j].Weight {
		return cw[i].DocId < cw[j].DocId
	} else {
		return false
	}
}
func (cw CoordWeightSort) Swap(i, j int) { cw[i], cw[j] = cw[j], cw[i] }

//
//  CompareFloat64
//  @Description: 比较两个float64类型的数。如果两值之差小于10的-15次方，则会认为它们相等。如果a大于b,则返回1。a小于b,则返回-1。
//  @param a
//  @param b
//  @return int
//
func CompareFloat64(a, b float64) int {
	if math.Abs(a-b) < 1e-15 {
		return 0
	}
	if math.Max(a, b) == a {
		return 1
	} else {
		return -1
	}
}

const DOCNODE_SIZE int = 16

// 索引类型说明
const (
	IDX_TYPE_STRING     = 1 //字符型索引[全词匹配]
	IDX_TYPE_STRING_SEG = 2 //字符型索引[切词匹配，全文索引,hash存储倒排]

	IDX_TYPE_NUMBER = 11 // 数字型索引，只支持整数，数字型索引只建立正排
	IDX_TYPE_FLOAT  = 12 // 数字型索引，支持浮点数，只能保留两位小数，数字型索引只建立正排

	IDX_TYPE_DATE = 15 // 日期型索引 '2015-11-11 00:11:12'，日期型只建立正排，转成时间戳存储

	IDX_TYPE_PK = 21 //主键类型，倒排正排都需要，倒排使用B+树存储

	IDX_TYPE_DESC = 31 // 只存储不索引的类型
)

// 过滤类型，对应filtertype
const (
	FILT_EQ    uint64 = 1 //等于
	FILT_OVER  uint64 = 2 //大于
	FILT_LESS  uint64 = 3 //小于
	FILT_RANGE uint64 = 4 //范围内
)

type TermInfo struct {
	Term string
	Tf   int
}

/*************************************************************************
索引查询接口
索引查询分为 查询和过滤
查询：倒排索引匹配
过滤：正排索引过滤
************************************************************************/
// FSSearchQuery function description : 查询接口数据结构[用于倒排索引查询]，内部都是求交集
type SearchQuery struct {
	FieldName string `json:"_field"`
	Value     string `json:"_value"`
}

// FSSearchFilted function description : 过滤接口数据结构，内部都是求交集
type SearchFilters struct {
	FieldName string  `json:"_field"`
	Start     int64   `json:"_start"`
	End       int64   `json:"_end"`
	Range     []int64 `json:"_range"`
	Type      uint64  `json:"_type"`
}

// DefaultResult
// @Description: 返回给Web层的 Json
type DefaultResult struct {
	TotalCount int64               `json:"totalCount"`
	From       int64               `json:"from"`
	To         int64               `json:"to"`
	Status     string              `json:"status"`
	CostTime   string              `json:"costTime"`
	Results    []map[string]string `json:"results"`
}

func Exist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

// IsDateTime function description : 判断是否是日期时间格式
// params : 字符串
// return : 是否是日期时间格式
func IsDateTime(datetime string) (int64, error) {

	var timestamp time.Time
	var err error

	if len(datetime) > 16 {
		timestamp, err = time.ParseInLocation("2006-01-02 15:04:05", datetime, time.Local)
		if err != nil {
			return -1, err
		}
	} else if len(datetime) > 10 {
		timestamp, err = time.ParseInLocation("2006-01-02 15:04", datetime, time.Local)
		if err != nil {
			return -1, err
		}
	} else {
		timestamp, err = time.ParseInLocation("2006-01-02", datetime, time.Local)
		if err != nil {
			return -1, err
		}
	}

	return timestamp.Unix(), nil

}

func FormatDateTime(timestamp int64) (string, bool) {

	if timestamp == 0 {
		return "", false
	}
	tm := time.Unix(timestamp, 0)
	return tm.Format("2006-01-02"), true

}

func DocIdNodeChangeUint64(docIdNode []DocIdNode) []uint64 {
	changeUint64 := make([]uint64, len(docIdNode))
	for i := range docIdNode {
		changeUint64[i] = docIdNode[i].Docid
	}
	return changeUint64

}

// FileExist 判断文件是否存在，如果存在返回true，否则返回false
func FileExist(path string) bool {
	_, err := os.Lstat(path)
	return !os.IsNotExist(err)
}
