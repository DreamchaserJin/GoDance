package engine

import (
	gdindex "GoDance/index"
	"GoDance/index/segment"
	"GoDance/search/boolea"
	"GoDance/search/related"
	"GoDance/utils"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var Engine *GoDanceEngine

type GoDanceEngine struct {
	idxManager *IndexManager
	LocalIP    string        // 本地IP
	LocalPort  int           // 本地端口号
	MasterIP   string        // 主节点 IP
	MasterPort int           // 主节点端口号
	Logger     *utils.Log4FE `json:"-"`
	trie       related.Trie
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

// 一些返回的错误常量
const (
	MethodError    string = "提交方式错误,请查看提交方式是否正确"
	ParamsError    string = "参数错误"
	JsonParseError string = "JSON格式解析错误"
	NoPrimaryKey   string = "没有主键"
	QueryError     string = "查询条件有问题，请检查查询条件"
	IndexNotFound  string = "未找到对应的索引"
	OK             string = `{"status":"OK"}`
	NotFound       string = `{"status":"NotFound"}`
	Fail           string = `{"status":"Fail"}`
)

func NewDefaultEngine(logger *utils.Log4FE) *GoDanceEngine {
	this := &GoDanceEngine{Logger: logger, idxManager: newIndexManager(logger), trie: related.Constructor()}
	return this
}

// CreateIndex
// @Description 创建新的索引
func (gde *GoDanceEngine) CreateIndex(params map[string]string, body []byte) error {

	indexName, hasindex := params["index"]
	if !hasindex {
		return errors.New(ParamsError)
	}

	var idx IndexStruct
	err := json.Unmarshal(body, &idx)
	if err != nil {
		gde.Logger.Error("[ERROR]  %v : %v ", JsonParseError, err)
		return errors.New(JsonParseError)
	}

	return gde.idxManager.CreateIndex(indexName, idx.FieldsMapping)
}

// DeleteIndex todo 删除索引
// @Description
func (gde *GoDanceEngine) DeleteIndex() {

}

// AddField
// @Description 给某个索引新增字段
// @Param indexName
// @Param field
// @Return error
func (gde *GoDanceEngine) AddField(indexName string, field segment.SimpleFieldInfo) error {

	return gde.idxManager.AddField(indexName, field)

}

// DeleteField
// @Description 给某个索引删除字段
// @Param indexName
// @Param fieldName
// @Return error
func (gde *GoDanceEngine) DeleteField(indexName string, fieldName string) error {

	return gde.idxManager.DeleteField(indexName, fieldName)

}

// DocumentOptions
// @Description 根据 HTTP 请求类型来判断进行 增删改
func (gde *GoDanceEngine) DocumentOptions(method string, params map[string]string, body []byte) (string, error) {

	indexName, hasIndex := params["index"]
	if !hasIndex {
		return Fail, errors.New(ParamsError)
	}

	switch method {
	case "POST":
		document := make(map[string]string)
		err := json.Unmarshal(body, &document)
		if err != nil {
			gde.Logger.Error("[ERROR] Parse JSON Fail : %v ", err)
			return "", errors.New(JsonParseError)
		}

		return gde.idxManager.addDocument(indexName, document)
	case "DELETE":
		pk, haspk := params["_pk"]
		if !haspk {
			return Fail, errors.New(NoPrimaryKey)
		}

		return gde.idxManager.deleteDocument(indexName, pk)
	case "PUT":

		document := make(map[string]string)
		err := json.Unmarshal(body, &document)
		if err != nil {
			gde.Logger.Error("[ERROR] Parse JSON Fail : %v ", err)
			return Fail, errors.New(JsonParseError)
		}

		return gde.idxManager.updateDocument(indexName, document)

	default:
		return Fail, errors.New(ParamsError)
	}
}

// RelatedSearch
// @Description 实时搜索返回内容<=10
// @Param key  关键词
// @Return string  json格式的返回结果
// @Return error 任何错误
func (gde *GoDanceEngine) RealTimeSearch(key string) (string, error) {

	search := gde.trie.Search(key, true)
	results, err := json.Marshal(search)
	if err == nil {
		return string(results), err
	}

	return "", nil
}

// RelatedSearch
// @Description 相关搜索返回10个内容
// @Param key  关键词
// @Return string  json格式的返回结果
// @Return error 任何错误
func (gde *GoDanceEngine) RelatedSearch(key string) (string, error) {

	search := gde.trie.Search(key, false)
	results, err := json.Marshal(search)
	if err == nil {
		return string(results), err
	}

	return "", nil
}

// Search todo 搜索
// @Description
func (gde *GoDanceEngine) Search(params map[string]string) (string, error) {

	startTime := time.Now()
	indexName, hasIndex := params["index"]
	pageSize, hasPageSize := params["pageSize"]
	curPage, hasCurPage := params["curPage"]

	if !hasIndex || !hasPageSize || !hasCurPage {
		return Fail, errors.New(ParamsError)
	}

	//获取索引
	idx := gde.idxManager.GetIndex(indexName)
	if idx == nil {
		return NotFound, errors.New(IndexNotFound)
	}

	// 建立过滤条件和搜索条件
	searchFilters, searchQueries, notSearchQueries := gde.parseParams(params, idx)
	docQueryNodes := make([]utils.DocIdNode, 0)
	docFilterIds := make([]uint64, 0)
	notDocQueryNodes := make([]utils.DocIdNode, 0)

	// todo 对每个 ids 求交集
	for _, query := range searchQueries {
		ids, ok := idx.SearchKeyDocIds(query)
		if ok {
			docQueryNodes = append(docQueryNodes, ids...)
		}
	}

	// todo 对每个 Ids 求交集
	for _, filter := range searchFilters {
		ids, ok := idx.SearchFilterDocIds(filter)
		if ok {
			docFilterIds = append(docFilterIds, ids...)
		}
	}

	// todo 需要建立一个关键词过滤的集合
	for _, query := range notSearchQueries {
		ids, ok := idx.SearchKeyDocIds(query)
		if ok {
			notDocQueryNodes = append(notDocQueryNodes, ids...)
		}
	}

	// todo 对 docQueryNodes 和 docFilterIds求交集, 注意类型 []DocIdNode 和 []uint64
	// 使用 bool模型汇总
	docMergeFilter := boolea.DocMergeFilter(docQueryNodes, docFilterIds, notDocQueryNodes)

	lens := int64(len(docMergeFilter))
	if lens == 0 {
		return NotFound, nil
	}

	//计算起始和终止位置
	start, end, err := gde.calcStartEnd(pageSize, curPage, lens)
	if err != nil {
		return NotFound, nil
	}

	var resultSet DefaultResult

	resultSet.Results = make([]map[string]string, 0)
	for _, docId := range docMergeFilter[start:end] {
		doc, ok := idx.GetDocument(docId)
		if ok {
			resultSet.Results = append(resultSet.Results, doc)
		}
	}

	resultSet.From = start + 1
	resultSet.To = end
	resultSet.Status = "OK"
	resultSet.TotalCount = lens
	endTime := time.Now()
	resultSet.CostTime = fmt.Sprintf("%v", endTime.Sub(startTime))

	r, err := json.Marshal(resultSet)
	if err != nil {
		return NotFound, err
	}

	return string(r), nil
}

func (gde *GoDanceEngine) calcStartEnd(ps, cp string, docSize int64) (int64, int64, error) {

	pageSize, ok1 := strconv.ParseInt(ps, 0, 0)
	curPage, ok2 := strconv.ParseInt(cp, 0, 0)
	if ok1 != nil || ok2 != nil {
		pageSize = 10
		curPage = 1
	}

	if pageSize <= 0 {
		pageSize = 10
	}

	if curPage <= 0 {
		curPage = 1
	}

	start := curPage * (pageSize - 1)
	end := curPage * pageSize

	if start >= docSize {
		return 0, 0, fmt.Errorf("out page")
	}

	if end > docSize {
		end = docSize
	}

	return start, end, nil
}

func (gde *GoDanceEngine) parseParams(params map[string]string, idx *gdindex.Index) ([]utils.SearchFilters, []utils.SearchQuery, []utils.SearchQuery) {

	searchFilters := make([]utils.SearchFilters, 0)
	searchQueries := make([]utils.SearchQuery, 0)
	notSearchQueries := make([]utils.SearchQuery, 0)
	for param, value := range params {

		// todo 还有一些其余的请求参数
		if param == "index" || param == "pageSize" || param == "curSize" {
			continue
		}

		switch param[0] {
		case '-':
			eqValues := strings.Split(value, ",")
			sf := utils.SearchFilters{FieldName: param[1:], Type: utils.FILT_EQ, Range: make([]int64, 0)}
			for _, v := range eqValues {
				if valueNum, err := strconv.ParseInt(v, 10, 64); err == nil {
					sf.Range = append(sf.Range, valueNum)
				}
			}
			searchFilters = append(searchFilters, sf)
		case '>':
			overValue, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				continue
			}

			sf := utils.SearchFilters{FieldName: param[1:], Type: utils.FILT_OVER, Start: overValue}
			searchFilters = append(searchFilters, sf)
		case '<':
			lessValue, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				continue
			}
			sf := utils.SearchFilters{FieldName: param[1:], Type: utils.FILT_LESS, Start: lessValue}
			searchFilters = append(searchFilters, sf)
		case '~':
			minMax := strings.Split(value, ",")
			if len(minMax) != 2 {
				continue
			}
			start, err1 := strconv.ParseInt(minMax[0], 10, 64)
			if err1 != nil {
				continue
			}
			end, err2 := strconv.ParseInt(minMax[1], 10, 64)
			if err2 != nil {
				continue
			}
			searchFilters = append(searchFilters, utils.SearchFilters{FieldName: param[1:],
				Type: utils.FILT_RANGE, Start: start, End: end})
		case '_': // 关键词过滤 比如  _content : 南昌  就是过滤content字段中有南昌的
			var query utils.SearchQuery
			// 针对某个字段名的过滤
			query.FieldName = param[1:]
			query.Value = value
			notSearchQueries = append(searchQueries, query)

		default:
			segmenter := utils.GetGseSegmenter()
			var terms = make([]string, 0)

			// todo value 加进 Trie 树
			gde.trie.Insert(value)

			fieldType, ok := idx.Fields[param]
			if ok {
				switch fieldType {
				case utils.IDX_TYPE_STRING:
					terms = append(terms, value)
				case utils.IDX_TYPE_STRING_SEG:
					terms = segmenter.CutSearch(value, false)
				}
			}

			for _, term := range terms {
				var query utils.SearchQuery
				query.FieldName = param
				query.Value = term
				searchQueries = append(searchQueries, query)
			}
		}
	}

	return searchFilters, searchQueries, notSearchQueries
}
