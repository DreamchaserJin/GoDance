package engine

import (
	gdindex "GoDance/index"
	"GoDance/index/segment"
	"GoDance/search/boolea"
	"GoDance/search/related"
	"GoDance/search/weight"
	"GoDance/utils"
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
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

// 一些返回的错误常量
const (
	MethodError    string = "提交方式错误,请查看提交方式是否正确"
	ParamsError    string = "参数错误"
	JsonParseError string = "JSON格式解析错误"
	NoPrimaryKey   string = "没有主键"
	QueryError     string = "查询条件有问题，请检查查询条件"
	IndexNotFound  string = "未找到对应的索引"
	OK             string = `"status":"OK"`
	NotFound       string = `"status":"NotFound"`
	Fail           string = `"status":"Fail"`
)

// NewDefaultEngine
// @Description 初始化引擎
// @Param logger 日志
// @Return *GoDanceEngine 引擎对象
func NewDefaultEngine(logger *utils.Log4FE) *GoDanceEngine {

	this := &GoDanceEngine{Logger: logger, idxManager: newIndexManager(logger), trie: related.Constructor(utils.TRIE_PATH)}
	return this
}

// CreateIndex
// @Description 创建一个新索引
// @Param params 请求参数
// @Param body  请求体 Json格式
// @Return error 任何错误
func (gde *GoDanceEngine) CreateIndex(indexName string, body []byte) error {

	var idx IndexStruct
	err := json.Unmarshal(body, &idx)
	// fmt.Println(idx)
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
// @Param indexName 索引名
// @Param field  字段信息
// @Return error  任何错误
func (gde *GoDanceEngine) AddField(indexName string, field segment.SimpleFieldInfo) error {
	return gde.idxManager.AddField(indexName, field)
}

// DeleteField
// @Description 给某个索引删除字段
// @Param indexName 索引名
// @Param fieldName 字段名
// @Return error  任何错误
func (gde *GoDanceEngine) DeleteField(indexName string, fieldName string) error {
	return gde.idxManager.DeleteField(indexName, fieldName)
}

func (gde *GoDanceEngine) AddDocument(params map[string]string, body []byte) (string, error) {
	indexName, hasIndex := params["index"]
	if !hasIndex || indexName == "" {
		return Fail, errors.New(ParamsError)
	}
	document := make(map[string]string)
	err := json.Unmarshal(body, &document)
	if err != nil {
		gde.Logger.Error("[ERROR] Parse JSON Fail : %v ", err)
		return "", errors.New(JsonParseError)
	}

	return gde.idxManager.addDocument(indexName, document)
}

func (gde *GoDanceEngine) DeleteDocument(params map[string]string) (string, error) {
	indexName, hasIndex := params["index"]
	if !hasIndex || indexName == "" {
		return Fail, errors.New(ParamsError)
	}

	pk, haspk := params["_pk"]
	if !haspk {
		return Fail, errors.New(NoPrimaryKey)
	}

	return gde.idxManager.deleteDocument(indexName, pk)
}
func (gde *GoDanceEngine) UpdateDocument(params map[string]string, body []byte) (string, error) {
	indexName, hasIndex := params["index"]
	if !hasIndex || indexName == "" {
		return Fail, errors.New(ParamsError)
	}
	document := make(map[string]string)
	err := json.Unmarshal(body, &document)
	if err != nil {
		gde.Logger.Error("[ERROR] Parse JSON Fail : %v ", err)
		return Fail, errors.New(JsonParseError)
	}

	return gde.idxManager.updateDocument(indexName, document)
}

// RealTimeSearch
// @Description 实时搜索返回内容<=10
// @Param key  关键词
// @Return string  json格式的返回结果
// @Return error 任何错误
func (gde *GoDanceEngine) RealTimeSearch(key string) []string {

	search := gde.trie.Search(key, true)
	return search
}

// RelatedSearch
// @Description 相关搜索返回10个内容
// @Param key  关键词
// @Return string  json格式的返回结果
// @Return error 任何错误
func (gde *GoDanceEngine) RelatedSearch(key string) []string {

	search := gde.trie.Search(key, false)
	return search
}

// Search
// @Description 搜索文档并返回
// @Param params 请求参数
// @Return string 搜索相关的Json字符串
// @Return error
func (gde *GoDanceEngine) Search(params map[string]string) (utils.DefaultResult, error) {

	startTime := time.Now()
	indexName, hasIndex := params["index"]
	pageSize, hasPageSize := params["pageSize"]
	curPage, hasCurPage := params["curPage"]

	var resultSet utils.DefaultResult

	if !hasIndex || !hasPageSize || !hasCurPage {
		return resultSet, errors.New(ParamsError)
	}

	//获取索引
	idx := gde.idxManager.GetIndex(indexName)
	if idx == nil {
		return resultSet, errors.New(IndexNotFound)
	}

	// 建立过滤条件和搜索条件
	searchFilters, searchQueries, notSearchQueries := gde.parseParams(params, idx)
	QueryNodes := make([]utils.DocIdNode, 0)
	docFilterIds := make([]uint64, 0)
	notDocQueryNodes := make([]utils.DocIdNode, 0)

	// 对每个 搜索词分词后的文档 求并集
	for _, query := range searchQueries {
		ids, ok := idx.SearchKeyDocIds(query)
		if ok {
			QueryNodes = boolea.UnionDocIdNode(QueryNodes, ids)
		}
		// fmt.Printf("ids res : %v\n", ids)
	}
	fmt.Printf("query res : %v\n", QueryNodes)
	//fmt.Printf("query: %v\n", len(QueryNodes))

	// 对每个 筛选词文档 求交集
	if len(searchFilters) == 1 {
		docFilterIds, _ = idx.SearchFilterDocIds(searchFilters[0])
	} else {
		for _, filter := range searchFilters {
			var ids []uint64
			var ok bool

			ids, ok = idx.SearchFilterDocIds(filter)
			if ok {
				docFilterIds = boolea.IntersectionUint64(docFilterIds, ids)
			}
		}
	}

	fmt.Printf("filter: %v\n", docFilterIds)

	// 对每个过滤词 求并集
	for _, query := range notSearchQueries {
		ids, ok := idx.SearchKeyDocIds(query)
		if ok {
			notDocQueryNodes = boolea.UnionDocIdNode(notDocQueryNodes, ids)
		}
	}
	fmt.Printf("notsearch: %v\n", notDocQueryNodes)

	// 对 搜索词和筛选词文档求交集，再对过滤词文档 NOT
	// 搜索词跟筛选词求交集
	var keyFilter []uint64
	if len(searchFilters) != 0 {
		keyFilter = boolea.IntersectionDocIdAndUint64(QueryNodes, docFilterIds)
	} else {
		keyFilter = utils.DocIdNodeChangeUint64(QueryNodes)
	}
	// not
	docAndNot := boolea.DocAndNot(keyFilter, notDocQueryNodes)
	fmt.Printf("keyFilter : %v\n", keyFilter)
	fmt.Printf("merge : %v\n", docAndNot)

	// 对 docMergeFilter 的所有文档进行权重排序
	docWeightSort := DocWeightSort(docAndNot, notDocQueryNodes, searchQueries, idx, docFilterIds)

	lens := int64(len(docWeightSort))
	fmt.Printf("lens : %v\n", lens)
	fmt.Printf("docWeightSort:", docWeightSort)

	if lens == 0 {
		return resultSet, nil
	}

	//计算起始和终止位置
	start, end, err := gde.calcStartEnd(pageSize, curPage, lens)

	if err != nil {
		return resultSet, nil
	}

	resultSet.Results = make([]map[string]string, 0)
	for _, docId := range docWeightSort[start:end] {
		doc, ok := idx.GetDocument(docId)
		doc["id"] = fmt.Sprintf("%v", docId)
		// fmt.Println(doc)
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

	// fmt.Println(resultSet.TotalCount)

	if err != nil {
		return resultSet, err
	}

	return resultSet, nil
}

// calcStartEnd
// @Description 计算分页
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

	// fmt.Println(curPage, pageSize)

	start := pageSize * (curPage - 1)
	end := pageSize * curPage

	// fmt.Println(start, end)

	if start >= docSize {
		return 0, 0, fmt.Errorf("out page")
	}

	if end > docSize {
		end = docSize
	}

	return start, end, nil
}

// parseParams
// @Description 根据请求参数生成对应的搜索条件和过滤条件
func (gde *GoDanceEngine) parseParams(params map[string]string, idx *gdindex.Index) ([]utils.SearchFilters, []utils.SearchQuery, []utils.SearchQuery) {

	searchFilters := make([]utils.SearchFilters, 0)
	searchQueries := make([]utils.SearchQuery, 0)
	notSearchQueries := make([]utils.SearchQuery, 0)

	fmt.Println(params)
	// 打开要写入的文件
	trieFd, err := os.OpenFile(utils.TRIE_PATH, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	defer trieFd.Close()
	writer := bufio.NewWriter(trieFd)
	defer writer.Flush()
	if err != nil {
		return nil, nil, nil
	}
	var isInsert = false
	insertNum := 100
	insertWords := make([]string, insertNum)

	for param, value := range params {

		// todo 还有一些其余的请求参数
		if param == "index" || param == "pageSize" || param == "curPage" {
			continue
		}

		switch param[0] {
		case '-':
			eqValue, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				continue
			}
			sf := utils.SearchFilters{FieldName: param[1:], Type: utils.FILT_EQ, Start: eqValue}
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
			if value != "" {
				query.Value = value
				notSearchQueries = append(notSearchQueries, query)
			}

		default:
			segmenter := utils.GetGseSegmenter()
			var terms = make([]string, 0)

			// value 加进 Trie 树
			if !isInsert {
				gde.trie.Insert(value)
				isInsert = true
			}

			// 将value写入TriePath的文件中，个数到达insertNum再一起写入
			insertNum--
			if insertNum <= 0 {
				for _, val := range insertWords {
					_, err2 := writer.WriteString(val + "\n")
					if err2 != nil {
						return nil, nil, nil
					}
				}
				insertNum = 100
			} else {
				insertWords[100-insertNum] = value
			}
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

	fmt.Printf("query:%v\n", searchQueries)
	fmt.Printf("filter:%v\n", searchFilters)
	fmt.Printf("notsearch:%v\n", notSearchQueries)

	return searchFilters, searchQueries, notSearchQueries
}

func (gde *GoDanceEngine) GetDocById(indexName, id string) (map[string]string, error) {

	idx := gde.idxManager.GetIndex(indexName)
	if idx == nil {
		return nil, errors.New("index not found")
	}

	docId, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, errors.New("docId error")
	}
	document, ok := idx.GetDocument(uint64(docId))
	if !ok {
		return nil, errors.New("doc not found")
	}
	return document, nil

}

//
//  DocWeightSort
//  @Description: 将文档按照权重进行排序
//  @param docMergeFilter 布尔模型过滤后的相关文档
//  @param searchQueries  关键词集合
//  @param idx  倒排
//  @return []uint64
//
func DocWeightSort(docMergeFilter []uint64, notDocQueryNodes []utils.DocIdNode, searchQueries []utils.SearchQuery,
	idx *gdindex.Index, docFilterIds []uint64) []uint64 {

	// IDF -> TFIDF -> 空间向量模型 -> 协调因子 -> 排序好的文档
	docLen := len(docMergeFilter)
	if docLen == 0 {
		return nil
	}
	keyLen := len(searchQueries)
	docNum := float64(docLen)
	// 搜索词向量权重
	vectorKey := make([]float64, keyLen)
	// 所有文档向量
	vectorAllDoc := make(map[uint64][]float64, 0)
	// 初始化类似于二维数组
	for _, id := range docMergeFilter {
		vectorAllDoc[id] = make([]float64, keyLen)
	}
	//协调因子 : 计算文档里出现的查询词个数 / 总个数
	coord := make(map[uint64]float64, 0)

	// 过滤词映射,过滤ids
	notMap := make(map[uint64]bool, 0)
	for _, v := range notDocQueryNodes {
		notMap[v.Docid] = true
	}

	// 向量空间模型
	for index, query := range searchQueries {
		ids, ok := idx.SearchKeyDocIds(query)
		if ok {
			// 过滤ids
			if len(docFilterIds) != 0 {
				ids = boolea.Intersection2DocIdAndUint64(ids, docFilterIds)
			}
			if len(notMap) != 0 {
				for i := 0; i < len(ids); {
					if notMap[ids[i].Docid] == true {
						// 删除过滤的文档
						ids = append(ids[:i], ids[i+1:]...)
					} else {
						i++
					}
				}
			}
			// query.Value 对应的idf
			idf := math.Log(docNum/float64(len(ids)+1)) + 1
			var maxTFIDF float64
			for i := range ids {
				var TFIDF float64
				if query.FieldName == "title" {
					TFIDF = ids[i].WordTF * idf * weight.TITLEBOOST
				} else {
					TFIDF = ids[i].WordTF * idf
				}
				maxTFIDF = math.Max(maxTFIDF, TFIDF)
				if cap(vectorAllDoc[ids[i].Docid]) != 0 {
					vectorAllDoc[ids[i].Docid][index] = TFIDF
				}

				coord[ids[i].Docid] += 1 / float64(keyLen)
			}
			vectorKey[index] = maxTFIDF
		}
	}
	//fmt.Println("vK:", vectorKey)
	//fmt.Println("vad:", vectorAllDoc)
	docVectorWeight := weight.DocVectorWeight(vectorKey, vectorAllDoc)
	//fmt.Println("docvw : ", docVectorWeight)

	//fmt.Println("coord:", coord)
	// 协调因子乘向量权重后排序
	var coordWeights utils.CoordWeightSort
	for k, v := range docVectorWeight {
		var cw utils.CoordWeight
		cw.DocId = k
		cw.Weight = v * coord[k]
		coordWeights = append(coordWeights, cw)
	}
	sort.Sort(coordWeights)
	fmt.Println(coordWeights)
	// 排序后去掉权重只返回文档id
	docWeightSort := make([]uint64, 0)
	for _, v := range coordWeights {
		docWeightSort = append(docWeightSort, v.DocId)
	}
	return docWeightSort
}
