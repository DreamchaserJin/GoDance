package engine

import (
	"GoDance/index/segment"
	"GoDance/utils"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type GoDanceEngine struct {
	idxManager *IndexManager
	LocalIP    string        // 本地IP
	LocalPort  int           // 本地端口号
	MasterIP   string        // 主节点 IP
	MasterPort int           // 主节点端口号
	Logger     *utils.Log4FE `json:"-"`
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
	MethodError        string = "提交方式错误,请查看提交方式是否正确"
	ParamsError        string = "参数错误"
	JsonParseError     string = "JSON格式解析错误"
	NoPrimaryKey       string = "没有主键"
	ProcessorBusyError string = "处理进程繁忙，请稍候提交"
	QueryError         string = "查询条件有问题，请检查查询条件"
	IndexNotFound      string = "未找到对应的索引"
	OK                 string = `{"status":"OK"}`
	NotFound           string = `{"status":"NotFound"}`
	Fail               string = `{"status":"Fail"}`
)

func NewDefaultEngine(logger *utils.Log4FE) *GoDanceEngine {
	this := &GoDanceEngine{Logger: logger, idxManager: newIndexManager(logger)}
	return this
}

// CreateIndex
// @Description 创建新的索引
func (gde *GoDanceEngine) CreateIndex(params map[string]string, body []byte) error {

	indexName, hasindex := params["index"]
	if !hasindex {
		return errors.New(ParamsError)
	}

	var idx utils.IndexStruct
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

// Search todo 搜索
// @Description
func (gde *GoDanceEngine) Search(params map[string]string) (string, error) {

	startTime := time.Now()
	indexName, hasIndex := params["index"]
	pageSize, hasPageSize := params["pageSize"]
	curPage, hasCurPage := params["curPage"]

	sortMethod, hasSort := params["sort"]

	if !hasIndex || !hasPageSize || !hasCurPage {
		return Fail, errors.New(ParamsError)
	}

	//获取索引
	indexer := gde.idxManager.GetIndex(indexName)
	if indexer == nil {
		return NotFound, errors.New(IndexNotFound)
	}

	// 建立过滤条件
	searchfilters := gde.parseFilted(parms, indexer)

	for field, value := range params {

	}

	docNodes := make([]utils.DocIdNode, 0)

	lens := int64(len(docNodes))
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
	for _, docNode := range docNodes[start:end] {
		doc, ok := indexer.GetDocument(docNode.Docid)
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

func (gde *GoDanceEngine) parseFilter(params map[string]string) []utils.SearchFilters {

	searchFilters := make([]utils.SearchFilters, 0)
	for param, value := range params {
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
			overValue := value
			sf := utils.SearchFilters{FieldName: param[1:], Type: utils.FILT_OVER, Start: overValue}
			for _, v := range eqValues {
				if valueNum, err := strconv.ParseInt(v, 10, 64); err == nil {
					sf.Range = append(sf.Range, valueNum)
				}
			}
			searchFilters = append(searchFilters, sf)
		case '<':

		case '~':

		}
	}

	return searchFilters
}
