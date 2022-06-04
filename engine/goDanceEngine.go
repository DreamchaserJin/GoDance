package engine

import (
	"GoDance/index/segment"
	"GoDance/utils"
	"encoding/json"
	"errors"
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
func (gde *GoDanceEngine) Search(params map[string]string) {

}
