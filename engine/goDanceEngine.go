package engine

import (
	"GoDance/index/segment"
	"GoDance/utils"
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

func NewDefaultEngine(logger *utils.Log4FE) *GoDanceEngine {
	this := &GoDanceEngine{Logger: logger, idxManager: newIndexManager(logger)}
	return this
}

// CreateIndex todo 创建新的索引
// @Description
func (gde *GoDanceEngine) CreateIndex() {

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

// DocumentOptions todo 根据 HTTP 请求类型来判断进行 增删改
// @Description
func (gde *GoDanceEngine) DocumentOptions(method string) {

	switch method {
	case "POST":
		gde.addDocument()
		return

	case "DELETE":
		gde.deleteDocument()
		return

	case "PUT":
		gde.updateDocument()
		return
	}

}

// addDocument todo 新增文档
// @Description
func (gde *GoDanceEngine) addDocument() {

}

// updateDocument todo 修改文档
// @Description
func (gde *GoDanceEngine) updateDocument() {

}

// deleteDocument todo 删除文档
// @Description
func (gde *GoDanceEngine) deleteDocument() {

}

// Search todo 搜索
// @Description
func (gde *GoDanceEngine) Search() {

}
