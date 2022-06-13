package web

import (
	"GoDance/web/idxopt"
	"GoDance/web/websearch"
	"github.com/gin-gonic/gin"
)

// Register
// @Description 注册 API 服务
func Register(r *gin.Engine) {

	// 对索引的操作
	r.POST("/create", idxopt.CreateIndex())

	// 对文档的操作
	r.POST("/update", idxopt.AddDocument())
	r.DELETE("/update", idxopt.DeleteDocument())
	r.PUT("/update", idxopt.UpdateDocument())

	// 搜索相关的API
	r.GET("/search_related", websearch.GetRelated())
	r.GET("/search_result", websearch.GetResult())

	// 获取文档
	r.POST("/get_doc", websearch.GetDocument())
}
