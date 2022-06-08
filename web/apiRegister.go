package web

import (
	"GoDance/web/idxopt"
	"github.com/gin-gonic/gin"
)

func Register(r *gin.Engine) {

	// 对索引的操作
	r.POST("/create", idxopt.CreateIndex())

	// 对文档的操作
	r.POST("/update", idxopt.AddDocument())
	r.DELETE("/update", idxopt.DeleteDocument())
	r.PUT("/update", idxopt.UpdateDocument())

}
