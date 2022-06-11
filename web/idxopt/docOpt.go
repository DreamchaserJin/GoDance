package idxopt

import (
	"GoDance/engine"
	"github.com/gin-gonic/gin"
	"net/http"
)

// AddDocument
// @Description 新增文档
func AddDocument() func(c *gin.Context) {
	return func(c *gin.Context) {
		data, _ := c.GetRawData()
		indexName := c.Query("index")

		params := make(map[string]string)
		params["index"] = indexName
		msg, err := engine.Engine.AddDocument(params, data)
		if err == nil {
			c.JSON(http.StatusOK, msg)
		} else {
			c.JSON(http.StatusBadRequest, msg)
		}
	}
}

// DeleteDocument
// @Description 删除文档
func DeleteDocument() func(c *gin.Context) {
	return func(c *gin.Context) {
		indexName := c.Query("index")
		params := make(map[string]string)
		params["index"] = indexName

		msg, err := engine.Engine.DeleteDocument(params)

		if err == nil {
			c.JSON(http.StatusOK, msg)
		} else {
			c.JSON(http.StatusBadRequest, msg)
		}
	}
}

// UpdateDocument
// @Description 修改文档
func UpdateDocument() func(c *gin.Context) {
	return func(c *gin.Context) {
		data, _ := c.GetRawData()
		indexName := c.Query("index")
		params := make(map[string]string)
		params["index"] = indexName

		msg, err := engine.Engine.UpdateDocument(params, data)

		if err == nil {
			c.JSON(http.StatusOK, msg)
		} else {
			c.JSON(http.StatusBadRequest, msg)
		}
	}
}
