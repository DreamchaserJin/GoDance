package idxopt

import (
	"GoDance/engine"
	"github.com/gin-gonic/gin"
	"net/http"
)

func AddDocument() func(c *gin.Context) {
	return func(c *gin.Context) {
		data, _ := c.GetRawData()
		indexName := c.Query("index")

		params := make(map[string]string)
		params["index"] = indexName
		msg, _ := engine.Engine.AddDocument(params, data)

		c.JSON(http.StatusBadRequest, msg)
	}
}

func DeleteDocument() func(c *gin.Context) {
	return func(c *gin.Context) {
		indexName := c.Query("index")
		params := make(map[string]string)
		params["index"] = indexName

		msg, _ := engine.Engine.DeleteDocument(params)

		c.JSON(http.StatusBadRequest, msg)

	}
}

func UpdateDocument() func(c *gin.Context) {
	return func(c *gin.Context) {
		data, _ := c.GetRawData()
		indexName := c.Query("index")
		params := make(map[string]string)
		params["index"] = indexName

		msg, _ := engine.Engine.UpdateDocument(params, data)

		c.JSON(http.StatusBadRequest, msg)
	}
}
