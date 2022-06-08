package idxopt

import (
	"GoDance/engine"
	"github.com/gin-gonic/gin"
	"net/http"
)

// CreateIndex 创建索引
func CreateIndex() func(c *gin.Context) {
	return func(c *gin.Context) {
		indexName := c.Query("index")
		data, _ := c.GetRawData()
		err := engine.Engine.CreateIndex(indexName, data)
		if err != nil {
			c.JSON(http.StatusBadRequest, err)
		} else {
			c.JSON(http.StatusOK, "OK")
		}

	}
}
