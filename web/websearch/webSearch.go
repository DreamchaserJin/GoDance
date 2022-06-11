package websearch

import (
	"GoDance/engine"
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
)

// GetRelated
// @Description 获取相关搜索
func GetRelated() func(c *gin.Context) {
	return func(c *gin.Context) {
		relation := make([]string, 0)
		key := c.Query("key")

		realTimeSearch := engine.Engine.RealTimeSearch(key)
		relation = realTimeSearch

		//返回数据
		c.JSON(http.StatusOK, gin.H{
			"relation": relation,
		})
	}
}

// GetResult
// @Description 获取搜索结果
func GetResult() func(c *gin.Context) {
	return func(c *gin.Context) {
		req := c.Request
		err := req.ParseForm()
		if err != nil {
			return
		}

		// 获取 GET 请求参数
		params := make(map[string]string)
		for k, v := range req.Form {
			params[k] = v[0]
		}

		search, err := engine.Engine.Search(params)
		if err == nil {
			c.JSON(http.StatusOK, search)
		} else {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("err : %v", err),
			})
		}
	}
}

// GetDocument
// @Description 获取文档内容
func GetDocument() func(c *gin.Context) {
	return func(c *gin.Context) {
		indexName := c.PostForm("index")
		id := c.PostForm("id")
		doc, err := engine.Engine.GetDocById(indexName, id)
		if err == nil {
			//返回数据
			var res gin.H
			for key, val := range doc {
				res[key] = val
			}
			c.JSON(http.StatusOK, res)
		} else {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("err : %v", err),
			})
		}

	}
}
