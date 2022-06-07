package web

import (
	"GoDance/web/Email"
	"GoDance/web/idxOpt"
	myjwt "GoDance/web/jwt"
	"GoDance/web/mysql"
	Redis "GoDance/web/redis"
	Search "GoDance/web/websearch"
	"github.com/gin-gonic/gin"
	"net/http"
)

func Register(r *gin.Engine) {

	r.Static("/godance", "./web/View_final/src")
	r.LoadHTMLFiles("web/View_final/index.html")
	//打开index页面
	r.GET("/index", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", nil)
	})
	//注册
	r.POST("/godance/register", mysql.Register())

	//登录
	r.POST("/godance/sign", mysql.Sign())
	//销户
	r.POST("/godance/delete", mysql.Delete())
	//邮箱
	r.POST("/godance/get_code", Email.SendEmail())

	//测试jwt
	r.POST("/test", myjwt.JWTAuthMiddleware(), func(c *gin.Context) {
		username, _ := c.Get("userId")

		c.JSON(http.StatusOK, gin.H{
			"token": username,
			"code":  200,
		})
	})

	//获取所有收藏夹的名称
	r.GET("/collection", myjwt.JWTAuthMiddleware(), Redis.Get_Collection_final())
	//获取指定收藏夹的内容
	r.GET("/bag_child", myjwt.JWTAuthMiddleware(), Redis.Get_Bag_Child())
	//添加收藏夹
	r.POST("/godance/add_one", myjwt.JWTAuthMiddleware(), Redis.Add_collection())
	//删除收藏夹
	r.POST("/godance/delete_one", myjwt.JWTAuthMiddleware(), Redis.Delete_collection())
	//增加收藏夹内容
	r.POST("/godance/add_one_child", myjwt.JWTAuthMiddleware(), Redis.Add_child())
	//删除收藏夹内容
	r.POST("/godance/delete_one_child", myjwt.JWTAuthMiddleware(), Redis.Delete_child())
	//判断某个id在收藏夹中是否存在
	r.POST("/godance/is_not", myjwt.JWTAuthMiddleware(), Redis.Is_not())

	//搜索
	r.GET("/search_text", Search.Get_relation())
	r.GET("/search_final", Search.Get_related())
	r.POST("/godance/get_essay", Search.Get_essay())

	// 对索引的操作
	r.POST("/create", idxOpt.CreateIndex())

	// 对文档的操作
	r.POST("/update", idxOpt.AddDocument())
	r.DELETE("/update", idxOpt.DeleteDocument())
	r.PUT("/update", idxOpt.UpdateDocument())

}
