package routing

//TableRooting 用于做table到分片路由的映射
type TableRooting struct {
	version int64
	//文档路由，映射文档名称和分片路由
	IndexRouting map[string]ShardRooting
}
