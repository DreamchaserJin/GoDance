package routing

//RoutingTable 所有index的路由表，用于描述所有分片的状态，用于路由操作，比如找到相关节点
type Table struct {
	version int64
	//索引路由，映射名称和ip
	indicesRouting map[string]string
}
