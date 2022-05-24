package metaData

import (
	"cluster/node"
	"cluster/routing"
)

//ClusterState
/**
掌握了集群的状态信息，在内存中持有该状态信息，部分数据会持久化到磁盘中来确保集群的元数据一致
部分会在每次更新时写入磁盘，因此它会在整个集群重新启动时保持不变。此数据的其余部分仅在内存中维护，
并在全集群重启时重置回其初始状态，但它保留在所有节点上，因此它在主选举期间持续存在
**/
type ClusterState struct {
	//当前版本号，每次更新加1
	version int64
	//该state对应的唯一id
	stateUUID string
	//所有index的路由表，用于描述所有分片的状态，用于路由操作，比如找到相关节点
	routingTable routing.Table
	//当前集群节点
	nodes node.DiscoveryNodes
	//集群的meta数据
	metaData MetaData
	//集群名称
	clusterName string
}
