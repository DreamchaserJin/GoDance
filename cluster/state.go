package cluster

import (
	"sync"
)

var (
	// State 当前的状态信息，包括集群状态和自身节点的状态,可以理解为raft算法里的状态机
	State = &state{}
)

type state struct {
	clusterState clusterState
	selfState    selfState
	// Mutex 用于控制读写
	mutex sync.RWMutex
}
type selfState struct {
	//节点id
	nodeId int64
	//节点名称
	nodeName string
	//主机地址
	address string
	//节点参数
	attributes map[string]string
	//Address of theed node(ip)
	seedNodes []string
	//当前节点状态
	state CMState
	//当前节点认为的主节点Id
	masterId int64
	//任期
	term int64
	//是否是数据节点
	isData bool
	//是否是主节点（有资格成为Leader）
	isMaster bool
	//内存负载率
	loadRate float32
}

//clusterState
/**
掌握了集群的状态信息，在内存中持有该状态信息，部分数据会持久化到磁盘中来确保集群的元数据一致
部分会在每次更新时写入磁盘，因此它会在整个集群重新启动时保持不变。此数据的其余部分仅在内存中维护，
并在全集群重启时重置回其初始状态，但它保留在所有节点上，因此它在主选举期间持续存在
**/
type clusterState struct {
	//protects following,Ensure write consistency
	mutex sync.Mutex
	//当前版本号，每次更新加1
	version int64
	//主节点任期
	term int64
	// MasterId 当前的主节点ID
	MasterId int64
	//所有table的路由表，用于描述所有table的状态，用于根据table类型找到分片路由
	IndexRouting map[string]ShardRooting
	//当前集群节点
	Nodes discoveryNodes
	//集群名称
	ClusterName string
}

// ShardRooting 分片路由
type ShardRooting struct {
	ShardId int64
	//分片数量
	ShardNum int8
	//主分片Id
	PrimaryId int64
	//分片情况
	Rooting []ShardMeta
}

func (c *clusterState) addNode(np *node) {
	c.mutex.Lock()
	c.Nodes.AddNode(np)
	c.version += 1
	c.mutex.Unlock()
}
func (c *clusterState) deleteNode(id int64) {
	c.mutex.Lock()
	c.Nodes.DeleteNode(id)
	c.version += 1
	c.mutex.Unlock()
}
