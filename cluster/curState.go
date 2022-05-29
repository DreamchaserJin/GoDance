package cluster

import (
	"cluster/metaData"
	"cluster/node"
	"sync"
)

var (
	// State 当前的状态信息，包括集群状态和自身节点的状态
	State = &CurState{}
)

type CurState struct {
	ClusterState metaData.ClusterState
	SelfState    SelfState
	// Mutex 用于控制读写
	Mutex sync.RWMutex
}
type SelfState struct {
	//节点id
	NodeId int64
	//节点名称
	NodeName string
	//主机名称
	HostName string
	//主机地址
	HostAddress string
	//节点参数
	Attributes map[string]string
	//当前节点状态
	State node.CMState
	//当前节点认为的主节点Id
	MasterId int64
	//任期
	Term int64
	//是否是主节点
	isMaster bool
	//是否是数据节点
	IsDataNode bool
	//是否是候选主节点
	IsCandidate bool
	//内存负载率
	LoadRate float32
}
