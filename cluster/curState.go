package cluster

import (
	"cluster/metaData"
	"sync"
)

// CMState 节点状态
type CMState int

const (
	// Leader 领导者
	Leader CMState = iota
	// Candidate 候选者
	Candidate
	// Follower 跟随者
	Follower
	// Dead 宕机状态
	Dead
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
	State CMState
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
