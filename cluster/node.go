package cluster

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

type node struct {
	//任期
	term int64
	//版本id
	version int64
	//节点名称
	nodeName string
	//节点id
	nodeId int64
	//主机地址
	address string
	//节点参数
	attributes map[string]string
	//节点状态
	state CMState
	//是否是数据节点
	isDataNode bool
	//是否是候选主节点
	isCandidate bool
}

// discoveryNodes 保存所有节点信息
type discoveryNodes struct {
	//所有节点
	nodes map[int64]*node
	//数据节点
	dataNodes map[int64]*node
	//候选主节点
	candidateNodes map[int64]*node
}

// DeleteNode 删除一个节点
func (ns *discoveryNodes) DeleteNode(id int64) {
	delete(ns.nodes, id)
	delete(ns.dataNodes, id)
	delete(ns.candidateNodes, id)
}

// AddNode 增加一个节点
func (ns *discoveryNodes) AddNode(n *node) {
	ns.nodes[n.nodeId] = n
	if n.isDataNode {
		ns.dataNodes[n.nodeId] = n
	}
	if n.isCandidate {
		ns.candidateNodes[n.nodeId] = n
	}
}
