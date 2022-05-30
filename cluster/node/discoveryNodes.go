package node

// DiscoveryNodes 保存所有节点信息
type DiscoveryNodes struct {
	//所有节点
	Nodes map[int64]*DiscoveryNode
	//数据节点
	DataNodes map[int64]*DiscoveryNode
	//候选主节点
	CandidateNodes map[int64]*DiscoveryNode
}

// DeleteNode 删除一个节点
func (ns *DiscoveryNodes) DeleteNode(id int64) {
	delete(ns.Nodes, id)
	delete(ns.DataNodes, id)
	delete(ns.CandidateNodes, id)
}

// AddNode 增加一个节点
func (ns *DiscoveryNodes) AddNode(n *DiscoveryNode) {
	ns.Nodes[n.NodeId] = n
	if n.IsDataNode {
		ns.DataNodes[n.NodeId] = n
	}
	if n.IsCandidate {
		ns.CandidateNodes[n.NodeId] = n
	}
}
