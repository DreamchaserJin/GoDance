package node

// DiscoveryNodes 保存所有节点信息
type DiscoveryNodes struct {
	//所有节点
	nodes map[string]*DiscoveryNode
	//数据节点
	dataNodes map[string]*DiscoveryNode
	//候选主节点
	candidateNodes map[string]*DiscoveryNode
}

// DeleteNode 删除一个节点
func (ns *DiscoveryNodes) DeleteNode(id string) {
	delete(ns.nodes, id)
	delete(ns.dataNodes, id)
	delete(ns.candidateNodes, id)
}

// AddNode 增加一个节点
func (ns *DiscoveryNodes) AddNode(n *DiscoveryNode) {
	ns.nodes[n.NodeId] = n
	if n.IsDataNode {
		ns.dataNodes[n.NodeId] = n
	}
	if n.IsCandidate {
		ns.candidateNodes[n.NodeId] = n
	}
}
