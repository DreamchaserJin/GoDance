package node

// DiscoveryNodes 保存所有节点信息
type DiscoveryNodes struct {
	//所有节点
	nodes map[string]DiscoveryNode
	//数据节点
	dataNodes map[string]DiscoveryNode
	//候选主节点
	masterNodes map[string]DiscoveryNode
}
