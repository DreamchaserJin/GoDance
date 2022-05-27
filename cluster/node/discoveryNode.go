package node

type DiscoveryNode struct {
	//任期
	Term int64
	//版本id
	Version int64
	//节点名称
	NodeName string
	//节点id
	NodeId string
	//主机名称
	HostName string
	//主机地址
	HostAddress string
	//节点参数
	Attributes map[string]string
	//节点状态
	//State CMState
	//是否是数据节点
	IsDataNode bool
	//是否是候选主节点
	IsCandidate bool
}
