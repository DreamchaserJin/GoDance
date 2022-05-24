package node

type DiscoveryNode struct {
	//版本id
	version int64
	//节点名称
	nodeName string
	//节点id
	nodeId string
	//主机名称
	hostName string
	//主机地址
	hostAddress string
	//节点参数
	attributes map[string]string
}
