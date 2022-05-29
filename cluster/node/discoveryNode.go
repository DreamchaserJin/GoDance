package node

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

type DiscoveryNode struct {
	//任期
	Term int64
	//版本id
	Version int64
	//节点名称
	NodeName string
	//节点id
	NodeId int64
	//主机名称
	HostName string
	//主机地址
	HostAddress string
	//节点参数
	Attributes map[string]string
	//节点状态
	State CMState
	//是否是数据节点
	IsDataNode bool
	//是否是候选主节点
	IsCandidate bool
}
