package cluster

type ShardState int8

const (
	// UNAVAILABLE 不可用
	UNAVAILABLE ShardState = iota
	// AVAILABLE 可用
	AVAILABLE
)

type ShardMeta struct {
	//分片Id
	ShardId int64
	//当前的节点Id
	CurrentNodeId int64
	//转移的节点Id
	RelocatingNodeId int64
	//是否是主节点
	Primary bool
	//分片状态
	State ShardState
}
