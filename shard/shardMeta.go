package shard

type State int8

const (
	// UNAVAILABLE 不可用
	UNAVAILABLE State = iota
	// AVAILABLE 可用
	AVAILABLE
)

type Meta struct {
	//分片Id
	ShardId int64
	//当前的节点Id
	CurrentNodeId int64
	//转移的节点Id
	RelocatingNodeId int64
	//是否是主节点
	Primary bool
	//分片状态
	State State
}
