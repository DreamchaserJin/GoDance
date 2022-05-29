package raft

import "cluster/node"

type VoteRequest struct {
	//任期
	term int64
	//元数据
	version int64
	//nodeID
	id int64
}
type VoteResponse struct {
	success bool
}

// EntryResponse 探活请求（日志请求）
type EntryResponse struct {
	//是否接受领导
	success bool
	//节点状态
	state node.CMState
	//接受到探活请求的任期
	term int64
}
