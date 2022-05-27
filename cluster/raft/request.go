package raft

type VoteRequest struct {
	//任期
	term int64
	//元数据
	version int64
	//nodeID
	id int64
}
