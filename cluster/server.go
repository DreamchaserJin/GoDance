package cluster

import (
	"context"
	"sync"
	"time"
)

var (
	//上一次投票的任期
	lastTerm int64 = 0
	//距离上次正式选举成功之后投票的节点,key是节点，val是任期，每次选举成功则会清除
	votedNodes map[int64]int64
	//上一次心跳请求的时间
	heartBeat = time.Now()
	//读写锁，用于同步
	serverMutex sync.RWMutex
)

type Server struct {
}

type VoteArgs struct {
	//任期
	Term int64
	//最新增加的日志id
	LogId uint64
	//最新提交日志ID
	CommittedId uint64
	//nodeID
	Id int64
}
type VoteReply struct {
	Success bool
}

// RequestVote 请求投票，候选节点通过rpc远程调用从节点的拉票方法来获取票数
func (s *Server) RequestVote(ctx context.Context, r *VoteArgs, res *VoteReply) error {
	//重置心跳
	heartBeat = time.Now()
	defer serverMutex.RUnlock()
	serverMutex.RLock()
	//之前相同任期内投过票的节点也会投票，此举是为了保证幂等性
	if votedNodes[r.Id] == r.Term {
		res.Success = true
		return nil
	}
	//日志完整度大于等于自身且超过自身投过的任期时，才会支持该节点
	if r.LogId >= latestLog && r.CommittedId > latestCommitted && r.Term > lastTerm {
		defer serverMutex.Unlock()
		serverMutex.Lock()
		lastTerm = r.Term
		votedNodes[r.Id] = r.Term
		res.Success = true
	}
	res.Success = false
	return nil
}

type HeartArgs struct {
	//latest log id(committed)
	LogEntryId uint64
	//任期
	Term int64
	//nodeID
	Id int64
}

// HeartReply 探活请求（日志增加请求）
type HeartReply struct {
	//是否承认该节点
	Success bool
	//接受到探活请求的任期
	Term int64
	//节点状态
	State CMState
	//上一次提交的日志id
	lastCommitted uint64
	//最近一次增加的日志id
	latestLog uint64
}

// HeatBeat 探活请求，由主节点来调用此RPC方法
//同时此Rpc调用也负责日志的同步
func (s *Server) HeatBeat(ctx context.Context, r *HeartArgs, res *HeartReply) error {
	self := State.selfState
	res.State = State.selfState.state
	//如果发现小于自己任期时，拒绝承认该节点
	if self.masterId != r.Id && r.Term < self.term {
		res.Term = State.selfState.term
		return nil
	}
	//当发现探活请求的主节点和自己认为的主节点不一致且任期比自己的大时，切换主节点
	if self.masterId != r.Id && r.Term > self.term {
		self.masterId = r.Id
		//退出主节点时需要主动关闭clients
		for _, c := range clients {
			c.client.Close()
		}
		//free up space，trigger GC
		clients = nil
	}
	res.latestLog = latestLog
	res.lastCommitted = latestCommitted
	heartBeat = time.Now()
	return nil
}

type EntryArgs struct {
	PreNode  int
	LogEntry LogEntry
}

// AppendEntry RPC called by leader
func (s *Server) AppendEntry(ctx context.Context, logEntry *LogEntry, reply *CommonReply) error {
	// todo 需要验证是否是上一个日志是否一致
	AppendSelfLog(logEntry)
	reply.Success = true
	return nil
}

type EntriesArgs struct {
	CommittedIds []uint64
	IncreasedLog []LogEntry
}

// AppendEntries 由Leader来调用，用于同步Leader和Flower日志的一致性
func (s *Server) AppendEntries(ctx context.Context, args *EntriesArgs, reply *CommonReply) error {
	// todo 需要验证是否是上一个日志是否一致以及提交committed是否一致
	for _, e := range args.IncreasedLog {
		AppendSelfLog(&e)
	}
	for _, id := range args.CommittedIds {
		CommitLog(id)
	}
	reply.Success = true
	return nil
}

/**
JoinToLeader Rpc
由跟随者来调用
*/
func (s *Server) JoinToLeader(ctx context.Context, n *node, r *CommonReply) error {
	if State.selfState.state != Leader {
		r.Success = false
		return nil
	}
	success := AddNode(n)
	//同时这里还要增加对应client
	r.Success = success
	return nil
}

// CommonReply 较为通用的应答
type CommonReply struct {
	Success bool
}
type QueryArgs struct {
}
type QueryReply struct {
	//领导者的地址（ip+port）
	LeaderAddress string
}

// SeedQuery 种子节点查询rpc，由找不到主节点的跟随者或者刚启动的节点调用，返回当前集群的主节点
func (s *Server) SeedQuery(ctx context.Context, args *QueryArgs, reply *QueryReply) {
	//若当前节点暂时没有Leader
	if State.selfState.state == Candidate || State.selfState.state == NoLeader {
		reply.LeaderAddress = ""
	} else {
		reply.LeaderAddress = State.getMasterAddress()
	}
}
