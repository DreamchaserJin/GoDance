package cluster

import (
	"context"
	"log"
	"sync"
	"time"
)

var (
	//上一次投票的任期
	//todo 这里需要直接变为集群状态中的term
	//lastTerm int64 = 0
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
	//最新增加的日志term
	LogTerm int64
	//nodeID
	Id int64
}
type VoteReply struct {
	Success bool
}

// RequestVote 请求投票，候选节点通过rpc远程调用从节点的拉票方法来获取票数
func (s *Server) RequestVote(ctx context.Context, r *VoteArgs, res *VoteReply) error {
	//之前相同任期内投过票的节点也会投票，此举是为了保证幂等性
	if votedNodes[r.Id] == r.Term {
		res.Success = true
		return nil
	}
	//如果任期没有大于自身，拒绝投票
	if r.Term <= State.selfState.term {
		res.Success = false
		return nil
	}
	//自身任期更新为较大的任期
	State.selfState.term = r.Term
	//重置心跳
	heartBeat = time.Now()
	defer serverMutex.RUnlock()
	serverMutex.RLock()
	//日志完整度(日志id和日志term)大于自身且超过自身的任期时，才会支持该节点
	if r.LogId >= logs[len(logs)-1].Id && r.LogTerm >= logs[len(logs)-1].Term {
		defer serverMutex.Unlock()
		serverMutex.Lock()
		votedNodes[r.Id] = r.Term
		res.Success = true
	}
	return nil
}

type HeartArgs struct {
	//todo 发送最新提交的日志id和前一个节点的id
	//前一个未提交的日志id
	PreCommitId uint64
	//最新提交的日志id
	CommittedId uint64
	//latest log id(committed)
	//LogEntryId uint64
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
	//State CMState
	//Follower最近一次提交的日志id
	LatestCommitted uint64
	//最近一次增加的日志id
	//latestLog uint64
}

// HeatBeat 探活请求，由主节点来调用此RPC方法
//同时此Rpc调用也负责日志的同步
//此RPC的作用有两个：1.表示Leader成功竞选或者Leader还活着；2.提交日志，如果提交不成功返回上一个提交的id，以便Leader进行同步
func (s *Server) HeatBeat(ctx context.Context, a *HeartArgs, r *HeartReply) error {
	self := State.selfState
	//res.State = State.selfState.state
	//如果发现小于自己任期时，拒绝承认该节点，不进行日志同步
	if self.masterId != a.Id && a.Term < self.term {
		r.Term = State.selfState.term
		return nil
	}
	//当发现探活请求的主节点和自己认为的主节点不一致且任期比自己的大时，切换主节点
	if self.masterId != a.Id && a.Term > self.term {
		self.masterId = a.Id
		//退出主节点时需要主动关闭clients
		for _, c := range clients {
			err := c.client.Close()
			if err != nil {
				return err
			}
		}
		//改变指针，触发GC
		clients = make(map[int64]*nodeClient)
	}
	if res, cid := CommitLog(a.CommittedId, a.PreCommitId); res {
		r.Success = true
	} else {
		r.LatestCommitted = cid
	}
	heartBeat = time.Now()
	return nil
}

type EntryArgs struct {
	PreNode  int
	LogEntry LogEntry
}

// AppendEntry RPC called by leader
//todo 这里要明白有些日志对于领导者而言是不存在的，需要验证前者是否一致
func (s *Server) AppendEntry(ctx context.Context, logEntry *LogEntry, reply *CommonReply) error {
	// todo 需要验证term是否大于等于自身（保证一旦发起竞选，新的日志将写入失败），以及是否是上一个日志是否一致
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
	// todo 需要验证提交的日志term是否大于等于自身，是否是上一个日志是否一致以及提交committed是否一致
	for _, e := range args.IncreasedLog {
		AppendSelfLog(&e)
	}

	for id := range args.CommittedIds {
		//todo 修改该调用方式
		//res,cid:= CommitLog(id,)
		if !res {
			log.Panicln("节点提交失败！")
			break
		}
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
