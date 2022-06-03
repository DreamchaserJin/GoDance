package cluster

import (
	"configs"
	"context"
	"github.com/smallnest/rpcx/server"
	"math/rand"
	"sync"
	"time"
)

const Port = "8972"

var (
	//上一次投票的任期
	lastTerm int64 = 0
	//距离上次正式选举成功之后投票的节点,key是节点，val是任期，每次选举成功则会清除
	votedNodes map[int64]int64
	//上一次心跳请求的时间
	heartBeat = time.Now()
	//读写锁，用于同步
	mutex sync.RWMutex
)

func init() {
	heartBeat = time.Now()
	// register RaftServer
	s := server.NewServer()
	s.RegisterName("RaftServe", new(Server), "")
	s.Serve("tcp", ":"+Port)
	go func() {
		config := configs.Config.Cluster
		baseTime := config.HeartBeatMin
		random := config.HeartBeatMax - config.HeartBeatMin
		for {
			//todo 根据配置设置随机超时时间
			//定期检查主节点是否近期发送过探活请求
			randomTimeOut := rand.Intn(random) + baseTime
			//如果超时没收到心跳
			if time.Now().Sub(heartBeat).Milliseconds() > int64(randomTimeOut) {
				//如果是候选节点则开始选举自己为主节点
				if State.selfState.isMaster {
					tryElection()
				} else {
					tryJoin()
				}
			}
			time.Sleep(time.Duration(random))
		}
	}()

}

type Server struct {
}

//todo 修改version为logId
type voteArgs struct {
	//任期
	term int64
	//元数据
	version int64
	//nodeID
	id int64
}
type voteReply struct {
	success bool
}

// RequestVote 请求投票，候选节点通过rpc远程调用从节点的拉票方法来获取票数
func (s *Server) RequestVote(ctx context.Context, r *voteArgs, res *voteReply) error {
	//重置心跳
	heartBeat = time.Now()
	defer mutex.RUnlock()
	mutex.RLock()
	state := State.clusterState
	//之前相同任期内投过票的节点也会投票，此举是为了保证幂等性
	if votedNodes[r.id] == r.term {
		res.success = true
		return nil
	}
	//数据版本大于等于自身且超过自身投过的任期时，才会支持该节点
	if r.version >= state.version && r.term > lastTerm {
		defer mutex.Unlock()
		mutex.Lock()
		lastTerm = r.term
		votedNodes[r.id] = r.term
		res.success = true
	}
	res.success = false
	return nil
}

type heartArgs struct {
	//latest log id(committed)
	logEntryId uint64
	//任期
	term int64
	//元数据
	version int64
	//nodeID
	id int64
}

// HeartReply 探活请求（日志增加请求）
type HeartReply struct {
	//是否承认该节点
	success bool
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
func (s *Server) HeatBeat(ctx context.Context, r *heartArgs, res *HeartReply) error {
	self := State.selfState
	res.State = State.selfState.state
	//如果发现小于自己任期时，拒绝承认该节点
	if self.masterId != r.id && r.term < self.term {
		res.Term = State.selfState.term
		return nil
	}
	//当发现探活请求的主节点和自己认为的主节点不一致且任期比自己的大时，切换主节点
	if self.masterId != r.id && r.term > self.term {
		self.masterId = r.id
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

// AppendEntry RPC called by leader
func (s *Server) AppendEntry(ctx context.Context, logEntry *LogEntry, reply *CommonReply) error {
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
JoinNode Rpc
*/
func (s *Server) tryJoin(ctx context.Context, n *node, r *CommonReply) error {
	if State.selfState.state != Leader {
		r.Success = false
		return nil
	}
	success := AddNode(n)
	r.Success = success
	return nil
}

// CommonReply 较为通用的应答
type CommonReply struct {
	Success bool
}
