package cluster

import (
	"configs"
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
				if State.selfState.isCandidate {
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
func (s *Server) RequestVote(r *voteArgs, res *voteReply) error {
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

// ConfirmVote 拉票请求的第二阶段，确定选举，此时说明主节点获得大多数选票，此种情况会调用所有已知节点的rpc
//func (c *Server) ConfirmVote(r *voteArgs, res *voteReply) error {
//	clusterState := cluster.state.clusterState
//	//当任期大于当前主节点的term时，才会转移
//	if r.term > clusterState.term {
//		cluster.state.Mutex.Lock()
//		cluster.state.selfState.masterId = r.id
//		//重置节点
//		heartBeat = time.Now()
//		cluster.state.Mutex.Unlock()
//		res.success = true
//	}
//	return nil
//}
// entryResponse 探活请求（日志请求）
type entryResponse struct {
	//是否接受领导
	success bool
	//节点状态
	state CMState
	//接受到探活请求的任期
	term int64
}

type commonResponse struct {
	success bool
	//leader Id that current node think
	leaderId int64
}

// HeatBeat 探活请求，由主节点来调用此RPC方法
func (s *Server) HeatBeat(r *voteArgs, res *entryResponse) error {
	self := State.selfState
	res.state = State.selfState.state
	//如果发现小于自己任期时，拒绝承认该节点
	if self.masterId != r.id && r.term < self.term {
		res.success = false
		res.term = State.selfState.term
		return nil
	}
	//当发现探活请求的主节点和自己认为的主节点不一致且任期比自己的大时，切换主节点
	if self.masterId != r.id && r.term > self.term {
		self.masterId = r.id
	}
	res.success = true
	heartBeat = time.Now()
	//当检测到主节点和从节点数据版本不一致时，开始拉数据
	if r.version > State.clusterState.version {
		pullMeta()
	}
	return nil
}
func (s *Server) tryJoin(n *node, r *commonResponse) {
	if State.selfState.state != Leader {
		r.success = false
	}
}

//todo 用于拉取数据
func pullMeta() {
}
