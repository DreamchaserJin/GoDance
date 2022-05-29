package raft

import (
	"cluster"
	"configs"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
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
	mutex sync.RWMutex
)

func init() {
	heartBeat = time.Now()
	go func() {
		config := configs.Config.NodeConfig
		baseTime := config.HeartBeatMin
		random := config.HeartBeatMax - config.HeartBeatMin
		for {
			//todo 根据配置设置随机超时时间
			//定期检查主节点是否近期发送过探活请求
			randomTimeOut := rand.Intn(random) + baseTime
			//如果超时没收到心跳
			if time.Now().Sub(heartBeat).Milliseconds() > int64(randomTimeOut) {
				//如果是候选节点则开始选举自己为主节点
				if cluster.State.SelfState.IsCandidate {
					tryElection()
				} else {
					tryJoin()
				}
			}
			time.Sleep(1000)
		}
	}()
	//注册RPC服务器
	err := rpc.Register(new(Server))
	//todo 这里可以有性能优化，不过为了简便暂时先用http方式来做rpc调用
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":1000")
	if err != nil {
		log.Fatalln("rpc init failed,listen 1000 error: ", err)
	}
	//go func() {
	//	for {
	//		conn, err := l.Accept()
	//		if err != nil {
	//			log.Print("Error: accept rpc connection", err.Error())
	//			continue
	//		}
	//		go rpc.ServeConn(conn)
	//	}
	//}()
	go http.Serve(l, nil)
}

type Server struct {
}

// RequestVote 请求投票，候选节点通过rpc远程调用从节点的拉票方法来获取票数
func (c *Server) RequestVote(r *VoteRequest, res *VoteResponse) error {
	//重置心跳
	heartBeat = time.Now()
	defer mutex.RUnlock()
	mutex.RLock()
	state := cluster.State.ClusterState
	//之前相同任期内投过票的节点也会投票，此举是为了保证幂等性
	if votedNodes[r.id] == r.term {
		res.success = true
		return nil
	}
	//数据版本大于等于自身且超过自身投过的任期时，才会支持该节点
	if r.version >= state.Version && r.term > lastTerm {
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
//func (c *Server) ConfirmVote(r *VoteRequest, res *VoteResponse) error {
//	clusterState := cluster.State.ClusterState
//	//当任期大于当前主节点的term时，才会转移
//	if r.term > clusterState.Term {
//		cluster.State.Mutex.Lock()
//		cluster.State.SelfState.MasterId = r.id
//		//重置节点
//		heartBeat = time.Now()
//		cluster.State.Mutex.Unlock()
//		res.success = true
//	}
//	return nil
//}

// HeatBeat 探活请求，由主节点来调用此RPC方法
func (c *Server) HeatBeat(r *VoteRequest, res *EntryResponse) error {
	self := cluster.State.SelfState
	res.state = cluster.State.SelfState.State
	//如果发现小于自己任期时，拒绝承认该节点
	if self.MasterId != r.id && r.term < self.Term {
		res.success = false
		res.term = cluster.State.SelfState.Term
		return nil
	}
	//当发现探活请求的主节点和自己认为的主节点不一致且任期比自己的大时，切换主节点
	if self.MasterId != r.id && r.term > self.Term {
		self.MasterId = r.id
	}
	res.success = true
	heartBeat = time.Now()
	//当检测到主节点和从节点数据版本不一致时，开始拉数据
	if r.version > cluster.State.ClusterState.Version {
		pullMeta()
	}
	return nil
}

//todo 用于拉取数据
func pullMeta() {

}
