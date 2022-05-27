package raft

import (
	"cluster"
	"math/rand"
	"sync"
	"time"
)

var (
	//上一次投票的任期
	lastTerm int64 = 0
	//距离上次正式选举成功之后投票的节点,key是节点，val是任期，每次选举成功则会清除
	votedNodes map[int64]int64
	//上一次心跳请求的时间
	heartBeat time.Time = time.Now()
	//读写锁，用于同步
	mutex sync.RWMutex
)

func init() {
	heartBeat = time.Now()
	go func() {
		for {
			//定期检查主节点是否近期发送过探活请求
			randomTimeOut := rand.Intn(2000)
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
}

type Client struct {
}

// RequestVote 请求投票，候选节点通过rpc远程调用从节点的拉票方法来获取票数
func (c *Client) RequestVote(r VoteRequest) (isVote bool) {
	//重置心跳
	heartBeat = time.Now()
	defer mutex.RUnlock()
	mutex.RLock()
	state := cluster.State.ClusterState
	//之前相同任期内投过票的节点也会投票，此举是为了保证幂等性
	if votedNodes[r.id] == r.term {
		return true
	}
	//数据版本大于等于自身且超过自身投过的任期时，才会支持该节点
	if r.version >= state.Version && r.term >= lastTerm {
		defer mutex.Unlock()
		mutex.Lock()
		lastTerm = r.term
		votedNodes[r.id] = r.term
		return true
	}
	return false
}

// ConfirmVote 拉票请求的第二阶段，确定选举，此时说明主节点获得大多数选票，此种情况会调用所有已知节点的rpc
func (c *Client) ConfirmVote(r VoteRequest) {
	clusterState := cluster.State.ClusterState
	//当任期大于当前主节点的term时，才会转移
	if r.term > clusterState.Term {
		cluster.State.Mutex.Lock()
		cluster.State.SelfState.MasterId = r.id
		//重置节点
		heartBeat = time.Now()
		cluster.State.Mutex.Unlock()
	}
}

// AppendEntries 探活请求，由主节点来调用此RPC方法
func (c *Client) AppendEntries(r VoteRequest) {
	self := cluster.State.SelfState
	//当发现探活请求的主节点和自己认为的主节点不一致且任期比自己的大时，切换主节点
	if self.MasterId != r.id && r.term > self.Term {
		self.MasterId = r.id
	}
	heartBeat = time.Now()
	//当检测到主节点和从节点数据版本不一致时，开始拉数据
	if r.version > cluster.State.ClusterState.Version {
		pullMeta()
	}
}

//todo 用于拉取数据
func pullMeta() {

}
