package raft

import (
	"cluster"
	"cluster/node"
	"configs"
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

var clients []*rpc.Client = nil

//尝试选举自己为主节点
//选举策略：像候选主节点发送拉票RPC消息，超过配置中设置的则选举成功
func tryElection() {
	//更新自身状态为候选者
	cluster.State.SelfState.State = node.Candidate
	config := configs.Config.NodeConfig
	candidateNodes := cluster.State.ClusterState.Nodes.CandidateNodes

	clients = make([]*rpc.Client, len(candidateNodes))
	i := 0
	for _, v := range candidateNodes {
		//todo 这里需要处理rpc连接失败的情况
		client, err := rpc.DialHTTP("tcp", v.HostAddress+":1000")
		if err != nil {
			log.Fatal("election 1阶段 rpc client init failed,dialing:", err)
		}
		clients[i] = client
		i += 1
	}
	//todo 选举 要考虑超时的情况
	for {
		cluster.State.SelfState.Term = cluster.State.SelfState.Term + 1
		r := VoteRequest{
			term:    cluster.State.SelfState.Term,
			version: cluster.State.ClusterState.Version,
			id:      cluster.State.SelfState.NodeId,
		}
		var sum uint32 = 0
		var wg sync.WaitGroup
		wg.Add(len(candidateNodes))
		//一阶段选举投票
		beginVote(&r, &sum, &wg)
		wg.Wait()
		//如果大于配置的“大多数”，且身份依旧是候选者（即在这期间没有收到过其他任期更高的心跳）
		if sum >= configs.Config.NodeConfig.ElectionMin && cluster.State.SelfState.State == node.Candidate {
			//晋升自己为领导者
			cluster.State.SelfState.State = node.Leader
			//开始向其他节点发送心跳
			go heatBeat()
			break
		}
		//否则选举在随机时间后重新开始
		randomElection := rand.Intn(config.ElectionTimeMax-config.ElectionTimeMin) + config.ElectionTimeMin
		time.Sleep(time.Duration(randomElection))
	}

}
func beginVote(r *VoteRequest, sum *uint32, wg *sync.WaitGroup) {
	for _, c := range clients {
		var res VoteResponse
		divCall := c.Go("Server.RequestVote", &r, &res, nil)
		replyCall := <-divCall.Done
		//异步处理返回值
		go func() {
			defer c.Close()
			if replyCall.Error != nil {
				log.Fatal("arith error:", replyCall.Error)
			}
			//累加票数
			if res.success {
				atomic.AddUint32(sum, 1)
			}
			wg.Done()
		}()
	}
}

//开始定时向其他节点发送心跳
func heatBeat() {
	min := configs.Config.NodeConfig.HeartBeatMin
	for _, n := range cluster.State.ClusterState.Nodes.Nodes {
		//不给自己发心跳
		if n.NodeId == cluster.State.SelfState.NodeId {
			continue
		}
		//todo 这里需要处理rpc连接失败的情况
		client, err := rpc.DialHTTP("tcp", n.HostAddress+":1000")
		if err != nil {
			log.Fatal("election 1阶段 rpc client init failed,dialing:", err)
		}
		//为每个节点开启一个协程,定时发送心跳
		go func() {
			defer client.Close()
			var res VoteResponse
			for {
				//当自身不是领导者时，不再发送
				if cluster.State.SelfState.State != node.Leader {
					break
				}
				r := VoteRequest{
					term:    cluster.State.SelfState.Term,
					version: cluster.State.ClusterState.Version,
					id:      cluster.State.SelfState.NodeId,
				}
				// rpc调用
				client.Go("Server.HeatBeat", &r, &res, nil)
				time.Sleep(time.Duration(min / 2))
			}
		}()
	}

}

//尝试加入节点
func tryJoin() {

}
