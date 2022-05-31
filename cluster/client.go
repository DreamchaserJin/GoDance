package cluster

import (
	"configs"
	"context"
	. "github.com/smallnest/rpcx/client"
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

var clients []*Client = nil

//尝试选举自己为主节点
//选举策略：像候选主节点发送拉票RPC消息，超过配置中设置的则选举成功
func tryElection() {
	//更新自身状态为候选者
	State.selfState.state = Candidate
	config := configs.Config.Cluster
	candidateNodes := State.clusterState.Nodes.candidateNodes

	clients = make([]*Client, len(candidateNodes))
	i := 0
	for _, v := range candidateNodes {
		clients[i] = NewClient(DefaultOption)
		err := clients[i].Connect("tcp", v.address+":"+Port)
		//todo 这里需要重试的逻辑
		if err != nil {
			log.Fatalf("failed to connect: %v", err)
		}
		i += 1
	}
	//todo 选举 要考虑超时的情况,这里标准库没有超时功能，所以需要
	for {
		State.selfState.term += 1
		r := voteArgs{
			term:    State.selfState.term,
			version: State.clusterState.version,
			id:      State.selfState.nodeId,
		}
		//the sum of vote cast
		var sum uint32 = 0
		var wg sync.WaitGroup
		wg.Add(len(candidateNodes))
		//一阶段选举投票
		beginVote(&r, &sum, &wg)
		wg.Wait()
		//如果大于配置的“大多数”，且身份依旧是候选者（即在这期间没有收到过其他任期更高的心跳）
		if sum >= configs.Config.Cluster.ElectionMin && State.selfState.state == Candidate {
			//晋升自己为领导者
			State.selfState.state = Leader
			//开始向其他节点发送心跳
			go heatBeat()
			break
		}
		//否则选举在随机时间后重新开始
		randomElection := rand.Intn(config.ElectionTimeMax-config.ElectionTimeMin) + config.ElectionTimeMin
		time.Sleep(time.Duration(randomElection))
	}

}
func beginVote(args *voteArgs, sum *uint32, wg *sync.WaitGroup) {
	for _, c := range clients {
		reply := &voteReply{}
		call := c.Go(context.Background(), "RaftServe", "RequestVote", args, reply, nil)

		//异步处理返回值
		go func() {
			defer c.Close()
			if call.Error != nil {
				log.Fatal("arith error:", call.Error)
			}
			//累加票数
			if reply.success {
				atomic.AddUint32(sum, 1)
			}
			wg.Done()
		}()
	}
}

//开始定时向其他节点发送心跳
func heatBeat() {
	min := configs.Config.Cluster.HeartBeatMin
	for _, n := range State.clusterState.Nodes.nodes {
		//不给自己发心跳
		if n.nodeId == State.selfState.nodeId {
			continue
		}
		//todo 这里需要处理rpc连接失败的情况
		client, err := rpc.DialHTTP("tcp", n.address+":1000")
		if err != nil {
			log.Fatal("election 1阶段 rpc client init failed,dialing:", err)
		}
		//为每个节点开启一个协程,定时发送心跳
		go func() {
			defer client.Close()
			var res voteReply
			for {
				//当自身不是领导者时，不再发送
				if State.selfState.state != Leader {
					break
				}
				r := voteArgs{
					term:    State.selfState.term,
					version: State.clusterState.version,
					id:      State.selfState.nodeId,
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
