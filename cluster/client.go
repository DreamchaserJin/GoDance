package cluster

import (
	"GoDance/configs"
	"context"
	. "github.com/smallnest/rpcx/client"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

// the client of all nodes(exclude itself)
var clients []*nodeClient = nil

type nodeClient struct {
	node *node
	//the client for rpc calls
	client *Client
}

//尝试选举自己为主节点
//选举策略：像候选主节点发送拉票RPC消息，超过配置中设置的则选举成功
func tryElection() {
	//更新自身状态为候选者
	State.selfState.state = Candidate
	config := configs.Config.Cluster
	candidateNodes := State.clusterState.Nodes.masterNodes

	clients = make([]*nodeClient, len(candidateNodes))
	for _, v := range candidateNodes {
		//exclude self
		if v.nodeId == State.selfState.nodeId {
			continue
		}
		c := &nodeClient{
			node: v,
		}
		c.client = NewClient(DefaultOption)
		err := c.client.Connect("tcp", v.address+":"+Port)
		//todo 这里需要重试的逻辑
		if err != nil {
			log.Fatalf("failed to connect: %v", err)
		}
		clients = append(clients, c)
	}
	//todo 选举
	for {
		State.selfState.term += 1
		args := voteArgs{
			term:    State.selfState.term,
			version: State.clusterState.version,
			id:      State.selfState.nodeId,
		}

		var (
			//the num of vote
			vote uint32 = 1
			//Completed requests
			cnum uint32 = 1
			//Used to control vote results
			//todo there may be resource leakage here
			ch = make(chan int)
		)

		//选举投票
		for _, c := range clients {
			reply := &voteReply{}
			call := c.client.Go(context.Background(), "RaftServe", "RequestVote", args, reply, nil)

			//异步处理返回值
			go func() {
				if call.Error != nil {
					log.Fatal("arith error:", call.Error)
				}
				//add the vote by CAS
				if reply.success {
					atomic.AddUint32(&vote, 1)
				}
				//add the number of complete request ,notice the order of CAS here，cnum must behind of vote
				atomic.AddUint32(&cnum, 1)
				//When the votes are greater than the majority, or when the immediate failure isMasterNode too many to reach the majority
				if vote >= configs.Config.Cluster.ElectionMin ||
					cnum-vote > uint32(len(State.clusterState.Nodes.masterNodes))-configs.Config.Cluster.ElectionMin {
					ch <- 1
				}
			}()
		}
		//goroutine will block until condition isMasterNode reached
		<-ch
		//如果大于配置的“大多数”，且身份依旧是候选者（即在这期间没有收到过其他任期更高的心跳）
		if vote >= configs.Config.Cluster.ElectionMin && State.selfState.state == Candidate {
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

//开始定时向其他节点发送心跳
func heatBeat() {
	min := configs.Config.Cluster.HeartBeatMin
	nodes := State.clusterState.Nodes.nodes
	for _, n := range nodes {
		//skip Candidate ,because the node isMasterNode in clients
		if n.isMasterNode {
			continue
		}
		c := &nodeClient{
			node: n,
		}
		c.client = NewClient(DefaultOption)
		err := c.client.Connect("tcp", n.address+":"+Port)
		if err != nil {
			log.Fatal("heartBeat client init failed,dialing:", err)
		}
		clients = append(clients, c)
	}
	for i := range clients {
		c := clients[i]
		//为每个节点开启一个协程,定时发送心跳
		go func() {
			var reply HeartReply
			for {
				//当自身不是领导者时，不再发送
				if State.selfState.state != Leader {
					break
				}
				args := heartArgs{
					term:    State.selfState.term,
					version: State.clusterState.version,
					id:      State.selfState.nodeId,
				}
				// rpc同步调用
				err := c.client.Call(context.Background(), "RaftServe", "HeartBeat", &args, &reply)
				// 此时判定为节点故障，摘除该节点
				if err != nil {
					DeleteNode(c.node.nodeId)
				}
				//TODO 日志同步
				if reply.success {
					//如果发现follower日志不一致，则进行RPC调用进行同步
					if reply.latestLog != latestLog || reply.lastCommitted != latestCommitted {
						cids, ids := Diff(reply.lastCommitted, reply.latestLog)
						entriesArgs := EntriesArgs{
							CommittedIds: cids,
							IncreasedLog: ids,
						}
						var entriesReply CommonReply
						// rpc同步调用
						err := c.client.Call(context.Background(), "RaftServe", "AppendEntries", &entriesArgs, &entriesReply)
						if err != nil {
							log.Fatal("日志同步失败！")
						}
					}
				}
				time.Sleep(time.Duration(min / 2))
			}
		}()
	}
}

//尝试加入节点
func tryJoin() {

}
