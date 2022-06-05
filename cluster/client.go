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
var clients map[int64]*nodeClient

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
	masterNodes := State.clusterState.Nodes.masterNodes

	clients = make(map[int64]*nodeClient, len(masterNodes))
	for _, v := range masterNodes {
		//exclude self
		if v.nodeId == State.selfState.nodeId {
			continue
		}
		c := &nodeClient{
			node: v,
		}
		c.client = NewClient(DefaultOption)
		err := c.client.Connect("tcp", v.address+":"+v.port)
		//todo 这里需要重试的逻辑
		if err != nil {
			log.Printf("failed to connect: %v", err)
		}
		clients[v.nodeId] = c
	}
	// 选举
	//Used to control vote results
	ch := make(chan int, 1)
	//defer close(ch)
	for {
		atomic.AddInt64(&State.selfState.term, 1)
		args := VoteArgs{
			Term:        State.selfState.term,
			LogId:       latestLog,
			CommittedId: latestCommitted,
			Id:          State.selfState.nodeId,
		}
		var (
			//the num of vote
			vote uint32 = 1
			//Completed requests
			cnum uint32 = 1
		)
		//选举投票
		for _, c := range clients {
			reply := &VoteReply{}
			call := c.client.Go(context.Background(), "RaftServe", "RequestVote", args, reply, nil)

			//异步处理返回值
			go func() {
				if call.Error != nil {
					log.Println("arith error:", call.Error)
				}
				//add the vote by CAS
				if reply.Success {
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
		//避免单节点时出现永久堵塞情况
		if vote >= configs.Config.Cluster.ElectionMin {
			ch <- 1
		}
		//goroutine will block until condition isMasterNode reached
		<-ch
		//如果大于配置的“大多数”，且身份依旧是候选者（即在这期间没有收到过其他任期更高的心跳）
		if vote >= configs.Config.Cluster.ElectionMin && State.selfState.state == Candidate {
			//晋升自己为领导者
			State.selfState.state = Leader
			//开始向其他节点发送心跳
			go sendHeartBeats()
			//如果自己不在集群节点中，则要主动加入
			if _, ok := State.clusterState.Nodes.nodes[State.selfState.nodeId]; !ok {
				s := State.selfState
				node := node{
					term:         0,
					nodeName:     s.nodeName,
					nodeId:       s.nodeId,
					address:      s.address,
					port:         s.port,
					attributes:   nil,
					state:        s.state,
					isDataNode:   s.isData,
					isMasterNode: s.isMaster,
				}
				AddNode(&node)
			}
			break
		}
		//如果竞选失败，检查一下自身节点是否在集群中，如果不在，则需要再次尝试获取当前Leader(避免初始情况下的无效自我选举)
		if _, ok := State.clusterState.Nodes.nodes[State.selfState.nodeId]; !ok {
			flag := tryJoin()
			if flag {
				break
			}
		}
		//否则选举在随机时间后重新开始
		randomElection := rand.Intn(config.ElectionTimeMax-config.ElectionTimeMin) + config.ElectionTimeMin
		time.Sleep(time.Duration(randomElection))
	}

}

//开始定时向其他节点发送心跳
func sendHeartBeats() {
	nodes := State.clusterState.Nodes.nodes
	for _, n := range nodes {
		//skip MasterNode ,because the node isMasterNode in clients
		if _, ok := clients[n.nodeId]; ok {
			continue
		}
		c := &nodeClient{
			node: n,
		}
		c.client = NewClient(DefaultOption)
		err := c.client.Connect("tcp", n.address+":"+n.port)
		if err != nil {
			log.Printf("heartBeat client init failed,dialing:%v", err)
		}
		clients[n.nodeId] = c
	}
	for _, c := range clients {
		//为每个节点开启一个协程,定时发送心跳
		go sendHeartBeat(c)
	}
}
func sendHeartBeat(c *nodeClient) {
	var reply HeartReply
	for {
		//当自身不是领导者时，不再发送
		if State.selfState.state != Leader {
			break
		}
		args := HeartArgs{
			Term: State.selfState.term,
			Id:   State.selfState.nodeId,
		}
		// rpc同步调用
		err := c.client.Call(context.Background(), "RaftServe", "HeartBeat", &args, &reply)
		// 此时判定为节点故障，摘除该节点
		if err != nil {
			DeleteNode(c.node.nodeId)
		}
		//TODO 这里可以修改为当日志差异过大直接进行state复制
		if reply.Success {
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
					log.Println("日志同步失败！")
				}
			}
		}
		time.Sleep(time.Duration(configs.Config.Cluster.HeartBeatMin / 2))
	}
}

//尝试加入节点,返回是否成功
func tryJoin() (success bool) {
	var (
		args  QueryArgs
		reply QueryReply
		cs    = map[string]*Client{}
	)
	success = false
	for _, s := range configs.Config.Cluster.SeedNodes {
		//rpc调用种子节点，尝试获取当前Leader
		if _, ok := cs[s]; !ok {
			c := NewClient(DefaultOption)
			err := c.Connect("tcp", s)
			if err != nil {
				log.Println("failed to connect: "+s, err)
				continue
			}
			cs[s] = c
		}
		err := cs[s].Call(context.Background(), "RaftServe", "SeedQuery", &args, &reply)
		if err != nil {
			log.Println("failed to call SeedQuery rpc: ", err)
			continue
		}
		s := State.selfState
		//如果存在Leader，则会尝试加入Leader
		if reply.LeaderAddress != "" {
			node := node{
				term:         0,
				nodeName:     s.nodeName,
				nodeId:       s.nodeId,
				address:      s.address,
				port:         s.port,
				attributes:   nil,
				state:        s.state,
				isDataNode:   s.isData,
				isMasterNode: s.isMaster,
			}
			r := CommonReply{}
			c := NewClient(DefaultOption)
			err := c.Connect("tcp", reply.LeaderAddress)
			if err != nil {
				log.Println("failed to connect: "+reply.LeaderAddress, err)
				continue
			}
			err = c.Call(context.Background(), "RaftServe", "JoinToLeader", &node, &r)
			if err != nil || !r.Success {
				log.Println("failed to join Leader", err)
				continue
			}
			success = true
			State.selfState.state = Follower
			break
		}
	}
	return
}

//增加一个发送心跳的PRC客户端
func appendClient(n *node) {
	c := &nodeClient{
		node:   n,
		client: NewClient(DefaultOption),
	}
	clients[n.nodeId] = c
	err := c.client.Connect("tcp", n.address+":"+n.port)
	if err != nil {
		log.Printf("failed to connect: %v", err)
	}
	go sendHeartBeat(c)
}

//增加一个发送心跳的PRC客户端
func deleteClient(id int64) {
	delete(clients, id)
}
