package cluster

import (
	"GoDance/configs"
	"math/rand"
	"time"
)

//todo 这里汇集了一些raft算法中的行为，比如addNode，deleteNode，汇集了一些操作的逻辑

// Election 尝试选举自己为主节点
//选举策略：像候选主节点发送拉票RPC消息，超过配置中设置的则选举成功
func Election() {
	//更新自身状态为候选者
	State.selfState.state = Candidate
	config := configs.Config.Cluster
	for {
		success := tryElection()
		//如果竞选成功
		if success {
			//晋升自己为领导者
			State.selfState.state = Leader
			checkLatest()
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
		} else {
			//如果竞选失败，且这期间收到了高于自己term的心跳，此时退出选举
			if !checkHeatBeat() {
				break
			}
			//如果竞选失败，检查一下自身节点是否在集群中，如果不在，则需要再次尝试获取当前Leader(避免初始情况下的无效自我选举)
			if _, ok := State.clusterState.Nodes.nodes[State.selfState.nodeId]; !ok {
				flag := tryJoin()
				if flag {
					break
				}
			}
		}

		//否则选举在随机时间后重新开始
		randomElection := rand.Intn(config.ElectionTimeMax-config.ElectionTimeMin) + config.ElectionTimeMin
		time.Sleep(time.Duration(randomElection))
	}

}

//TODO 需要检查新最新的日志是否已经大多数提交或者存在节点已经提交，如果存在这种情况则提交该日志
func checkLatest() {

}

//检验心跳是否超时,返回是否超时
func checkHeatBeat() bool {
	config := configs.Config.Cluster
	baseTime := config.HeartBeatMin
	random := config.HeartBeatMax - config.HeartBeatMin
	//根据配置设置随机超时时间
	//检查主节点是否近期发送过探活请求
	randomTimeOut := rand.Intn(random) + baseTime
	//如果超时没收到心跳
	if time.Now().Sub(heartBeat).Milliseconds() > int64(randomTimeOut) {
		return true
	}
	return false
}
