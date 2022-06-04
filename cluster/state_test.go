package cluster

import (
	"testing"
	"time"
)

func TestCluster(t *testing.T) {
	for {
		if State.selfState.state == Candidate {
			println("当前节点状态：正在参与竞选...")
		} else if State.selfState.state == Leader {
			println("当前节点状态：领导者")
		} else if State.selfState.state == Follower {
			println("当前节点状态：跟随者")
			println("Leader：" + State.getMasterAddress())
		} else {
			println("暂无Leader节点...")
		}
		time.Sleep(time.Second)
	}
	//n:=node{
	//	term:         0,
	//	nodeName:     "127.0.0.1",
	//	nodeId:       12,
	//	address:      "127.0.0.1",
	//	port:         "8972",
	//	attributes:   nil,
	//	state:        Follower,
	//	isDataNode:   true,
	//	isMasterNode: true,
	//}
	//State.clusterState.addNode(&n)
	//log.Println(n)
}
