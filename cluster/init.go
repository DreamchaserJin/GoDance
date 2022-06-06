package cluster

import (
	"GoDance/configs"
	"github.com/smallnest/rpcx/server"
	"log"
	"math/rand"
	"sync"
	"time"
)

//为了方便管理整个包的初始化步骤而特意建了一个文件
func init() {
	recover()
	stateInit()
	serverInit()
}
func stateInit() {
	config := configs.Config.Cluster
	ip, err := GetOutBoundIP()
	if err != nil {
		log.Fatal("启动失败，原因：获取本地IP失败")
	}
	State = state{
		selfState: selfState{
			nodeId:     time.Now().UnixNano(),
			nodeName:   ip,
			address:    ip,
			port:       configs.Config.Cluster.Port,
			attributes: nil,
			seedNodes:  config.SeedNodes,
			state:      Follower,
			masterId:   0,
			term:       0,
			isData:     config.Data,
			isMaster:   config.Master,
		},
		clusterState: clusterState{
			clusterMutex: sync.Mutex{},
			term:         0,
			MasterId:     0,
			IndexRouting: make(map[string]ShardRooting),
			Nodes: discoveryNodes{
				nodes:       make(map[int64]*node),
				dataNodes:   make(map[int64]*node),
				masterNodes: make(map[int64]*node),
			},
			ClusterName: "default-cluster",
		},
	}
}

func serverInit() {
	heartBeat = time.Now()
	// register RaftServer
	s := server.NewServer()
	err := s.RegisterName("RaftServe", new(Server), "")
	if err != nil {
		log.Fatalln("注册集群服务失败")
	}
	//心跳检查
	go func() {
		for {
			overtime := checkHeatBeat()
			//如果超时没收到心跳
			if overtime {
				//修改状态为无Leader
				State.selfState.state = NoLeader
				//如果是候选节点则开始选举
				if State.selfState.isMaster {
					Election()
					break
				} else {
					for !tryJoin() {
					}
				}
			}
			time.Sleep(time.Duration(random))
		}
	}()
	//开启另一个协程监听RPC调用
	go func() {
		err := s.Serve("tcp", ":"+State.selfState.port)
		if err != nil {
			log.Fatalln("开放端口失败！")
		}
	}()
}

//todo 节点恢复 ,注意需要检查自身最新的日志在当前集群中是否提交
//因为有一种很特殊很特殊的情况： 假设有A B C D E五个节点，A B C成功复制了最新的日志项，leader节点A提交了，B C未提交，
//这时leader节点A挂了，选举出新leader节点B，这时候只能发现2个节点具备某条uncommitted的日志项，不满足大多数
func recover() {

}
