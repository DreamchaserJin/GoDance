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
		config := configs.Config.Cluster
		baseTime := config.HeartBeatMin
		random := config.HeartBeatMax - config.HeartBeatMin
		for {
			//根据配置设置随机超时时间
			//定期检查主节点是否近期发送过探活请求
			randomTimeOut := rand.Intn(random) + baseTime
			//如果超时没收到心跳
			if time.Now().Sub(heartBeat).Milliseconds() > int64(randomTimeOut) {
				State.selfState.state = NoLeader
				//如果是候选节点则开始选举自己为主节点
				if State.selfState.isMaster {
					tryJoin()
					tryElection()
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
