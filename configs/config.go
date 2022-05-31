package configs

// Config 默认配置
//todo 这里需要根据文件去读写
var Config = Configs{
	ClusterConfig{
		HeartBeatMax:    2000,
		HeartBeatMin:    1000,
		ElectionTimeMax: 500,
		ElectionTimeMin: 200,
		ElectionMin:     0,
	},
}

type Configs struct {
	//集群配置
	Cluster ClusterConfig
}

type ClusterConfig struct {
	//随机心跳超时时间上限（ms）
	HeartBeatMax int
	//随机心跳超时时间下限（ms）
	HeartBeatMin int
	//随机竞选超时时间最大值（ms）
	ElectionTimeMax int
	//随机竞选超时时间最小值（ms）
	ElectionTimeMin int
	//竞选所需的最小票数
	ElectionMin uint32
}