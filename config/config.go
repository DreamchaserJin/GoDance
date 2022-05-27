package config

var Setting Config

type Config struct {
	ClusterConfig ClusterConfig
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
}
