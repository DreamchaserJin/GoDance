package routing

import "cluster/metaData"

// ShardRooting 分片路由
type ShardRooting struct {
	ShardId int64
	//分片数量
	ShardNum int8
	//主分片Id
	PrimaryId int64
	//分片情况
	Rooting []metaData.ShardMeta
}
