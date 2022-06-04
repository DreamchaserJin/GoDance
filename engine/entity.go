package engine

import "GoDance/index/segment"

type NodeIndex struct {
	IndexName    string                    `json:"indexname"`
	ShardNum     uint64                    `json:"shardnum"`
	Shard        []uint64                  `json:"shard"`
	IndexMapping []segment.SimpleFieldInfo `json:"indexmapping"`
	ShardNodes   map[uint64][]string       `json:"shardnodes"`
}

type NodeNetInfo struct {
	Addr    string         `json:"addr"`
	MPort   string         `json:"mport"`
	CPort   string         `json:"cport"`
	IdxChan chan NodeIndex `json:"-"`
}

// IndexStrct 索引构造结构，包含字段信息
type IndexStruct struct {
	IndexName     string                    `json:"indexname"`
	ShardNum      uint64                    `json:"shardnum"`
	ShardField    string                    `json:"shardfield"`
	FieldsMapping []segment.SimpleFieldInfo `json:"fieldsmapping"`
}
