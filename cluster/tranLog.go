package cluster

import (
	"configs"
	"context"
	"log"
	"sync"
	"sync/atomic"
)

type operation int

//linked list of tranLog
var (
	logs   map[uint64]logEntry
	tailId uint64
	//control log linked list
	logMutex sync.Mutex
)

/**
维持元数据的日志
*/
const (
	// NodeAdd 增加节点
	NodeAdd operation = iota
	NodeDelete
	ShardAdd
	ShardDelete
)

var (
	id uint64 = 0
)

type logEntry[T node | int64] struct {
	//当前日志的id
	id uint64
	//前一条日志项的id
	PrevLogEntryId uint64
	//前面一条日志项的任期编号
	PrevLogTerm int64
	//操作类型
	operation operation
	//操作参数
	Object T
}

func NewNodeAddLog(n *node) *logEntry {

	return &logEntry[node]{
		atomic.AddUint64(&id, 1),
		0,
		State.selfState.term,
		NodeAdd,
		*n,
	}
}

//todo persistent logEntry
func (e *logEntry) persistent() {

}

//todo load log to State
func (e *logEntry) load2State() {
	switch e.operation {
	case NodeAdd:
		//State.clusterState.addNode(&node(e.Object))
	case NodeDelete:

	case ShardAdd:

	case ShardDelete:

	}
}

//Send log replication RPCs to other nodes,
//and determine the success based on the majority of candidate nodes
//this method called by Leader
func appendClusterLog(entry *logEntry) bool {
	var (
		//number of successes
		num uint32 = 0
		//number of done
		dnum uint32 = 0
		//mutex for controlling log replication results
		//todo there may be resource leakage here
		ch chan int
	)
	for i := range clients {
		c := clients[i]
		reply := &commonReply{}
		call := c.client.Go(context.Background(), "RaftServe", "AppendEntries", entry, reply, nil)
		//异步处理返回值
		go func() {
			if call.Error != nil {
				log.Fatal("arith error:", call.Error)
			}
			if c.node.isCandidate {
				//add the vote by CAS
				if reply.success {
					atomic.AddUint32(&num, 1)
				}
				//add the number of complete request ,notice the order of CAS here，dnum must behind of vote
				atomic.AddUint32(&dnum, 1)
				//When the votes are greater than the majority, or when the immediate failure is too many to reach the majority
				if num >= configs.Config.Cluster.ElectionMin ||
					dnum-num > uint32(len(State.clusterState.Nodes.candidateNodes))-configs.Config.Cluster.ElectionMin {
					ch <- 1
				}
			}
		}()
	}
	//goroutine will block until condition is reached
	<-ch
	if num < configs.Config.Cluster.ElectionMin {
		return false
	}
	//update log
	appendSelfLog(entry)
	return true
}

//append cur node log
func appendSelfLog(entry *logEntry) {
	logMutex.Lock()
	entry.PrevLogEntryId = tailId
	logs[entry.id] = *entry
	tailId = entry.id
	entry.persistent()
	logMutex.Unlock()
}
