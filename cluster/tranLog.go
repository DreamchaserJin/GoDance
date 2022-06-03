package cluster

import (
	"GoDance/configs"
	"context"
	"log"
	"sync"
	"sync/atomic"
)

type operation int

//linked list of tranLog
var (
	//日志数组
	logs []LogEntry
	//日志id到数组索引下标的映射
	id2index map[uint64]int
	//上一次提交的日志id，当提交日志id一样时，意味着state肯定是一致的
	latestCommitted uint64
	//最近一次增加的日志id
	latestLog uint64
	//用于控制日志增加的逻辑
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

type LogEntry struct {
	//当前日志的id，这个是递增的
	Id uint64
	//日志任期
	Term int64
	//操作类型
	Operation operation
	//操作参数
	Object any
	//是否已经提交
	Committed bool
}

// AddNode  增加一个节点，这里的逻辑保证Leader节点中每一条记录都是最终的
//这里只能由领导者调用
func AddNode(n *node) bool {
	return appendClusterLog(NodeAdd, *n)
}

// DeleteNode  删除一个节点
// 这里只能由领导者调用
func DeleteNode(nodeId int64) bool {
	return appendClusterLog(NodeDelete, nodeId)
}

// AppendSelfLog 自身节点增加一条日志记录
// 从节点自身调用
func AppendSelfLog(entry *LogEntry) {
	logMutex.Lock()
	//入股不存在才添加，保证幂等性
	if _, ok := id2index[entry.Id]; !ok {
		appendLog(entry)
	}
	logMutex.Unlock()
}

// CommitLog 根据日志id提交日志到状态机中
func CommitLog(id uint64) {
	logMutex.Lock()
	logs[id2index[id]].load2State()
	logMutex.Unlock()
}

// Diff 用于比较当前节点和传入参数之前的日志，并返回对应的差异
//@committed 最近提交的id
//@logId 最近增加的日志id
//@return committedIds 差异提交的id数组， increasedLog 这期间增加的Log
func Diff(committed uint64, logId uint64) (committedIds []uint64, increasedLog []LogEntry) {
	logMutex.Lock()
	defer logMutex.Unlock()
	//提交的索引下标
	c := id2index[committed]
	//增加的日志下标
	l := id2index[logId]
	for i := c + 1; i < len(logs); i += 1 {
		if logs[i].Committed {
			committedIds = append(committedIds, logs[i].Id)
		}
		if i > l {
			increasedLog = append(increasedLog, logs[i])
		}
	}
	return committedIds, increasedLog
}

//todo persistent LogEntry
func (e *LogEntry) persistent() {

}

//todo load log to State
func (e *LogEntry) load2State() {
	//避免重复提交
	if e.Committed {
		return
	}
	switch e.Operation {
	case NodeAdd:
		n, ok := (e.Object).(node)
		if ok {
			log.Println("LogEntry Conversion failed")
		}
		State.clusterState.addNode(&n)
	case NodeDelete:
		id, ok := (e.Object).(int64)
		if ok {
			log.Println("LogEntry Conversion failed")
		}
		State.clusterState.deleteNode(id)
	case ShardAdd:

	case ShardDelete:

	}
	e.Committed = true
	latestCommitted = e.Id
}

//Send log replication RPCs to other nodes,
//and determine the Success based on the majority of candidate nodes
//this method called by Leader
func appendClusterLog(op operation, object any) bool {
	logMutex.Lock()
	defer logMutex.Unlock()
	id += 1
	entry := LogEntry{
		id,
		State.selfState.term,
		op,
		object,
		false,
	}
	//update log ，there must first appendSelfLog  to ensure that the leader node' log must be latest
	appendLog(&entry)
	var (
		//number of successes
		num uint32 = 0
		//number of done
		dnum uint32 = 0
		//controlling log replication results
		//todo there may be resource leakage here
		ch chan int
	)
	for i := range clients {
		c := clients[i]
		reply := &CommonReply{}
		call := c.client.Go(context.Background(), "RaftServe", "AppendEntry", entry, reply, nil)
		//异步处理返回值
		go func() {
			if call.Error != nil {
				log.Println("arith error:", call.Error)
			}
			if c.node.isMasterNode {
				//add the vote by CAS
				if reply.Success {
					atomic.AddUint32(&num, 1)
				}
				//add the number of complete request ,notice the order of CAS here，dnum must behind of vote
				atomic.AddUint32(&dnum, 1)
				//When the votes are greater than the majority, or when the immediate failure isMasterNode too many to reach the majority
				if num >= configs.Config.Cluster.ElectionMin ||
					dnum-num > uint32(len(State.clusterState.Nodes.masterNodes))-configs.Config.Cluster.ElectionMin {
					ch <- 1
				}
			}
		}()
	}
	//goroutine will block until condition isMasterNode reached
	<-ch
	//如果没达到大多数，则该日志失败，不进行提交，直接返回失败结果
	if num < configs.Config.Cluster.ElectionMin {
		return false
	}
	entry.load2State()
	return true
}

func appendLog(entry *LogEntry) {
	entry.persistent()
	logs = append(logs, *entry)
	latestLog = entry.Id
	id2index[entry.Id] = len(logs) - 1
}
