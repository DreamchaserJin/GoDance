package cluster

import (
	"GoDance/configs"
	"context"
	"log"
	"sync"
	"sync/atomic"
)

type operation int

var (
	//日志数组
	logs []LogEntry
	//日志id到数组索引下标的映射
	id2index = map[uint64]int{}
	//最近一次提交的日志id，当提交日志id一样时，意味着state肯定是一致的
	latestCommitted uint64
	//上一次提交的日志Id
	preCommitted uint64
	//用于控制日志增加的逻辑
	logMutex = sync.Mutex{}
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
	//主分片转移
	//
)

var (
	//TODO 当Leader节点切换时，需要找到最新的日志开始计数
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

// AppendSelfLog 自身节点增加一条日志记录,返回是否成功
// 从节点自身调用
func AppendSelfLog(entry *LogEntry, preId uint64) (res bool) {
	logMutex.Lock()
	defer logMutex.Unlock()
	//如果不存在才添加，保证幂等性
	if _, ok := id2index[entry.Id]; ok {
		return true
	}
	res = appendLogWithCheck(entry, preId)

	return res
}

// AppendSelfLogs 批量增加日志，当前已提交的日志开始覆盖之后的日志（不过这之前需要进行校验），返回执行结果
func AppendSelfLogs(entries []LogEntry, preCommitted uint64) (res bool) {
	logMutex.Lock()
	defer logMutex.Unlock()
	if latestCommitted != preCommitted {
		return false
	}
	base := id2index[preCommitted] + 1
	index := 0
	//找到最先不重复的下标
	for i, v := range entries {
		//如果找到则返回
		if v.Id != logs[i+base].Id {
			index = i
			break
		}
	}
	//删除后续的元素映射
	for i := index; i < len(entries); i++ {
		//删除id到下标映射
		delete(id2index, logs[index+base].Id)
	}
	//删除后续元素
	logs = logs[:base+index]
	//增加日志
	for i := index; i < len(entries); i++ {
		appendLog(&entries[i])
		if entries[i].Committed {
			entries[i].load2State()
		}
	}
	return true
}

// CommitLog 根据日志id提交日志到状态机中
// @id 提交日志id
// @preId 上一次提交日志id
// return res 是否提交成功 ;committedId 方法执行完成后的最新提交日志id
func CommitLog(id uint64, preId uint64) (res bool, committedId uint64) {
	logMutex.Lock()
	//只有前一个提交日志id一致才能提交
	if preId == latestCommitted {
		res = logs[id2index[id]].load2State()
		res = true
	}
	committedId = latestCommitted
	logMutex.Unlock()
	return
}

// Diff 用于比较当前节点和传入参数之前的日志，并返回对应的差异日志
//@committed 最近提交的id
//@return increasedLog 这期间增加的Log
func Diff(committed uint64) (increasedLog []LogEntry) {
	logMutex.Lock()
	defer logMutex.Unlock()
	//提交的索引下标
	c := id2index[committed]
	return logs[c+1:]
}

//todo persistent LogEntry ，只有MasterNode节点需要持久化日志
func (e *LogEntry) persistent() {

}

//todo load log to State
func (e *LogEntry) load2State() bool {
	//避免重复提交
	if e.Committed {
		return true
	}
	switch e.Operation {
	case NodeAdd:
		n, ok := (e.Object).(node)
		if !ok {
			log.Println("LogEntry Conversion failed")
			return false
		}
		State.clusterState.addNode(&n)
	case NodeDelete:
		id, ok := (e.Object).(int64)
		if !ok {
			log.Println("LogEntry Conversion failed")
			return false
		}
		State.clusterState.deleteNode(id)
		//todo 需要删除对应的client

	case ShardAdd:

	case ShardDelete:

	}
	e.Committed = true
	preCommitted = latestCommitted
	latestCommitted = e.Id
	return true
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
		num uint32 = 1
		//number of done
		dnum uint32 = 1
		//controlling log replication results
		ch = make(chan int, 1)
	)
	for i := range clients {
		c := clients[i]
		reply := &CommonReply{}
		call := c.client.Go(context.Background(), "RaftServe", "AppendEntry", entry, reply, nil)
		//异步处理返回值
		go func() {
			//TODO 需要对日志不一致的情况进行处理，要手动进行一次同步
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
	//避免单节点时出现永久堵塞情况
	if num >= configs.Config.Cluster.ElectionMin {
		ch <- 1
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

//增加一条日志，返回是否成功
//@entry 增加的日志
//@preId 前一个日志的id
func appendLogWithCheck(entry *LogEntry, preId uint64) bool {
	if logs[len(logs)-1].Id != preId {
		return false
	}
	appendLog(entry)
	return true
}

//增加一条日志，返回是否成功
func appendLog(entry *LogEntry) {
	entry.persistent()
	logs = append(logs, *entry)
	id2index[entry.Id] = len(logs) - 1
}
