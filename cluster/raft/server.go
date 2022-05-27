package raft

import (
	"math/rand"
	"time"
)

//尝试选举自己为主节点
//选举策略：像候选主节点发送拉票RPC消息，超过配置中设置的则选举成功
func tryElection() {
	//todo 选举
	randomElection := rand.Intn(150) + 150
	time.Sleep(time.Duration(randomElection))
}

//尝试加入节点
func tryJoin() {

}
