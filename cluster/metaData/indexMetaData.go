package metaData

type State int

const (
	CLOSE State = iota
	OPEN  State = iota
)

// IndexMetaData 具体某个Index的Meta，比如这个Index的shard数，replica数，mappings等
type IndexMetaData struct {
	//当前版本号，每次更新加1
	version int64
	//用于routing的shard数, 只能是该Index的numberOfShards的倍数，用于split
	routingNumShards int64
	//Index的状态, 值是OPEN或CLOSE
	state State
	//numbersOfShards，numbersOfRepilicas等配置
	settings map[string]string
	//primaryTerm在每次Shard切换Primary时加1，用于保序
	primaryTerms []int64
	//inSyncAllocationIds：处于InSync状态的AllocationId，用于保证数据一致性
	inSyncAllocationIds map[string]map[string]string
}
