package cluster

type operation int

const (
	// NodeAdd 增加节点
	NodeAdd operation = iota
	NodeDelete
)

type Entry struct {
	//当前日志的id
	id int64
	//前一条日志项的id
	PrevLogEntry int64
	//前面一条日志项的任期编号
	PrevLogTerm int64
	//操作类型
	operation operation
	//操作参数
	Object interface{}
}
