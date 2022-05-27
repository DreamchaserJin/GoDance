package log

type operation int

const (
	// NodeAdd 增加节点
	NodeAdd operation = iota
	NodeDelete
)

type Log struct {
	operation operation
}
