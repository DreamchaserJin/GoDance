package related

import (
	"sync"
)

// 队列
type Queue struct {
	Array []*Trie
	// 广搜过程中记录整个单词
	word [][]rune
	Size uint64
	Lock sync.Mutex
}

// 入队操作
func (q *Queue) Add(v *Trie, pre []rune, ch rune) {
	q.Lock.Lock()
	defer q.Lock.Unlock()

	q.Array = append(q.Array, v)
	// 不能再pre上原地修改,否则会覆盖word中上一次的值
	p := make([]rune, len(pre))
	copy(p, pre)
	p = append(p, ch)
	q.word = append(q.word, p)

	q.Size++
}

// 出队操作
func (q *Queue) Remove() (*Trie, []rune) {
	q.Lock.Lock()
	defer q.Lock.Unlock()

	if q.Size == 0 {
		return nil, nil
	}

	v := q.Array[0]
	q.Array = q.Array[1:]

	w := q.word[0]
	q.word = q.word[1:]
	q.Size--
	return v, w
}
