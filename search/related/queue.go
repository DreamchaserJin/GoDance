package related

import (
	"fmt"
	"sync"
)

// 定义数组队列的数据结构
type Queue struct {
	Array []*Trie
	word  [][]rune
	Size  int
	Lock  sync.Mutex
}

// 1. 入队操作
func (q *Queue) Add(v *Trie, pre []rune, ch rune) {
	q.Lock.Lock()
	defer q.Lock.Unlock()

	q.Array = append(q.Array, v)

	pre = append(pre, ch)
	q.word = append(q.word, pre)
	fmt.Println("pre", pre)
	fmt.Println("word", q.word)

	q.Size++
}

// 2. 出队操作
func (q *Queue) Remove() (*Trie, []rune) {
	q.Lock.Lock()
	defer q.Lock.Unlock()

	if q.Size == 0 {
		return nil, nil
	}

	v := q.Array[0]
	q.Array = q.Array[1:]

	fmt.Println("Remove", q.word)
	w := q.word[0]
	q.word = q.word[1:]
	q.Size--
	return v, w
}
