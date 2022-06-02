package related

import (
	"container/heap"
)

type Trie struct {
	// rune代表一个字符
	children  map[rune]*Trie
	isWord    bool
	frequency int64
}

func Constructor() Trie {
	return Trie{}
}

func (t *Trie) Insert(word string) {
	node := t
	for _, ch := range word {
		if node.children == nil {
			node.children = make(map[rune]*Trie)
		}
		if node.children[ch] == nil {
			node.children[ch] = &Trie{}
		}
		node = node.children[ch]
	}
	node.isWord = true
}

func (t *Trie) Search(word string) []string {
	n := len([]rune(word))
	h := &RHeap{}
	heap.Init(h)
	node := t.SearchPrefix(word)

	if node == nil {
		return nil
	}
	if node.isWord == true {
		temp := &Related{
			frequency: node.frequency,
			value:     word,
		}
		heap.Push(h, temp)
	}
	// 广搜
	q := &Queue{}
	preWord := string([]rune(word)[:n-1])
	q.Add(node, nil, []rune(word)[n-1])
	count := 0
	for q.Size > 0 {
		no, ru := q.Remove()
		//fmt.Println(string(ru), no, 1)
		if no.isWord == true {
			temp := &Related{
				frequency: no.frequency,
				value:     preWord + string(ru),
			}
			heap.Push(h, temp)
			count++
		}
		if h.Len() >= 10 {
			heap.Pop(h)
		}
		if count >= 50 {
			break
		}
		for k, v := range no.children {
			ru = q.Add(v, ru, k)
		}
	}

	relateds := make([]string, 10)
	for h.Len() > 0 {
		r := heap.Pop(h).(*Related)
		relateds = append(relateds, r.value)
	}
	return relateds
}

//
//  SearchPrefix
//  @Description: 查找以prefix为前缀的单词的树
//  @receiver t
//  @param prefix
//  @return *Trie
//
func (t *Trie) SearchPrefix(prefix string) *Trie {
	node := t
	for _, ch := range prefix {
		if node.children[ch] == nil {
			return nil
		}
		node = node.children[ch]
	}
	return node
}
