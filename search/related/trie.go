package related

import (
	"container/heap"
)

type Trie struct {
	// rune代表一个中文字符
	children  map[rune]*Trie
	isWord    bool
	frequency int64
}

//
//  Constructor
//  @Description: 添加接口，创建字典树
//  @return Trie
//
func Constructor() Trie {
	return Trie{}
}

//
//  Insert
//  @Description: 添加单词到字典树
//  @receiver t
//  @param word
//
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
	node.frequency++
	node.isWord = true
}

//
//  Search
//  @Description: 查询接口，查询以word开头的单词
//  @receiver t
//  @param word
//  @return []string
//
func (t *Trie) Search(word string) []string {
	n := len([]rune(word))
	h := &RHeap{}
	heap.Init(h)
	node := t.searchPrefix(word)

	if node == nil {
		return nil
	}
	// 广搜
	count := 0
	q := &Queue{}
	q.Add(node, []rune(word)[:n-1], []rune(word)[n-1])
	for q.Size > 0 {
		no, ru := q.Remove()
		if no.isWord == true {
			temp := &Related{
				frequency: no.frequency,
				value:     string(ru),
			}
			heap.Push(h, temp)
			count++
		}
		if h.Len() > 10 {
			heap.Pop(h)
		}
		if count >= 50 {
			break
		}
		for k, v := range no.children {
			q.Add(v, ru, k)
		}
	}
	// 取元素返回
	relateds := make([]string, h.Len())
	for h.Len() > 0 {
		r := heap.Pop(h).(*Related)
		// 因为是小顶堆，堆顶最小，所以逆序存储
		relateds[h.Len()] = r.value
	}

	return relateds
}

//
//  SearchPrefix
//  @Description: 查找以prefix为前缀的单词的节点
//  @receiver t
//  @param prefix
//  @return *Trie
//
func (t *Trie) searchPrefix(prefix string) *Trie {
	node := t
	for _, ch := range prefix {
		if node.children[ch] == nil {
			return nil
		}
		node = node.children[ch]
	}
	return node
}
