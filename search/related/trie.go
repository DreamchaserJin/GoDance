package related

import (
	"GoDance/utils"
	"container/heap"
)

type Trie struct {
	// rune代表一个中文字符
	children  map[rune]*Trie
	isWord    bool
	frequency uint64
}

//
//  Constructor
//  @Description: 添加接口，创建字典树
//  @return Trie
//
func Constructor(triePath string) Trie {

	var trieTree = Trie{}

	// todo 反序列化
	if utils.Exist(triePath) {

	}

	return trieTree
}

//
//  Insert
//  @Description: 添加单词到字典树
//  @receiver t
//  @param words
//
func (t *Trie) Insert(words string) {
	node := t
	for _, ch := range words {
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
//  @Description: 搜索接口
//  @receiver t
//  @param words  查询的语句
//  @param BOOL   为true代表实时搜索，为false代表相关搜索
//  @return []string  实时搜索个数<= 10， 相关搜索个数肯定为10个
//
func (t *Trie) Search(words string, BOOL bool) []string {
	// 如果BOOL为false则是相关搜索
	if BOOL == false {
		isOne := true
		relaters := make([]string, 0)
		// 分词
		segmenter := utils.GetGseSegmenter()
		terms := segmenter.CutSearch(words, false)

		for i := 0; i < len(terms); {
			// 如果是第一次就搜索原搜索词words
			if isOne == true {
				relaters = t.searchWord(words)
				if len(relaters) < 10 {
					isOne = false
				} else {
					return relaters
				}
			} else {
				// 不是第一次就搜索分词,然后添加到末尾
				relaters = append(relaters, t.searchWord(terms[i])...)
				if len(relaters) >= 10 {
					return relaters[:10]
				}
				i++
			}
		}
		// 到这里说明肯定不足10个，直接补全words算了
		for len(relaters) < 10 {
			relaters = append(relaters, words)
		}
	}
	// 如果是实时搜索立马返回
	return t.searchWord(words)
}

//
//  searchWord
//  @Description: 查询以words开头的单词
//  @receiver t
//  @param words
//  @return []string
//
func (t *Trie) searchWord(words string) []string {
	n := len([]rune(words))
	h := &RHeap{}
	heap.Init(h)
	node := t.searchPrefix(words)

	if node == nil {
		return nil
	}
	// 广搜
	count := 0
	q := &Queue{}
	q.Add(node, []rune(words)[:n-1], []rune(words)[n-1])
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
	relaters := make([]string, h.Len())
	for h.Len() > 0 {
		r := heap.Pop(h).(*Related)
		// 因为是小顶堆，堆顶最小，所以逆序存储
		relaters[h.Len()] = r.value
	}

	return relaters
}

//
//  SearchPrefix
//  @Description: 查找以prefix为前缀的单词的节点
//  @receiver t
//  @param prefix
//  @return *Trie   返回以prefix为前缀得到节点
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
