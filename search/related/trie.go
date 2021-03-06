package related

import (
	"GoDance/utils"
	"bufio"
	"container/heap"
	"io"
	"os"
)

// 字典树
type Trie struct {
	// rune代表一个中文字符
	Children  map[rune]*Trie
	IsWord    bool
	Frequency uint64
}

//
//  Constructor
//  @Description: 添加接口，创建字典树
//  @return Trie
//
func Constructor(triePath string) Trie {

	var trieTree = Trie{}

	// 初始化trie树，将triePath文件下的搜索词插入到字典树中
	fd, err := os.OpenFile(triePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	reader := bufio.NewReader(fd)
	for {
		word, _, e := reader.ReadLine()
		if e == io.EOF {
			break
		}
		if e != nil {
			panic(err)
		}
		// 插入操作
		trieTree.Insert(string(word))
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
		if node.Children == nil {
			node.Children = make(map[rune]*Trie)
		}
		if node.Children[ch] == nil {
			node.Children[ch] = &Trie{}
		}
		node = node.Children[ch]
	}
	node.Frequency++
	node.IsWord = true
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
		relaters := make([]string, 0)
		// 先搜索相关词
		relaters = t.searchWord(words)
		if len(relaters) == 10 {
			return relaters
		}

		// 如果没满，搜分词
		segmenter := utils.GetGseSegmenter()
		terms := segmenter.CutSearch(words, false)

		for i := 0; i < len(terms); i++ {
			relaters = append(relaters, t.searchWord(terms[i])...)
			if len(relaters) >= 10 {
				return relaters[:10]
			}
		}

		// 搜不满从trie根搜
		for k := range t.Children {
			relaters = append(relaters, t.searchWord(string(k))...)
			if len(relaters) >= 10 {
				return relaters
			}
		}
		// 总数据>=10就不会到这一步
		return relaters
	}
	// 如果是实时搜索立马返回
	return t.searchWord(words)
}

//
//  searchWord
//  @Description: 查询以words开头的单词,返回结果 <= 10个
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
		if no.IsWord == true {
			temp := &Related{
				Frequency: no.Frequency,
				Value:     string(ru),
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
		for k, v := range no.Children {
			q.Add(v, ru, k)
		}
	}
	// 取元素返回
	relaters := make([]string, h.Len())
	for h.Len() > 0 {
		r := heap.Pop(h).(*Related)
		// 因为是小顶堆，堆顶最小，所以逆序存储
		relaters[h.Len()] = r.Value
	}
	if len(relaters) > 10 {
		return relaters[:10]
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
		if node.Children[ch] == nil {
			return nil
		}
		node = node.Children[ch]
	}
	return node
}
