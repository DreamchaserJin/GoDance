package related

type RHeap []*Related

//小顶堆，按照单词频次、单词长度依次存储
type Related struct {
	Value     string
	Frequency uint64
}

func (h RHeap) Len() int      { return len(h) }
func (h RHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h RHeap) Less(i, j int) bool {
	if h[i].Frequency < h[j].Frequency {
		return true
	} else if h[i].Frequency == h[j].Frequency {
		return len(h[i].Value) > len(h[j].Value)
	} else {
		return false
	}
} // 小顶堆大小为10，超过则去掉频次最小且长度最大的

func (h *RHeap) Push(x interface{}) {
	*h = append(*h, x.(*Related))
}

func (h *RHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
