package related

type RHeap []*Related

type Related struct {
	value     string
	frequency int64
}

func (h RHeap) Len() int      { return len(h) }
func (h RHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h RHeap) Less(i, j int) bool {
	if h[i].frequency > h[j].frequency {
		return true
	} else if h[i].frequency == h[j].frequency {
		return len(h[i].value) < len(h[j].value)
	} else {
		return false
	}
} // 先按照频次排，相等则按照字符串长度排

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
