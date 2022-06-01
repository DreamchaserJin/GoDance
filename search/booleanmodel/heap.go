package booleanmodel

type BMHeap [][]int

func (h BMHeap) Len() int           { return len(h) }
func (h BMHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h BMHeap) Less(i, j int) bool { return len(h[i]) < len(h[j]) } // 小顶堆

func (h *BMHeap) Push(x interface{}) {
	*h = append(*h, x.([]int))
}

func (h *BMHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
