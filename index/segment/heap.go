package segment

import (
	"github.com/blevesearch/vellum"
	"strings"
)

type FstHeap []*FstNode

type FstNode struct {
	Key  string
	ivt  *invert
	Iter *vellum.FSTIterator
}

func (h FstHeap) Len() int { return len(h) }
func (h FstHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h FstHeap) Less(i, j int) bool { return strings.Compare(h[i].Key, h[j].Key) < 0 } // 小顶堆

func (h *FstHeap) Push(x interface{}) {
	*h = append(*h, x.(*FstNode))
}

func (h *FstHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
