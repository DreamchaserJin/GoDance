package boolea

import (
	"GoDance/utils"
)

func DocAndNot(keyFilter []uint64, notDocQueryNodes []utils.DocIdNode) []uint64 {
	// 记录要过滤的id
	notMap := make(map[uint64]bool, 0)
	for _, v := range notDocQueryNodes {
		notMap[v.Docid] = true
	}
	// NOT 有过滤的id则删除
	for i := 0; i < len(keyFilter); {
		if notMap[keyFilter[i]] == true {
			// 删除过滤的文档
			keyFilter = append(keyFilter[:i], keyFilter[i+1:]...)
		} else {
			i++
		}
	}
	return keyFilter
}

//func DocMergeAndFilter1(keyMap map[string][]int, filterMap map[string][]int) []int {
//
//	// 初始化小顶堆 ： 按照数组的长度排序
//	h := &BMHeap{}
//	heap.Init(h)
//
//	// 所有关键词文档加入小顶堆
//	for _, v := range keyMap {
//		heap.Push(h, v)
//	}
//
//	// Merge
//	for h.Len() > 1 {
//		top1 := heap.Pop(h).([]int)
//		top2 := heap.Pop(h).([]int)
//		top1 = merge(top1, top2)
//		heap.Push(h, top1)
//	}
//	keyMergeDoc := heap.Pop(h).([]int)
//
//	m := len(filterMap)
//	// 记录过滤词文档Id
//	newMap := make(map[int]bool, m)
//	for _, v := range filterMap {
//		for _, docId := range v {
//			newMap[docId] = true
//		}
//	}
//
//	// NOT操作
//	for i := 0; i < len(keyMergeDoc); {
//		if newMap[keyMergeDoc[i]] == true {
//			// 删除过滤的文档
//			keyMergeDoc = append(keyMergeDoc[:i], keyMergeDoc[i+1:]...)
//		} else {
//			i++
//		}
//	}
//
//	return keyMergeDoc
//}

//
//  Union
//  @Description: 求并集
//  @param docs1
//  @param docs2
//  @return []int   返回合并后的文档
//
func UnionUint64(docs1 []uint64, docs2 []uint64) []uint64 {
	n := len(docs1)
	m := len(docs2)
	sorted := make([]uint64, 0)
	p1, p2 := 0, 0
	for {
		if p1 == n {
			sorted = append(sorted, docs2[p2:]...)
			break
		}
		if p2 == m {
			sorted = append(sorted, docs1[p1:]...)
			break
		}
		if docs1[p1] < docs2[p2] {
			sorted = append(sorted, docs1[p1])
			p1++
		} else if docs1[p1] > docs2[p2] {
			sorted = append(sorted, docs2[p2])
			p2++
		} else {
			sorted = append(sorted, docs1[p1])
			p1++
			p2++
		}
	}
	return sorted
}

//
//  UnionDocIdNode
//  @Description: 求并集
//  @param docs1
//  @param docs2
//  @return []utils.DocIdNode
//
func UnionDocIdNode(docs1 []utils.DocIdNode, docs2 []utils.DocIdNode) []utils.DocIdNode {
	n := len(docs1)
	m := len(docs2)
	sorted := make([]utils.DocIdNode, 0)
	p1, p2 := 0, 0
	for {
		if p1 == n {
			sorted = append(sorted, docs2[p2:]...)
			break
		}
		if p2 == m {
			sorted = append(sorted, docs1[p1:]...)
			break
		}
		if docs1[p1].Docid < docs2[p2].Docid {
			sorted = append(sorted, docs1[p1])
			p1++
		} else if docs1[p1].Docid > docs2[p2].Docid {
			sorted = append(sorted, docs2[p2])
			p2++
		} else {
			sorted = append(sorted, docs1[p1])
			p1++
			p2++
		}
	}
	return sorted
}

//
//  IntersectionUint64
//  @Description: 求交集
//  @param docs1
//  @param docs2
//  @return []uint64
//
func IntersectionUint64(docs1 []uint64, docs2 []uint64) []uint64 {
	n := len(docs1)
	m := len(docs2)
	sorted := make([]uint64, 0)
	p1, p2 := 0, 0
	for {
		if p1 == n || p2 == m {
			break
		}
		if docs1[p1] < docs2[p2] {
			p1++
		} else if docs1[p1] > docs2[p2] {
			p2++
		} else {
			sorted = append(sorted, docs1[p1])
			p1++
			p2++
		}
	}
	return sorted
}

//
//  IntersectionDocIdAndUint64
//  @Description: DocIdNode类型跟uint64 求交集
//  @param docs1
//  @param docs2
//  @return []uint64
//
func IntersectionDocIdAndUint64(docs1 []utils.DocIdNode, docs2 []uint64) []uint64 {
	Docs1 := utils.DocIdNodeChangeUint64(docs1)

	return IntersectionUint64(Docs1, docs2)
}
