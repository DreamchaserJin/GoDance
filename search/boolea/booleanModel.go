package boolea

import (
	"GoDance/utils"
)

//
//  DocMergeAndFilter
//  @Description: 布尔模型接口，合并搜索词和范围查询的文档id，过滤掉过滤词的文档id
//  @param docQueryNodes   		搜索词文档id
//  @param docFilterIds    		发内查询文档id
//  @param notDocQueryNodes     过滤词文档id
//  @return []uint64			返回搜索词相关的文档
//
func DocMergeFilter(docQueryNodes []utils.DocIdNode, docFilterIds []uint64, notDocQueryNodes []utils.DocIdNode) []uint64 {
	// 取出[]uint64
	nums1 := make([]uint64, 0)
	for _, v := range docQueryNodes {
		nums1 = append(nums1, v.Docid)
	}
	// 合并搜索词与范围查找的文档
	docMerge := merge(nums1, docFilterIds)

	// 记录要过滤的id
	notMap := make(map[uint64]bool, 0)
	for _, v := range notDocQueryNodes {
		notMap[v.Docid] = true
	}
	// NOT 有过滤的id则删除
	for i := 0; i < len(docMerge); {
		if notMap[docMerge[i]] == true {
			// 删除过滤的文档
			docMerge = append(docMerge[:i], docMerge[i+1:]...)
		} else {
			i++
		}
	}
	return docMerge
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
//  merge
//  @Description: 合并并去重
//  @param nums1  要合并的文档
//  @param nums2  要合并的文档
//  @return []int   返回合并后的文档
//
func merge(nums1 []uint64, nums2 []uint64) []uint64 {
	n := len(nums1)
	m := len(nums2)
	sorted := make([]uint64, 0)
	p1, p2 := 0, 0
	for {
		if p1 == n {
			sorted = append(sorted, nums2[p2:]...)
			break
		}
		if p2 == m {
			sorted = append(sorted, nums1[p1:]...)
			break
		}
		if nums1[p1] < nums2[p2] {
			sorted = append(sorted, nums1[p1])
			p1++
		} else if nums1[p1] > nums2[p2] {
			sorted = append(sorted, nums2[p2])
			p2++
		} else {
			sorted = append(sorted, nums1[p1])
			p1++
			p2++
		}
	}
	return sorted
}
