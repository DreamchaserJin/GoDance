package booleanmodel

import (
	"container/heap"
)

/*************************************************************************
*  FilterAndMerge ： 布尔模型接口
*  FilterAndMerge : 将包含过滤词的文档id去掉，合并所有的跟关键词相关文档
*  merge : 合并两个文档并去重
************************************************************************/

func DocMergeAndFilter(keyMap map[string][]int, filterMap map[string][]int) []int {
	m := len(filterMap)
	// 记录过滤词文档Id
	newMap := make(map[int]bool, m)
	for _, v := range filterMap {
		for _, docId := range v {
			newMap[docId] = true
		}
	}

	// 初始化小顶堆 ： 按照数组的长度排序
	h := &BMHeap{}
	heap.Init(h)

	// 所有关键词文档加入小顶堆
	for _, v := range keyMap {
		heap.Push(h, v)
	}

	// Merge
	for h.Len() > 1 {
		top1 := heap.Pop(h).([]int)
		top2 := heap.Pop(h).([]int)
		top1 = merge(top1, top2)
		heap.Push(h, top1)
	}
	keyMergeDoc := heap.Pop(h).([]int)

	// NOT操作
	for i := 0; i < len(keyMergeDoc); {
		if newMap[keyMergeDoc[i]] == true {
			// 删除过滤的文档
			keyMergeDoc = append(keyMergeDoc[:i], keyMergeDoc[i+1:]...)
		} else {
			i++
		}
	}

	return keyMergeDoc
}

func merge(nums1 []int, nums2 []int) []int {
	n := len(nums1)
	m := len(nums2)
	sorted := make([]int, 0)
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
