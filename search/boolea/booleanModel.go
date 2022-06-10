package boolea

import (
	"GoDance/utils"
)

//
//  DocAndNot
//  @Description: 过滤掉过滤词文档
//  @param keyFilter
//  @param notDocQueryNodes
//  @return []uint64
//
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

func Intersection2DocIdAndUint64(docs1 []utils.DocIdNode, docs2 []uint64) []utils.DocIdNode {
	n := len(docs1)
	m := len(docs2)
	sorted := make([]utils.DocIdNode, 0)
	p1, p2 := 0, 0
	for {
		if p1 == n || p2 == m {
			break
		}
		if docs1[p1].Docid < docs2[p2] {
			p1++
		} else if docs1[p1].Docid > docs2[p2] {
			p2++
		} else {
			sorted = append(sorted, docs1[p1])
			p1++
			p2++
		}
	}

	return sorted
}
