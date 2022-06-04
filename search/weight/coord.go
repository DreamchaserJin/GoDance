package weight

import (
	"sort"
)

type CoordWeight struct {
	DocId  uint64
	Weight float64
}

// 使用sort.Sort需要实现接口的三个方法：Len(),Less(),Swap()
type CoordWeights []CoordWeight
type Interface interface {
	Len() int
	Less(i, j int) bool
	Swap(i, j int)
}

func (cw CoordWeights) Len() int {
	return len(cw)
}
func (cw CoordWeights) Less(i, j int) bool {
	return cw[i].Weight > cw[j].Weight
}
func (cw CoordWeights) Swap(i, j int) {
	cw[i], cw[j] = cw[j], cw[i]
}

/*************************************************************************
*  协调因子 : 计算文档里出现的查询词个数 / 总个数
*  CoordAndVectorWeight： 计算协调因子与所有向量的乘积
************************************************************************/

func CoordAndVectorWeight(docVectorWeight map[int]float64, keyWordIdfs WordTfIdfs, reverseIndex map[string][]int) CoordWeights {
	keyLen := len(keyWordIdfs)
	// 一篇文档包含出现几种关键词
	docKeyCount := make(map[int]float64, 0)
	for _, wordIdfs := range keyWordIdfs {
		// 遍历关键词对应的所有文档
		for _, docId := range reverseIndex[wordIdfs.Word] {
			docKeyCount[docId]++
		}
	}

	var cws CoordWeights
	for k, v := range docVectorWeight {
		var cw CoordWeight
		cw.DocId = uint64(k)
		cw.Weight = v * docKeyCount[k] / float64(keyLen)
		cws = append(cws, cw)
	}
	sort.Sort(cws)
	return cws
}
