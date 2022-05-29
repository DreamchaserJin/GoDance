package weight

import (
	"math"
)

/*************************************************************************
*  向量空间模型：计算关键词 q 与每一个文档中含有的 q 有关的 t 之间的夹角余弦值
*  VectorCosine:  得出两个向量之间的夹角
*  VectorPosition： 获取所有文档的向量权重
*  KeyVectorPosition：获取key的向量
*  DocVectorWeight： 获取所有文档与关键词向量的余弦值
************************************************************************/

func VectorCosine(a []float64, b []float64) float64 {
	var (
		aLen  = len(a)
		bLen  = len(b)
		s     = 0.0
		sa    = 0.0
		sb    = 0.0
		count = 0
	)
	if aLen > bLen {
		count = aLen
	} else {
		count = bLen
	}
	for i := 0; i < count; i++ {
		if i >= bLen {
			sa += math.Pow(a[i], 2)
			continue
		}
		if i >= aLen {
			sb += math.Pow(b[i], 2)
			continue
		}
		s += a[i] * b[i]
		sa += math.Pow(a[i], 2)
		sb += math.Pow(b[i], 2)
	}
	return s / (math.Sqrt(sa) * math.Sqrt(sb))
}

// 获取所有文档的向量
func VectorPosition(keyWordIdfs WordTfIdfs, reverseIndex map[string][]int) map[int][]float64 {
	n := len(reverseIndex)
	m := len(keyWordIdfs)
	position := make(map[int][]float64, n)
	for _, v := range reverseIndex {
		for _, doc := range v {
			position[doc] = make([]float64, m)
		}
	}

	// 遍历关键词
	for column, wordIdfs := range keyWordIdfs {
		// 遍历关键词对应的所有文档
		for _, row := range reverseIndex[wordIdfs.Word] {
			position[row][column] = wordIdfs.Value
		}
	}
	return position
}

// 获取key的向量
func KeyVectorPosition(keyWordIdfs WordTfIdfs) []float64 {
	m := len(keyWordIdfs)
	position := make([]float64, m)
	for k, v := range keyWordIdfs {
		position[k] = v.Value
	}
	return position
}

// 获取所有文档与关键词向量的余弦值
func DocVectorWeight(docPosition map[int][]float64, keyPosition []float64) map[int]float64 {
	docVectorWeight := make(map[int]float64)
	for k, v := range docPosition {
		docVectorWeight[k] = VectorCosine(keyPosition, v)
	}
	return docVectorWeight
}
