package weight

import (
	"math"
)

/*************************************************************************
*  向量空间模型：计算关键词 q 与每一个文档中含有的 q 有关的 t 之间的夹角余弦值
*  VectorCosine:  得出两个向量之间的夹角
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

// 获取所有文档与关键词向量的余弦值
func DocVectorWeight(vectorKey []float64, vectorAllDoc map[uint64][]float64) map[uint64]float64 {
	docVectorWeight := make(map[uint64]float64)
	for k, v := range vectorAllDoc {
		docVectorWeight[k] = VectorCosine(vectorKey, v)
	}
	return docVectorWeight
}
