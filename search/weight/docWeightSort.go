package weight

import (
	gdindex "GoDance/index"
	"GoDance/utils"
	"math"
	"sort"
)

//
//  DocWeightSort
//  @Description: 将文档按照权重进行排序
//  @param docMergeFilter 布尔模型过滤后的相关文档
//  @param searchQueries  关键词集合
//  @param idx  倒排
//  @return []uint64
//
func DocWeightSort(docMergeFilter []uint64, notDocQueryNodes []utils.DocIdNode, searchQueries []utils.SearchQuery, idx *gdindex.Index) []uint64 {
	// IDF -> TFIDF -> 空间向量模型 -> 协调因子 -> 排序好的文档
	docLen := len(docMergeFilter)
	keyLen := len(searchQueries)
	docNum := float64(docLen)
	// 某个单词在对应文档中的TFIDF值
	//wordDocTFIDF := make(map[string][]utils.DocIdNode)
	// 搜索词向量权重
	vectorKey := make([]float64, docLen)
	// 所有文档向量
	vectorAllDoc := make(map[uint64][]float64, docLen)
	// 初始化类似于二维数组
	for _, id := range docMergeFilter {
		vectorAllDoc[id] = make([]float64, keyLen)
	}
	//协调因子 : 计算文档里出现的查询词个数 / 总个数
	coord := make(map[uint64]float64, 0)

	// 过滤词映射,过滤ids
	notMap := make(map[uint64]bool, 0)
	for _, v := range notDocQueryNodes {
		notMap[v.Docid] = true
	}

	// 向量空间模型
	for index, query := range searchQueries {
		ids, ok := idx.SearchKeyDocIds(query)
		if ok {
			// 过滤ids
			for i := 0; i < len(ids); {
				if notMap[ids[i].Docid] == true {
					// 删除过滤的文档
					ids = append(ids[:i], ids[i+1:]...)
				} else {
					i++
				}
			}
			// query.Value 对应的idf
			idf := math.Log(docNum / float64(len(ids)+1))
			var maxTFIDF float64
			for i := range ids {
				//ids[i].WordTF = ids[i].WordTF * idf
				TFIDF := ids[i].WordTF * idf
				maxTFIDF = math.Max(maxTFIDF, TFIDF)
				vectorAllDoc[ids[i].Docid][index] = TFIDF
				coord[ids[i].Docid] += float64(1 / keyLen)
			}
			//wordDocTFIDF[query.Value] = ids
			vectorKey[index] = maxTFIDF
		}
	}
	docVectorWeight := DocVectorWeight(vectorKey, vectorAllDoc)

	// 协调因子乘向量权重后排序
	var coordWeights utils.CoordWeightSort
	for k, v := range docVectorWeight {
		var cw utils.CoordWeight
		cw.DocId = k
		cw.Weight = v * coord[k]
		coordWeights = append(coordWeights, cw)
	}
	sort.Sort(coordWeights)
	// 排序后去掉权重只返回文档id
	docWeightSort := make([]uint64, 0)
	for _, v := range coordWeights {
		docWeightSort = append(docWeightSort, v.DocId)
	}
	return docWeightSort
}
