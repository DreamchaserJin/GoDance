package weight

import (
	"fmt"
)

// 标题权重
const TITLEBOOST float64 = 4

// 摘要权重
const SUMMARYBOOST float64 = 2

/*************************************************************************
*  docWeight : 计算文档权重的接口
************************************************************************/

func DocWeight(keyWords []string, TitleWords [][]string, ContentWords [][]string, KeyReverseIndex map[string][]int) {

	// TF-IDF
	// title、content权重，然后两者权重和就是TF-IDF
	titleTf := TF(keyWords, TitleWords)
	titleIdf := IDF(keyWords, TitleWords)
	titleWeight := WordWeight(titleTf, titleIdf)
	wordTf := TF(keyWords, ContentWords)
	wordIdf := IDF(keyWords, ContentWords)
	contentWeight := WordWeight(wordTf, wordIdf)
	TFIDFWeight := TwoWeightSum(titleWeight, contentWeight, TITLEBOOST)
	fmt.Println("TF-IDF:", TFIDFWeight)

	// 向量模型
	docPosition := VectorPosition(TFIDFWeight, KeyReverseIndex)
	keyPosition := KeyVectorPosition(TFIDFWeight)
	docVectorWeight := DocVectorWeight(docPosition, keyPosition)
	fmt.Println("Vector : ", docVectorWeight)

	// 协调因子
	coordWeights := CoordAndVectorWeight(docVectorWeight, TFIDFWeight, KeyReverseIndex)
	fmt.Println("coord : ", coordWeights)
}
