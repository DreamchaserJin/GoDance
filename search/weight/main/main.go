package test

import (
	"fmt"
	"search"
	"search/weight"
	"time"
)

func main() {
	fmt.Println()
	start := currentTimeMillis()

	// TF-IDF
	// title、content权重，然后两者权重和就是TF-IDF
	titleWeight := weight.WordWeight(search.KeyWord(), search.Title())
	contentWeight := weight.WordWeight(search.KeyWord(), search.Content())
	TFIDFWeight := weight.TitleAndContentWeight(titleWeight, contentWeight)
	fmt.Println("TF-IDF:", TFIDFWeight)

	// 向量模型
	docPosition := weight.VectorPosition(TFIDFWeight, search.KeyReverseIndex())
	keyPosition := weight.KeyVectorPosition(TFIDFWeight)
	docVectorWeight := weight.DocVectorWeight(docPosition, keyPosition)
	//fmt.Println("Vector : ", docVectorWeight)

	// 协调因子
	coordWeights := weight.CoordAndVectorWeight(docVectorWeight, TFIDFWeight, search.KeyReverseIndex())
	fmt.Println("coord : ", coordWeights)

	cost := currentTimeMillis() - start
	fmt.Printf("耗时 %d ms \n", cost)
}

// 当前时间
func currentTimeMillis() int64 {
	return time.Now().UnixNano() / 1000000
}
