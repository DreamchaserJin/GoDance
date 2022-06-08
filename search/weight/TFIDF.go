package weight

const TITLEBOOST = 4

// 关键词与TF-IDF
type WordTfIdf struct {
	Word  string
	Value float64
}

type WordTfIdfs []WordTfIdf

//
//  TF
//  @Description: 统计一段文章的词频
//  @param listWords
//  @return map[string]float64
//
func TF(listWords []string) map[string]float64 {
	// 用map统计单词出现次数
	docFrequency := make(map[string]float64, 0)
	sumWorlds := 0
	for _, word := range listWords {
		docFrequency[word] += 1
		sumWorlds++
	}

	// 计算TF词频： 关键词 / 所有单词
	wordTf := make(map[string]float64)
	for word := range docFrequency {
		wordTf[word] = docFrequency[word] / float64(sumWorlds)
	}
	return wordTf
}

//
//  TwoWeightSum
//  @Description: 一篇文章两种类型的权重和，比如标题，摘要，内容；BOOST控制倍数
//  @param WeightType1
//  @param WeightType2
//  @param BOOST
//  @return WordTfIdfs
//
func TwoWeightSum(WeightType1 WordTfIdfs, WeightType2 WordTfIdfs, BOOST float64) WordTfIdfs {
	var sumWeight WordTfIdfs
	// 都是以关键词的顺序进行遍历
	for i := range WeightType2 {
		var wti WordTfIdf
		wti.Word = WeightType1[i].Word
		wti.Value = WeightType1[i].Value*BOOST + WeightType2[i].Value
		sumWeight = append(sumWeight, wti)
	}
	return sumWeight
}
