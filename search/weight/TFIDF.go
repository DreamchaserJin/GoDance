package weight

// 标题与内容权重的倍数
const TITLEBOOST = 10

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
