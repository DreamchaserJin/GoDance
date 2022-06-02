package weight

import (
	"math"
)

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
//  IDF
//  @Description: 统计搜索词的IDF
//  @param keyWords
//  @param docMaps : 搜索词相关的多篇文章的map权重数组
//  @return map[string]float64
//
func IDF(keyWords []string, docMaps []map[string]float64) map[string]float64 {
	docNum := float64(len(docMaps))
	wordIdf := make(map[string]float64)
	// 统计每个关键词在几篇文章中
	wordDoc := make(map[string]float64, 0)
	for _, word := range keyWords {
		// 遍历每一篇文章的序号，看是否在该文章中存在
		for _, Maps := range docMaps {
			if _, ok := Maps[word]; ok {
				wordDoc[word]++
				break
			}
		}
	}
	// 计算IDF逆文件频率：log(总数/这个关键词出现的文章数)
	for _, word := range keyWords {
		wordIdf[word] = math.Log(docNum / (wordDoc[word] + 1))
	}
	return wordIdf
}

//
//  WordWeight
//  @Description: 计算单词的TF-IDF权重
//  @param keyWords
//  @param listWords
//  @return WordTfIdfs
//
func WordWeight(wordTf map[string]float64, wordIdf map[string]float64) WordTfIdfs {
	// 3. TF * IDF
	var wordidfS WordTfIdfs
	for word := range wordIdf {
		var wti WordTfIdf
		wti.Word = word
		wti.Value = wordTf[word] * wordIdf[word]
		wordidfS = append(wordidfS, wti)
	}
	return wordidfS
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
