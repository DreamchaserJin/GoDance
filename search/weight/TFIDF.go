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

func TF(keyWords []string, listWords [][]string) map[string]float64 {
	// 用map统计单词出现次数
	docFrequency := make(map[string]float64, 0)
	sumWorlds := 0
	for _, wordList := range listWords {
		for _, v := range wordList {
			docFrequency[v] += 1
			sumWorlds++
		}
	}

	// 计算TF词频： 关键词 / 所有单词
	wordTf := make(map[string]float64)
	for _, word := range keyWords {
		wordTf[word] = docFrequency[word] / float64(sumWorlds)
	}
	return wordTf
}

func IDF(keyWords []string, listWords [][]string) map[string]float64 {
	docNum := float64(len(listWords))
	wordIdf := make(map[string]float64)
	// 统计每个关键词在几篇文章中
	wordDoc := make(map[string]float64, 0)
	for _, word := range keyWords {
		for _, v := range listWords {
			for _, vs := range v {
				if word == vs {
					wordDoc[word] += 1
					break
				}
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
	for word := range wordTf {
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
