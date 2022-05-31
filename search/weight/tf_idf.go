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

/*************************************************************************
*  WordWeight： 计算单词的权重
*  TitleAndContentWeight： 一篇文章两种类型的权重和，比如标题，摘要，内容
************************************************************************/

func WordWeight(keyWords []string, listWords [][]string) WordTfIdfs {
	// 1. TF
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

	// 2. IDF
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
	// 3. TF * IDF
	var wordidfS WordTfIdfs
	for _, word := range keyWords {
		var wti WordTfIdf
		wti.Word = word
		wti.Value = wordTf[word] * wordIdf[word]
		wordidfS = append(wordidfS, wti)
	}
	return wordidfS
}

// BOOST: 第一种类型是第二种类型的权重倍数
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
