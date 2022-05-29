package weight

import (
	"math"
)

// title比content的权重倍数
const TITLEBOOST = 2

// 关键词与TF-IDF
type WordTfIdf struct {
	Word  string
	Value float64
}

type WordTfIdfs []WordTfIdf

/*************************************************************************
*  TF-IDF : 计算单词 t 与 文档 d 之间的关联度
*  WordWeight： 计算单词的权重
*  TitleAndContentWeight： 计算标题与内容的权重和
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
func TitleAndContentWeight(titleWeight WordTfIdfs, contentWeight WordTfIdfs) WordTfIdfs {
	var sumWeight WordTfIdfs
	for i := range contentWeight {
		var wti WordTfIdf
		wti.Word = titleWeight[i].Word
		wti.Value = titleWeight[i].Value*TITLEBOOST + contentWeight[i].Value
		sumWeight = append(sumWeight, wti)
	}
	return sumWeight
}
