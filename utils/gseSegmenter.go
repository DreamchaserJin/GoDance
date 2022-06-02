package utils

import (
	"github.com/go-ego/gse"
)

type GseSegmenter struct {
	segmenter gse.Segmenter
}

var gseSegmenter GseSegmenter

func init() {
	segmenter, err := gse.New()
	if err != nil {
		panic(err.Error())
	}
	gseSegmenter.segmenter = segmenter
}

func GetGseSegmenter() GseSegmenter {
	return gseSegmenter
}

// AddDict
//  @Description: 添加字典
//  @receiver this
//  @param file 字典所在路径
func (this *GseSegmenter) AddDict(file ...string) {
	this.segmenter.LoadDict(file...)
}

// CutAll
//  @Description: 全模式分词
//  @receiver this
//  @param text
//  @return []string 分词结果
func (this *GseSegmenter) CutAll(text string) []string {
	return this.segmenter.CutAll(text)
}

// Cut
//  @Description: 分词
//  @receiver this
//  @param text
//  @param 如果hmm为空，则为普通模式，如果为false则使用dag不使用hmm分词，如果为true则使用dag和hmm分词
//  @return []string 分词结果
func (this *GseSegmenter) Cut(text string, hmm ...bool) []string {
	return this.segmenter.Cut(text, hmm...)
}

// CutSearch
//  @Description: 搜索引擎模式分词
//  @receiver this
//  @param text
//  @param hmm
//  @return []string
func (this *GseSegmenter) CutSearch(text string, hmm ...bool) []string {
	return this.segmenter.CutSearch(text, hmm...)
}
