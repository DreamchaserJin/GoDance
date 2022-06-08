package utils

import (
	"bufio"
	"github.com/go-ego/gse"
	"io"
	"os"
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
	err = segmenter.LoadStop()
	if err != nil {
		panic(err.Error())
	}
	// 加载停用词
	fd, err := os.OpenFile(STOP_WORD_FILE_PATH, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	reader := bufio.NewReader(fd)
	stopWords := make([]string, 16)
	for {
		word, _, e := reader.ReadLine()
		if e == io.EOF {
			break
		}
		if e != nil {
			panic(err)
		}
		stopWords = append(stopWords, string(word))
	}
	err = gseSegmenter.segmenter.LoadStop()
	if err != nil {
		return
	}
	gseSegmenter.segmenter = segmenter
}

func GetGseSegmenter() GseSegmenter {
	return gseSegmenter
}

func AddStopWords() {

}

// AddDict
//  @Description: 添加字典
//  @receiver this
//  @param file 字典所在路径
func (this *GseSegmenter) AddDict(file ...string) {
	err := this.segmenter.LoadDict(file...)
	if err != nil {
		panic(err)
	}
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
	return this.segmenter.Stop(this.segmenter.CutSearch(text, hmm...))
}
