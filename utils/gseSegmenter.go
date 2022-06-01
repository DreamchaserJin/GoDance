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

func (this *GseSegmenter) AddDict(file ...string) {
	this.segmenter.LoadDict(file...)
}

func (this *GseSegmenter) CutAll(text string) []string {
	return this.segmenter.CutAll(text)
}

func (this *GseSegmenter) Cut(text string, hmm ...bool) []string {
	return this.segmenter.Cut(text, hmm...)
}

func (this *GseSegmenter) CutSearch(text string, hmm ...bool) []string {
	return this.segmenter.CutSearch(text, hmm...)
}
