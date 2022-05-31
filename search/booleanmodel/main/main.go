package main

import (
	"fmt"
	"search"
	"search/booleanmodel"
)

func main() {
	filterAndMerge := booleanmodel.DocMergeAndFilter(search.KeyReverseIndex(), search.FilterReverseIndex())
	fmt.Println(filterAndMerge)
}
