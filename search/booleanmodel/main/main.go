package main

import (
	"fmt"
	"search"
	"search/booleanmodel"
)

func main() {
	fmt.Println()
	filterAndMerge := booleanmodel.DocMergeAndFilter(search.KeyReverseIndex(), search.FilterReverseIndex())
	fmt.Println(filterAndMerge)
}
