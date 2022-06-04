package main

import (
	"GoDance/search"
	"GoDance/search/booleanmodel"
	"fmt"
)

func main() {
	filterAndMerge := booleanmodel.DocMergeAndFilter(search.KeyReverseIndex(), search.FilterReverseIndex())
	fmt.Println(filterAndMerge)
}
