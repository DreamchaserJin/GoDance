package main

import (
	"search"
	"search/weight"
)

func main() {
	weight.DocWeight(search.KeyWord(), search.Title(), search.Content(), search.KeyReverseIndex())
}
