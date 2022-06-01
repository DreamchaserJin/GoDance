package main

import (
	"fmt"
	"search/related"
)

func main() {
	t := related.Constructor()
	t.Insert("字节跳动")
	t.Insert("字节跳动")
	t.Insert("字节跳动啊")
	t.Insert("字节跳动2")
	t.Insert("字节跳动3")
	t.Insert("字节跳动4")
	t.Insert("字节跳动5")
	res := t.Search("字节跳动")
	fmt.Println(res)

}
