package main

import (
	"GoDance/search/related"
	"fmt"
)

func main() {
	t := related.Constructor()
	t.Insert("字节跳动")
	t.Insert("字节跳动第一")
	t.Insert("字节跳动第一")
	t.Insert("字节跳动第一")
	t.Insert("字节跳动第二")
	t.Insert("字节跳动第二")
	t.Insert("字节跳动公司简介")
	t.Insert("字节跳动创始人")
	t.Insert("字节跳动工资待遇")
	t.Insert("字节跳动招聘")
	t.Insert("字节跳动是干什么的")
	t.Insert("字节跳动什么时候上市")
	t.Insert("字节跳动byte")
	t.Insert("字节跳动byteDance")
	res := t.Search("字节跳动")
	fmt.Println(res)
}
