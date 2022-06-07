package test

import (
	"GoDance/utils"
	"bufio"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestADD(t *testing.T) {
	trieMmap, _ := utils.NewMmap("./trieData.txt", utils.MODE_APPEND)
	insertData := []string{"时间就是金钱", "tomato", "photo", "banana", "apple", "cat", "pig", "find"}
	for _, val := range insertData {
		err := trieMmap.AppendStringWithLen(val)
		if err != nil {
			return
		}
	}
}

func TestRead(t *testing.T) {
	trieMmap, _ := utils.NewMmap("./trieData.txt", utils.MODE_APPEND)
	var offset uint64 = 0
	len := trieMmap.ReadUInt64(offset)
	str := trieMmap.ReadString(int64(offset+8), int64(len))
	fmt.Println(str)
	offset += 8 + len
}

func TestBufio(t *testing.T) {
	insertData := []string{"时间就是金钱", "toma\nto", "photo", "banana", "apple", "cat", "pig", "find"}
	fd, err := os.OpenFile("./bufio.txt", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	bf := bufio.NewWriter(fd)
	defer bf.Flush()
	for _, val := range insertData {
		_, err := bf.WriteString(val + "\n")
		if err != nil {
			return
		}
	}
}

func TestRBufio(t *testing.T) {
	fd, err := os.OpenFile("./bufio.txt", os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	bf := bufio.NewReader(fd)
	for {
		line, _, e := bf.ReadLine()
		if e == io.EOF {
			break
		}
		if e != nil {
			fmt.Println(e)
		}
		fmt.Println(string(line))
	}
}
