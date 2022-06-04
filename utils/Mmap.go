/*****************************************************************************
 *  file name : Mmap.go
 *  author : iceberg
 *  email  : iceberg_iceberg@163.com
 *
 *  file description : mmap底层封装
 *
******************************************************************************/

package utils

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

type Mmap struct {
	MmapBytes   []byte
	FileName    string
	FileLen     int64
	FilePointer int64
	MapType     int64
	FileFd      *os.File
}

const APPEND_DATA int64 = 1024 * 1024
const (
	MODE_APPEND = iota
	MODE_CREATE // 打开文件时清空文件
)

// 新建一个 Mmap
func NewMmap(file_name string, mode int) (*Mmap, error) {

	this := &Mmap{MmapBytes: make([]byte, 0), FileName: file_name, FileLen: 0, MapType: 0, FilePointer: 0, FileFd: nil}

	file_mode := os.O_RDWR
	file_create_mode := os.O_RDWR | os.O_CREATE | os.O_APPEND
	if mode == MODE_CREATE {
		file_mode = os.O_RDWR | os.O_CREATE | os.O_APPEND
	}

	f, err := os.OpenFile(file_name, file_mode, 0664)

	if err != nil {
		f, err = os.OpenFile(file_name, file_create_mode, 0664)
		if err != nil {
			return nil, err
		}
	}

	fi, err := f.Stat()
	if err != nil {
		fmt.Printf("ERR:%v", err)
	}

	this.FileLen = fi.Size()
	if mode == MODE_CREATE || this.FileLen == 0 {
		syscall.Ftruncate(int(f.Fd()), fi.Size()+APPEND_DATA)
		this.FileLen = APPEND_DATA
	}

	this.MmapBytes, err = syscall.Mmap(int(f.Fd()), 0, int(this.FileLen), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	if err != nil {
		fmt.Printf("MAPPING ERROR  %v \n", err)
		return nil, err
	}

	this.FileFd = f
	return this, nil
}

func (m *Mmap) SetFileEnd(file_len int64) {
	m.FilePointer = file_len
}

func (m *Mmap) checkFilePointer(check_value int64) error {

	if m.FilePointer+check_value >= m.FileLen {
		err := syscall.Ftruncate(int(m.FileFd.Fd()), m.FileLen+APPEND_DATA)
		if err != nil {
			fmt.Printf("ftruncate error : %v\n", err)
			return err
		}
		m.FileLen += APPEND_DATA
		syscall.Munmap(m.MmapBytes)
		m.MmapBytes, err = syscall.Mmap(int(m.FileFd.Fd()), 0, int(m.FileLen), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

		if err != nil {
			fmt.Printf("MAPPING ERROR  %v \n", err)
			return err
		}
	}

	return nil
}

func (m *Mmap) checkFileCap(start, lens int64) error {

	if start+lens >= m.FileLen {
		err := syscall.Ftruncate(int(m.FileFd.Fd()), m.FileLen+APPEND_DATA)
		if err != nil {
			fmt.Printf("ftruncate error : %v\n", err)
			return err
		}

		m.FileLen += APPEND_DATA
		m.FilePointer = start + lens
	}

	return nil

}

func (m *Mmap) isEndOfFile(start int64) bool {

	if m.FilePointer == start {
		return true
	}
	return false

}

func (m *Mmap) ReadInt64(start int64) int64 {

	return int64(binary.LittleEndian.Uint64(m.MmapBytes[start : start+8]))
}

func (m *Mmap) ReadUInt64(start uint64) uint64 {

	return binary.LittleEndian.Uint64(m.MmapBytes[start : start+8])
}

func (m *Mmap) ReadUInt64Arry(start, len uint64) []DocIdNode {

	arry := *(*[]DocIdNode)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&m.MmapBytes[start])),
		Len:  int(len),
		Cap:  int(len),
	}))
	return arry
}

// ReadDocIdsArray 读取一个倒排列表
func (m *Mmap) ReadDocIdsArry(start uint64, len uint64) []DocIdNode {

	arry := *(*[]DocIdNode)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&m.MmapBytes[start])),
		Len:  int(len),
		Cap:  int(len),
	}))
	return arry
}

// ReadIdsSet 读取一个倒排集合
func (m *Mmap) ReadIdsSet(start uint64, len int) map[uint64]struct{} {
	offset := start
	res := make(map[uint64]struct{})
	for i := 0; i < len; i++ {
		id := binary.LittleEndian.Uint64(m.MmapBytes[offset : offset+8])
		res[id] = struct{}{}
		offset += 8
	}

	return res
}

// ReadIdsArray
// @Description: 读取 doc_id 列表
// @receiver this
// @return []uint32
//
func (m *Mmap) ReadIdsArray(start uint64, len int) []uint64 {
	arr := make([]uint64, 0)
	offset := start
	for i := 0; i < len; i++ {
		arr = append(arr, binary.LittleEndian.Uint64(m.MmapBytes[offset:offset+8]))
		offset += 8
	}

	return arr
}

func (m *Mmap) ReadString(start, lens int64) string {

	return string(m.MmapBytes[start : start+lens])
}

func (m *Mmap) Read(start, end int64) []byte {

	return m.MmapBytes[start:end]
}

func (m *Mmap) Write(start int64, buffer []byte) error {

	copy(m.MmapBytes[start:int(start)+len(buffer)], buffer)

	return nil
}

func (m *Mmap) WriteUInt64(start int64, value uint64) error {

	binary.LittleEndian.PutUint64(m.MmapBytes[start:start+8], uint64(value))

	return nil
}

func (m *Mmap) WriteInt64(start, value int64) error {
	binary.LittleEndian.PutUint64(m.MmapBytes[start:start+8], uint64(value))
	return nil //m.Sync()
}

func (m *Mmap) AppendInt64(value int64) error {

	if err := m.checkFilePointer(8); err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(m.MmapBytes[m.FilePointer:m.FilePointer+8], uint64(value))
	m.FilePointer += 8
	return nil //m.Sync()
}

func (m *Mmap) AppendUInt64(value uint64) error {

	if err := m.checkFilePointer(8); err != nil {
		return err
	}

	binary.LittleEndian.PutUint64(m.MmapBytes[m.FilePointer:m.FilePointer+8], value)
	m.FilePointer += 8
	return nil //m.Sync()
}

func (m *Mmap) AppendStringWithLen(value string) error {
	m.AppendInt64(int64(len(value)))
	m.AppendString(value)
	return nil //m.Sync()

}

func (m *Mmap) AppendDetail(shard uint64, value string) error {
	m.AppendUInt64(shard)
	m.AppendInt64(int64(len(value)))
	m.AppendString(value)
	return nil //m.Sync()
}

func (m *Mmap) AppendString(value string) error {

	lens := int64(len(value))
	if err := m.checkFilePointer(lens); err != nil {
		return err
	}

	dst := m.MmapBytes[m.FilePointer : m.FilePointer+lens]
	copy(dst, []byte(value))
	m.FilePointer += lens
	return nil //m.Sync()

}

func (m *Mmap) AppendBytes(value []byte) error {
	lens := int64(len(value))
	if err := m.checkFilePointer(lens); err != nil {
		return err
	}
	dst := m.MmapBytes[m.FilePointer : m.FilePointer+lens]
	copy(dst, value)

	m.FilePointer += lens
	return nil //m.Sync()

}

func (m *Mmap) WriteBytes(start int64, value []byte) error {
	lens := int64(len(value))
	dst := m.MmapBytes[start : start+lens]
	copy(dst, value)
	return nil //m.Sync()
}

func (m *Mmap) Unmap() error {

	syscall.Munmap(m.MmapBytes)
	m.FileFd.Close()
	return nil
}

func (m *Mmap) GetPointer() int64 {
	return m.FilePointer
}

func (m *Mmap) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(&m.MmapBytes))
}

func (m *Mmap) Sync() error {
	dh := m.header()
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC, dh.Data, uintptr(dh.Len), syscall.MS_SYNC)
	if err != 0 {
		fmt.Printf("Sync Error ")
		return errors.New("Sync Error")
	}
	return nil
}

func (m *Mmap) AppendStringWith32Bytes(value string, lens int64) error {

	err := m.AppendInt64(lens)
	if err != nil {
		return err
	}
	if err := m.checkFilePointer(32); err != nil {
		return err
	}
	dst := m.MmapBytes[m.FilePointer : m.FilePointer+32]
	copy(dst, value)
	m.FilePointer += 32
	return nil //m.Sync()
}

func (m *Mmap) ReadStringWith32Bytes(start int64) string {

	lens := m.ReadInt64(start)
	return m.ReadString(start+8, lens)

}

func (m *Mmap) WriteStringWith32Bytes(start int64, value string, lens int64) error {

	m.WriteInt64(start, lens)
	m.WriteBytes(start+4, []byte(value))
	return nil
}
