/**
 * @Author hz
 * @Date 6:08 AM$ 5/28/22$
 * @Note B+树 正排索引，用于 数值类型和日期时间类型
 **/

package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"gdindex/tree"
	"os"
	"strconv"
	"utils"
)

type profileindex struct {
	curDocId      uint32
	isMemory      bool
	fieldType     uint32
	fieldName     string
	pfiMmap       *utils.Mmap
	memoryHashMap map[int64][]uint32
	Logger        *utils.Log4FE
	btree         *tree.BTreeDB
}

// newEmptyProfileIndex
// @Description 新建一个空的正排索引
// @Param fieldType 字段类型
// @Param startDocId 起始文档ID
// @Param fieldName 字段名
// @Param logger  日志
// @Return *profileindex 正排索引的引用
func newEmptyProfileIndex(fieldType uint32, startDocId uint32, fieldName string, logger *utils.Log4FE) *profileindex {
	ivt := &profileindex{
		curDocId:      startDocId,
		isMemory:      true,
		fieldType:     fieldType,
		fieldName:     fieldName,
		memoryHashMap: make(map[int64][]uint32),
		Logger:        logger,
	}
	return ivt
}

func newProfileIndexFromLocalFile(btdb *tree.BTreeDB, fieldType uint32, fieldName, segmentName string,
	pfiMmap *utils.Mmap, logger *utils.Log4FE) *profileindex {

	ivt := &profileindex{
		isMemory:  false,
		fieldType: fieldType,
		fieldName: fieldName,
		pfiMmap:   pfiMmap,
		Logger:    logger,
		btree:     btdb,
	}
	return ivt
}

// addDocument
// @Description 正排索引新增文档
// @Param docId 文档ID
// @Param contentStr 内容
// @Return error 任何错误
func (pfi *profileindex) addDocument(docId uint32, contentStr string) error {
	if docId != pfi.curDocId {
		return errors.New("profileindex AddDocument :: Wrong DocId Number")
	}
	pfi.Logger.Trace("[TRACE] profileindex AddDocument :: docid %v content %v", docId, contentStr)

	var value int64 = -1

	switch pfi.fieldType {
	case utils.IDX_TYPE_FLOAT, utils.IDX_TYPE_NUMBER:
		intValue, err := strconv.Atoi(contentStr)
		if err != nil {
			intValue = -1
		}
		value = int64(intValue)
	case utils.IDX_TYPE_DATE:
		value, _ = utils.IsDateTime(contentStr)
	}

	if _, ok := pfi.memoryHashMap[value]; !ok {
		var docIds []uint32
		docIds = append(docIds, docId)
		pfi.memoryHashMap[value] = docIds
	} else {
		pfi.memoryHashMap[value] = append(pfi.memoryHashMap[value], docId)
	}

	pfi.curDocId++
	return nil
}

func (pfi *profileindex) serialization(segmentName string, btdb *tree.BTreeDB) error {
	idxFileName := fmt.Sprintf("%v%v_profileindex.pfi", segmentName, pfi.fieldName)
	idxFd, err := os.OpenFile(idxFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

	pfi.btree = btdb

	if err != nil {
		return err
	}
	defer idxFd.Close()

	leafNodes := make(map[int64]string)
	nowOffset := uint64(0)

	for key, value := range pfi.memoryHashMap {

		lens := len(value)

		lenBuffer := make([]byte, 8)
		binary.LittleEndian.PutUint64(lenBuffer, uint64(lens))
		idxFd.Write(lenBuffer)

		stringBuffer := new(bytes.Buffer)

		err = binary.Write(stringBuffer, binary.LittleEndian, value)
		if err != nil {
			pfi.Logger.Error("[Error] invert Serialization Error : %v", err)
			return err
		}

		idxFd.Write(stringBuffer.Bytes())
		leafNodes[key] = fmt.Sprintf("%v", nowOffset)

		nowOffset += uint64(lens)*4 + 8
	}

	pfi.btree.SetBatch(pfi.fieldName, leafNodes)

	pfi.memoryHashMap = nil
	pfi.isMemory = false
	pfi.Logger.Trace("[Trace] invert Serialization Finish, Writing to : %v%v_profileindex.pfi", segmentName, pfi.fieldName)
	pfi.Logger.Trace("[Trace] invert Serialization Finish, Writing to : %v.db", segmentName)

	return nil

}

func (pfi *profileindex) destroy() {
	pfi.memoryHashMap = nil
}

func (pfi *profileindex) setPfiMmap(mmap *utils.Mmap) {
	pfi.pfiMmap = mmap
}

func (pfi *profileindex) setBtree(btdb *tree.BTreeDB) {
	pfi.btree = btdb
}

func (pfi *profileindex) mergeProfileIndex(inverts []*profileindex, segmentName string, btdb *tree.BTreeDB) error {
	idxFileName := fmt.Sprintf("%v%v_profileindex.pfi", segmentName, pfi.fieldName)
	idxFd, err := os.OpenFile(idxFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer idxFd.Close()

	fi, _ := idxFd.Stat()
	totalOffset := int(fi.Size())

	pfi.btree = btdb
	type ivtMerge struct {
		ivt    *profileindex
		key    int64
		docids []uint32
	}

	ivts := make([]ivtMerge, 0)

	for _, i := range inverts {
		if i.btree == nil {
			continue
		}

		key, _, ok := pfi.GetFirstKV()
		if !ok {
			continue
		}

		docIds, _ := pfi.queryTerm(key)
		ivts = append(ivts, ivtMerge{
			ivt:    i,
			key:    key,
			docids: docIds,
		})
	}

	resflag := 0
	for i := range ivts {
		resflag = resflag | (1 << uint(i))
	}
	flag := 0
	for flag != resflag {
		minKey := ivts[0].key
		meridxs := make([]int, 0)
		for idx, ivt := range ivts {
			if flag>>uint(idx)&1 != 0 {
				continue
			}
			if minKey > ivt.key {
				minKey = ivt.key
				meridxs = make([]int, 0)
				meridxs = append(meridxs, idx)
			} else if minKey == ivt.key {
				meridxs = append(meridxs, idx)
				continue
			}
		}

		value := make([]uint32, 0)

		for _, idx := range meridxs {
			value = append(value, ivts[idx].docids...)

			key, _, ok := ivts[idx].ivt.GetNextKV(ivts[idx].key)
			if !ok {
				flag = flag | (1 << uint(idx))
				continue
			}

			ivts[idx].key = key
			ivts[idx].docids, ok = ivts[idx].ivt.queryTerm(key)
		}

		lens := len(value)
		lenBuffer := make([]byte, 8)
		binary.LittleEndian.PutUint64(lenBuffer, uint64(lens))
		idxFd.Write(lenBuffer)
		buffer := new(bytes.Buffer)
		err = binary.Write(buffer, binary.LittleEndian, value)
		if err != nil {
			pfi.Logger.Error("[ERROR] invert --> Merge :: Error %v", err)
			return err
		}
		idxFd.Write(buffer.Bytes())
		pfi.btree.Set(pfi.fieldName, minKey, uint64(totalOffset))
		totalOffset = totalOffset + 8 + lens*4
	}

	pfi.memoryHashMap = nil
	pfi.isMemory = false

	return nil
}

func (pfi *profileindex) GetFirstKV() (int64, uint64, bool) {

	if pfi.btree == nil {
		return 0, 0, false
	}
	return pfi.btree.GetFirstKV(pfi.fieldName)
}

func (pfi *profileindex) GetNextKV(key int64) (int64, uint64, bool) {

	if pfi.btree == nil {
		return 0, 0, false
	}

	return pfi.btree.GetNextKV(pfi.fieldName, key)
}

func (pfi *profileindex) queryTerm(key int64) ([]uint32, bool) {

	if pfi.isMemory == true {
		docIds, ok := pfi.memoryHashMap[key]
		if ok {
			return docIds, true
		}
	} else if pfi.pfiMmap != nil {
		ok, offset := pfi.btree.Search(pfi.fieldName, key)
		if !ok {
			return nil, false
		}
		lens := pfi.pfiMmap.ReadInt64(int64(offset))

		res := pfi.pfiMmap.ReadIdsArray(uint64(offset)+8, int(lens))
		return res, true
	}

	return nil, false
}

func (pfi *profileindex) queryRange(keyMin, keyMax int64) ([]uint32, bool) {

	res := make([]uint32, 0)
	if pfi.isMemory == true {
		for k, v := range pfi.memoryHashMap {
			if k >= keyMin && k <= keyMax {
				res = utils.MergeIds(res, v)
			}
		}
		return res, true
	} else if pfi.pfiMmap != nil {
		ok, offsets := pfi.btree.SearchRange(pfi.fieldName, keyMin, keyMax)
		if ok {
			for _, offset := range offsets {
				lens := pfi.pfiMmap.ReadInt64(int64(offset))
				IdsArray := pfi.pfiMmap.ReadIdsArray(uint64(offset)+8, int(lens))
				res = utils.MergeIds(res, IdsArray)
			}
			return res, true
		}

	}
	return nil, false
}
